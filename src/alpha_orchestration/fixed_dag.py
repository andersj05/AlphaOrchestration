"""Dependency-aware fixed-DAG execution with exact event-journal traces.

The runtime emits unsequenced event drafts only. RunController remains the
sole owner of timestamps, sequence numbers, reduction, and journal writes, so
replay never needs a model, a tool registry, or network access.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from alpha_orchestration.actions import ActionParseError, FinalAction, ToolCallsAction, parse_action
from alpha_orchestration.dag import TaskDefinition, WorkflowDefinition
from alpha_orchestration.data.ledger import (
    MAX_PACKET_BYTES,
    MAX_PACKET_EVIDENCE,
    MAX_PACKET_OBSERVATIONS,
    EvidencePacket,
)
from alpha_orchestration.data.observations import FinancialObservation, UnitKind
from alpha_orchestration.domain import EventKind, JsonValue, RunSpec, TaskStatus
from alpha_orchestration.ports import (
    ActionModel,
    ActionModelRequest,
    ActionModelResult,
    EventDraft,
    ToolCall,
    ToolResult,
)
from alpha_orchestration.tools.registry import ToolRegistry
from alpha_orchestration.tools.schema import SchemaValidationError, validate_json

_OBSERVATION_BOUND_TOOLS = frozenset(
    {
        "finance.calculate",
        "finance.metrics",
        "finance.forecast_growth",
        "finance.discounted_cash_flow",
        "finance.market_statistics",
        "finance.rank",
    }
)
MAX_MODEL_PROMPT_IDS = 8_192
MAX_MODEL_OUTPUT_IDS = 4_096
MAX_MODEL_TOKEN_ID = 2**31 - 1
MAX_MODEL_TELEMETRY_BYTES = 16_384
MAX_MODEL_TRACE_BYTES = 512_000
MAX_MODEL_LABEL_LENGTH = 500
MAX_CITATION_IDS = 100

_FAILED = frozenset({TaskStatus.FAILED, TaskStatus.SKIPPED})
_NATURAL_STOPS = frozenset({"stop", "eos"})


class WorkflowExecutionError(RuntimeError):
    """Raised after journaling a workflow with required failures."""


class ObservationBindingError(ValueError):
    """A controller-side normalized observation binding violation."""

    def __init__(self, code: str, message: str, *, repairable: bool = False) -> None:
        super().__init__(message)
        self.code = code
        self.repairable = repairable


@dataclass(slots=True)
class _TaskOutcome:
    status: TaskStatus = TaskStatus.QUEUED
    output: JsonValue = None
    error: str | None = None
    source_ids: tuple[str, ...] = ()
    turns: int = 0
    tool_calls: int = 0
    degraded_ancestry: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class _TaskEvidence:
    mode: str
    source_ids: tuple[str, ...]
    packet: EvidencePacket | None = None


@dataclass(frozen=True, slots=True)
class _PreparedCall:
    name: str
    proposed_arguments: dict[str, JsonValue]
    arguments: dict[str, JsonValue]
    observation_bindings: dict[str, JsonValue]


@dataclass(frozen=True, slots=True)
class _WorkerEvent:
    task_id: str
    draft: EventDraft


@dataclass(frozen=True, slots=True)
class _WorkerFinished:
    task_id: str
    error: BaseException | None


@dataclass(slots=True)
class _SchedulerStats:
    observed_peak_active_tasks: int = 0


class FixedDagRuntime:
    """Execute one immutable workflow with bounded dependency-aware concurrency."""

    def __init__(
        self,
        workflow: WorkflowDefinition,
        model: ActionModel,
        tool_registry: ToolRegistry,
        *,
        source_ids_by_task: Mapping[str, Sequence[str]] | None = None,
        evidence_packets_by_task: Mapping[str, EvidencePacket] | None = None,
        sampling: Mapping[str, JsonValue] | None = None,
        allow_unverified_sources: bool = False,
    ) -> None:
        contracts = tool_registry.contracts()
        read_only = [
            str(contract["name"])
            for contract in contracts
            if isinstance(contract.get("annotations"), dict)
            and contract["annotations"].get("read_only") is True
        ]
        workflow.validate_tools(tool_registry.names, read_only_tools=read_only)
        manual = source_ids_by_task or {}
        packets = evidence_packets_by_task or {}
        unknown_tasks = sorted((set(manual) | set(packets)) - set(workflow.task_ids))
        if unknown_tasks:
            raise ValueError(f"evidence configured for unknown tasks: {unknown_tasks!r}")
        overlap = sorted(set(manual) & set(packets))
        if not isinstance(allow_unverified_sources, bool):
            raise ValueError("allow_unverified_sources must be a boolean")
        if any(manual.values()) and not allow_unverified_sources:
            raise ValueError(
                "manual source IDs require allow_unverified_sources=True"
            )
        if overlap:
            raise ValueError(
                "tasks cannot combine normalized packets and manual source IDs: "
                f"{overlap!r}"
            )
        task_evidence: dict[str, _TaskEvidence] = {}
        for task_id in workflow.task_ids:
            packet = packets.get(task_id)
            if packet is not None:
                if not isinstance(packet, EvidencePacket):
                    raise ValueError(
                        f"evidence packet for {task_id!r} must be an EvidencePacket"
                    )
                detached = EvidencePacket.from_dict(
                    _json_copy(packet.to_dict(), "evidence packet")
                )
                if not detached.observations:
                    raise ValueError(
                        f"evidence packet for {task_id!r} must contain observations"
                    )
                if len(detached.observations) > MAX_PACKET_OBSERVATIONS:
                    raise ValueError(f"evidence packet for {task_id!r} exceeds observation limit")
                if len(detached.evidence) > MAX_PACKET_EVIDENCE:
                    raise ValueError(f"evidence packet for {task_id!r} exceeds evidence limit")
                if detached.encoded_size_bytes > MAX_PACKET_BYTES:
                    raise ValueError(f"evidence packet for {task_id!r} exceeds byte limit")
                task_evidence[task_id] = _TaskEvidence(
                    "normalized_packet",
                    detached.source_ids,
                    detached,
                )
                continue
            source_ids = tuple(manual.get(task_id, ()))
            if any(not isinstance(item, str) or not item.strip() for item in source_ids):
                raise ValueError(f"source IDs for {task_id!r} must be non-empty strings")
            if len(source_ids) != len(set(source_ids)):
                raise ValueError(f"source IDs for {task_id!r} must be unique")
            task_evidence[task_id] = _TaskEvidence(
                "manual_unverified_source_ids" if source_ids else "none",
                source_ids,
            )
        self.workflow = workflow
        self.model = model
        self.tool_registry = tool_registry
        self._task_evidence = task_evidence
        self.sampling = _json_copy(
            sampling or {"temperature": 0.0, "seed": 0},
            "sampling controls",
        )
        self._contracts = {
            str(contract["name"]): _json_copy(contract, "tool contract")
            for contract in contracts
        }
        self._output_schemas = {
            task.task_id: _json_copy(task.output_schema, "task output schema")
            for task in workflow.tasks
        }
        self._plan = _json_copy(workflow.to_dict(), "workflow plan")
        self._plan_hash = _canonical_hash(self._plan)
        if self._plan_hash != workflow.plan_hash:
            raise ValueError("workflow plan snapshot hash mismatch")
        raw_tasks = self._plan.get("tasks")
        if not isinstance(raw_tasks, list):  # pragma: no cover - validated upstream
            raise AssertionError("workflow plan tasks must be a list")
        self._planned_tasks = _json_copy(raw_tasks, "planned tasks")

    async def stream(self, spec: RunSpec) -> AsyncIterator[EventDraft]:
        agent_ids = tuple(dict.fromkeys(task.agent_id for task in self.workflow.tasks))
        if len(agent_ids) > spec.agent_budget:
            raise WorkflowExecutionError(
                f"workflow requires {len(agent_ids)} agents; run budget is {spec.agent_budget}"
            )
        for agent_id in agent_ids:
            yield EventDraft(
                EventKind.AGENT_REGISTERED,
                f"Registered fixed-DAG agent {agent_id}",
                agent_id=agent_id,
                payload={"role": agent_id, "lane": "fixed_dag"},
            )
        effective_active_slots = min(self.workflow.active_slots, spec.active_slots)
        scheduler_stats = _SchedulerStats()
        yield EventDraft(
            EventKind.WORKFLOW_PLANNED,
            f"Planned fixed workflow {self.workflow.workflow_id}@{self.workflow.version}",
            payload={
                "workflow_id": self.workflow.workflow_id,
                "workflow_version": self.workflow.version,
                "plan_hash": self._plan_hash,
                "plan": _json_copy(self._plan, "workflow plan"),
                "tasks": _json_copy(self._planned_tasks, "planned tasks"),
                "configured_active_slots": self.workflow.active_slots,
                "requested_active_slots": spec.active_slots,
                "effective_active_slots": effective_active_slots,
                "actual_active_slots": None,
                "active_slots": effective_active_slots,
            },
        )

        outcomes: dict[str, _TaskOutcome] = {}
        if effective_active_slots > 1:
            async for event in self._stream_concurrent_tasks(
                spec,
                outcomes,
                effective_active_slots,
                scheduler_stats,
            ):
                yield event
        serial_tasks = () if effective_active_slots > 1 else self.workflow.topological_tasks
        for task in serial_tasks:
            failed_dependencies = tuple(
                dependency
                for dependency in task.depends_on
                if outcomes[dependency].status in _FAILED
            )
            degraded_dependencies = tuple(
                dependency
                for dependency in task.depends_on
                if outcomes[dependency].status is TaskStatus.PARTIAL
            )
            degraded_ancestry = tuple(
                dict.fromkeys(
                    ancestor
                    for dependency in task.depends_on
                    for ancestor in (
                        *((dependency,) if outcomes[dependency].status in _FAILED else ()),
                        *((dependency,) if outcomes[dependency].status is TaskStatus.PARTIAL else ()),
                        *outcomes[dependency].degraded_ancestry,
                    )
                )
            )
            if failed_dependencies and not task.allow_failed_dependencies:
                reason = f"blocked by failed dependencies: {', '.join(failed_dependencies)}"
                outcomes[task.task_id] = _TaskOutcome(status=TaskStatus.SKIPPED, error=reason)
                yield EventDraft(
                    EventKind.TASK_SKIPPED,
                    f"Skipped task {task.task_id}: dependency failure",
                    agent_id=task.agent_id,
                    payload={
                        "task_id": task.task_id,
                        "reason": reason,
                        "failed_dependencies": list(failed_dependencies),
                        "required": task.required,
                    },
                )
                continue

            task_evidence = self._task_evidence[task.task_id]
            allowed_sources = _task_source_ids(
                task,
                outcomes,
                task_evidence.source_ids,
            )
            outcome = _TaskOutcome(
                status=TaskStatus.RUNNING, degraded_ancestry=degraded_ancestry
            )
            outcomes[task.task_id] = outcome
            yield EventDraft(
                EventKind.TASK_STARTED,
                f"Started fixed task {task.task_id}",
                agent_id=task.agent_id,
                payload={
                    "task_id": task.task_id,
                    "dependency_outcomes": {
                        dependency: outcomes[dependency].status.value
                        for dependency in task.depends_on
                    },
                    "failed_dependencies": list(failed_dependencies),
                    "degraded_dependencies": list(degraded_dependencies),
                    "evidence_mode": task_evidence.mode,
                    "degraded_ancestry": list(degraded_ancestry),
                    "evidence_packet_hash": (
                        None
                        if task_evidence.packet is None
                        else _canonical_hash(task_evidence.packet.to_dict())
                    ),
                    "allowed_source_ids": list(allowed_sources),
                },
            )
            async for event in self._stream_task(
                spec,
                task,
                outcome,
                outcomes,
                failed_dependencies,
                degraded_dependencies,
                allowed_sources,
                degraded_ancestry,
                task_evidence,
            ):
                yield event

        if effective_active_slots == 1:
            scheduler_stats.observed_peak_active_tasks = 1
        for agent_id in agent_ids:
            owned = [
                outcomes[task.task_id] for task in self.workflow.tasks if task.agent_id == agent_id
            ]
            if any(item.status in _FAILED for item in owned):
                yield EventDraft(
                    EventKind.AGENT_FAILED,
                    f"Fixed-DAG work for {agent_id} ended with failed work",
                    agent_id=agent_id,
                    payload={"error": "one or more owned tasks failed or were skipped"},
                )
            else:
                yield EventDraft(
                    EventKind.AGENT_COMPLETED,
                    f"Fixed-DAG work for {agent_id} completed",
                    agent_id=agent_id,
                )

        counts = {status.value: 0 for status in TaskStatus}
        for outcome in outcomes.values():
            counts[outcome.status.value] += 1
        required_failures = [
            task.task_id
            for task in self.workflow.tasks
            if task.required and outcomes[task.task_id].status in _FAILED
        ]
        yield EventDraft(
            EventKind.WORKFLOW_COMPLETED,
            f"Fixed workflow completed with {len(required_failures)} required failures",
            payload={
                "workflow_id": self.workflow.workflow_id,
                "workflow_version": self.workflow.version,
                "plan_hash": self._plan_hash,
                "counts": counts,
                "required_failures": required_failures,
                "observed_peak_active_tasks": scheduler_stats.observed_peak_active_tasks,
                "partial": any(
                    outcome.status is not TaskStatus.COMPLETE for outcome in outcomes.values()
                ),
            },
        )
        if required_failures:
            raise WorkflowExecutionError(f"required tasks failed: {', '.join(required_failures)}")

    async def _stream_concurrent_tasks(
        self,
        spec: RunSpec,
        outcomes: dict[str, _TaskOutcome],
        active_slots: int,
        stats: _SchedulerStats,
    ) -> AsyncIterator[EventDraft]:
        task_order = self.workflow.topological_task_ids
        tasks_by_id = self.workflow.tasks_by_id
        pending = set(task_order)
        terminal: set[str] = set()
        active_agents: set[str] = set()
        running: dict[str, asyncio.Task[None]] = {}
        queue: asyncio.Queue[_WorkerEvent | _WorkerFinished] = asyncio.Queue()

        async def pump(
            task: TaskDefinition,
            outcome: _TaskOutcome,
            failed_dependencies: tuple[str, ...],
            degraded_dependencies: tuple[str, ...],
            allowed_sources: tuple[str, ...],
            degraded_ancestry: tuple[str, ...],
            task_evidence: _TaskEvidence,
        ) -> None:
            error: BaseException | None = None
            try:
                async for draft in self._stream_task(
                    spec,
                    task,
                    outcome,
                    outcomes,
                    failed_dependencies,
                    degraded_dependencies,
                    allowed_sources,
                    degraded_ancestry,
                    task_evidence,
                ):
                    await queue.put(_WorkerEvent(task.task_id, draft))
            except BaseException as exc:
                error = exc
            finally:
                await queue.put(_WorkerFinished(task.task_id, error))

        try:
            while pending or running:
                made_progress = True
                while made_progress:
                    made_progress = False
                    for task_id in task_order:
                        if task_id not in pending:
                            continue
                        task = tasks_by_id[task_id]
                        if any(dependency not in terminal for dependency in task.depends_on):
                            continue

                        failed_dependencies = tuple(
                            dependency
                            for dependency in task.depends_on
                            if outcomes[dependency].status in _FAILED
                        )
                        degraded_dependencies = tuple(
                            dependency
                            for dependency in task.depends_on
                            if outcomes[dependency].status is TaskStatus.PARTIAL
                        )
                        degraded_ancestry = tuple(
                            dict.fromkeys(
                                ancestor
                                for dependency in task.depends_on
                                for ancestor in (
                                    *((dependency,) if outcomes[dependency].status in _FAILED else ()),
                                    *((dependency,) if outcomes[dependency].status is TaskStatus.PARTIAL else ()),
                                    *outcomes[dependency].degraded_ancestry,
                                )
                            )
                        )
                        if failed_dependencies and not task.allow_failed_dependencies:
                            reason = f"blocked by failed dependencies: {', '.join(failed_dependencies)}"
                            outcomes[task_id] = _TaskOutcome(
                                status=TaskStatus.SKIPPED,
                                error=reason,
                            )
                            pending.remove(task_id)
                            yield EventDraft(
                                EventKind.TASK_SKIPPED,
                                f"Skipped task {task_id}: dependency failure",
                                agent_id=task.agent_id,
                                payload={
                                    "task_id": task_id,
                                    "reason": reason,
                                    "failed_dependencies": list(failed_dependencies),
                                    "required": task.required,
                                },
                            )
                            terminal.add(task_id)
                            made_progress = True
                            break

                        if len(running) >= active_slots:
                            break
                        if task.agent_id in active_agents:
                            continue

                        task_evidence = self._task_evidence[task_id]
                        allowed_sources = _task_source_ids(
                            task,
                            outcomes,
                            task_evidence.source_ids,
                        )
                        outcome = _TaskOutcome(
                            status=TaskStatus.RUNNING,
                            degraded_ancestry=degraded_ancestry,
                        )
                        outcomes[task_id] = outcome
                        pending.remove(task_id)
                        active_agents.add(task.agent_id)
                        yield EventDraft(
                            EventKind.TASK_STARTED,
                            f"Started fixed task {task_id}",
                            agent_id=task.agent_id,
                            payload={
                                "task_id": task_id,
                                "dependency_outcomes": {
                                    dependency: outcomes[dependency].status.value
                                    for dependency in task.depends_on
                                },
                                "failed_dependencies": list(failed_dependencies),
                                "degraded_dependencies": list(degraded_dependencies),
                                "evidence_mode": task_evidence.mode,
                                "degraded_ancestry": list(degraded_ancestry),
                                "evidence_packet_hash": (
                                    None
                                    if task_evidence.packet is None
                                    else _canonical_hash(task_evidence.packet.to_dict())
                                ),
                                "allowed_source_ids": list(allowed_sources),
                            },
                        )
                        running[task_id] = asyncio.create_task(
                            pump(
                                task,
                                outcome,
                                failed_dependencies,
                                degraded_dependencies,
                                allowed_sources,
                                degraded_ancestry,
                                task_evidence,
                            ),
                            name=f"alpha:{spec.run_id}:{task_id}",
                        )
                        stats.observed_peak_active_tasks = max(
                            stats.observed_peak_active_tasks,
                            len(running),
                        )
                        made_progress = True

                if not running:
                    if pending:
                        blocked = ", ".join(
                            task_id for task_id in task_order if task_id in pending
                        )
                        raise WorkflowExecutionError(
                            f"scheduler deadlock with pending tasks: {blocked}"
                        )
                    break

                item = await queue.get()
                if isinstance(item, _WorkerEvent):
                    yield item.draft
                    continue

                worker = running.pop(item.task_id)
                active_agents.remove(tasks_by_id[item.task_id].agent_id)
                await worker
                if item.error is not None:
                    raise item.error
                terminal.add(item.task_id)
        finally:
            unfinished = tuple(running.values())
            for worker in unfinished:
                worker.cancel()
            if unfinished:
                await asyncio.gather(*unfinished, return_exceptions=True)

    async def _stream_task(
        self,
        spec: RunSpec,
        task: TaskDefinition,
        outcome: _TaskOutcome,
        outcomes: Mapping[str, _TaskOutcome],
        failed_dependencies: tuple[str, ...],
        degraded_dependencies: tuple[str, ...],
        allowed_sources: tuple[str, ...],
        degraded_ancestry: tuple[str, ...],
        task_evidence: _TaskEvidence,
    ) -> AsyncIterator[EventDraft]:
        scoped = self.tool_registry.scoped(
            task.allowed_tools,
            allowed_source_ids=allowed_sources,
        )
        packet_payload = (
            None if task_evidence.packet is None else task_evidence.packet.to_dict()
        )
        output_schema = self._output_schemas[task.task_id]
        transcript: list[dict[str, JsonValue]] = [
            {
                "role": "controller",
                "content": {
                    "task_id": task.task_id,
                    "prompt_key": task.prompt_key,
                    "dependencies": {
                        dependency: {
                            "status": outcomes[dependency].status.value,
                            "output": outcomes[dependency].output,
                        }
                        for dependency in task.depends_on
                    },
                    "allowed_source_ids": list(allowed_sources),
                    "evidence_mode": task_evidence.mode,
                    "evidence_packet": packet_payload,
                    "observation_binding": _binding_contract(task.allowed_tools),
                    "degraded_ancestry": list(degraded_ancestry),
                    "output_schema": _json_copy(output_schema, "task output schema"),
                },
            }
        ]
        repairs_used = 0
        for turn in range(1, task.max_turns + 1):
            outcome.turns = turn
            request_id = f"{spec.run_id}:{task.task_id}:t{turn}"
            session_id = f"{spec.run_id}:{task.agent_id}"
            request = ActionModelRequest(
                run_id=spec.run_id,
                workflow_id=self.workflow.workflow_id,
                task_id=task.task_id,
                agent_id=task.agent_id,
                turn=turn,
                request_id=request_id,
                session_id=session_id,
                prompt_key=task.prompt_key,
                transcript=tuple(transcript),
                tool_contracts=scoped.contracts(),
                output_schema=output_schema,
                evidence_packet=packet_payload,
                allowed_source_ids=allowed_sources,
                max_new_tokens=task.max_new_tokens,
                max_action_bytes=task.max_action_bytes,
                sampling=self.sampling,
            )
            exact_request = _json_copy(request.to_dict(), "model request")
            yield EventDraft(
                EventKind.MODEL_TURN_STARTED,
                f"Requested action for task {task.task_id} turn {turn}",
                agent_id=task.agent_id,
                payload={
                    "task_id": task.task_id,
                    "turn": turn,
                    "request_id": request_id,
                    "session_id": session_id,
                    "request": exact_request,
                    "request_hash": _canonical_hash(exact_request),
                },
            )
            try:
                result = await self.model.complete(request)
            except Exception as exc:
                result = ActionModelResult(
                    request_id=request_id,
                    output_text="",
                    finish_reason="model_error",
                    telemetry={"error_type": type(exc).__name__, "error": str(exc)},
                )
            terminal_error = _model_result_error(
                result,
                request_id,
                max_action_bytes=task.max_action_bytes,
            )
            trace = (
                _model_result_dict(result, task.max_action_bytes)
                if terminal_error is None
                else _invalid_model_result_trace(request_id, terminal_error)
            )
            yield EventDraft(
                EventKind.MODEL_TURN_COMPLETED,
                f"Received action for task {task.task_id} turn {turn}",
                agent_id=task.agent_id,
                payload={
                    "task_id": task.task_id,
                    "turn": turn,
                    "request_id": trace["request_id"],
                    "session_id": session_id,
                    "output": trace["output_text"],
                    "output_bytes": trace["output_bytes"],
                    "output_hash": trace["output_hash"],
                    "output_truncated": trace["output_truncated"],
                    "trace": trace,
                    "trace_hash": _canonical_hash(trace),
                },
            )
            if terminal_error is not None:
                code, message = terminal_error
                yield _action_rejected(
                    task,
                    turn,
                    code,
                    message,
                    trace["output_text"],
                    repair_allowed=False,
                    repairs_remaining=task.repair_budget - repairs_used,
                )
                yield _fail_task(task, outcome, code, message)
                return
            model_output = trace["output_text"]
            if not isinstance(model_output, str):  # pragma: no cover - validated above
                raise AssertionError("validated model output must be text")
            transcript.append({"role": "assistant", "turn": turn, "content": model_output})
            try:
                action = parse_action(
                    model_output,
                    max_bytes=task.max_action_bytes,
                    max_calls=task.max_calls_per_turn,
                )
            except ActionParseError as exc:
                can_repair = (
                    exc.repairable
                    and repairs_used < task.repair_budget
                    and turn < task.max_turns
                )
                if can_repair:
                    repairs_used += 1
                yield _action_rejected(
                    task,
                    turn,
                    exc.code.value,
                    str(exc),
                    trace["output_text"],
                    repair_allowed=can_repair,
                    repairs_remaining=task.repair_budget - repairs_used,
                )
                if can_repair:
                    transcript.append(
                        _repair_message(
                            exc.code.value,
                            str(exc),
                            task.repair_budget - repairs_used,
                        )
                    )
                    continue
                yield _fail_task(task, outcome, exc.code.value, str(exc))
                return

            if isinstance(action, FinalAction):
                try:
                    validate_json(action.payload, output_schema)
                except (SchemaValidationError, ValueError, TypeError, OverflowError) as exc:
                    can_repair = repairs_used < task.repair_budget and turn < task.max_turns
                    if can_repair:
                        repairs_used += 1
                    yield _action_rejected(
                        task,
                        turn,
                        "invalid_final_output",
                        str(exc),
                        action.payload,
                        repair_allowed=can_repair,
                        repairs_remaining=task.repair_budget - repairs_used,
                    )
                    if can_repair:
                        transcript.append(
                            _repair_message(
                                "invalid_final_output",
                                str(exc),
                                task.repair_budget - repairs_used,
                            )
                        )
                        continue
                    yield _fail_task(task, outcome, "invalid_final_output", str(exc))
                    return
                try:
                    cited_sources = _cited_source_ids(action.payload)
                except ObservationBindingError as exc:
                    yield _action_rejected(
                        task,
                        turn,
                        exc.code,
                        str(exc),
                        action.payload,
                        repair_allowed=False,
                        repairs_remaining=task.repair_budget - repairs_used,
                    )
                    yield _fail_task(task, outcome, exc.code, str(exc))
                    return
                unknown_sources = sorted(set(cited_sources) - set(allowed_sources))
                if unknown_sources:
                    message = f"final output cites untrusted source IDs: {unknown_sources!r}"
                    yield _action_rejected(
                        task,
                        turn,
                        "source_not_allowed",
                        message,
                        action.payload,
                        repair_allowed=False,
                        repairs_remaining=task.repair_budget - repairs_used,
                    )
                    yield _fail_task(task, outcome, "source_not_allowed", message)
                    return
                degraded = bool(degraded_ancestry)
                outcome.status = TaskStatus.PARTIAL if degraded else TaskStatus.COMPLETE
                outcome.output = action.payload
                outcome.source_ids = tuple(dict.fromkeys((*outcome.source_ids, *cited_sources)))
                outcome.error = (
                    f"completed with degraded dependencies: {', '.join(degraded_ancestry)}"
                    if degraded
                    else None
                )
                payload: dict[str, JsonValue] = {
                    "task_id": task.task_id,
                    "output": action.payload,
                    "source_ids": list(cited_sources),
                    "used_source_ids": list(outcome.source_ids),
                    "turns": outcome.turns,
                    "tool_calls": outcome.tool_calls,
                    "partial": degraded,
                    "failed_dependencies": list(failed_dependencies),
                    "degraded_dependencies": list(degraded_dependencies),
                    "degraded_ancestry": list(degraded_ancestry),
                }
                if outcome.error is not None:
                    payload["error"] = outcome.error
                yield EventDraft(
                    EventKind.TASK_COMPLETED,
                    f"Completed fixed task {task.task_id}",
                    agent_id=task.agent_id,
                    payload=payload,
                )
                return

            if not isinstance(action, ToolCallsAction):
                raise AssertionError(f"unhandled action type: {type(action).__name__}")
            if outcome.tool_calls + len(action.calls) > task.max_tool_calls:
                message = (
                    f"tool-call budget exceeded: requested {len(action.calls)} "
                    f"with {task.max_tool_calls - outcome.tool_calls} remaining"
                )
                yield _action_rejected(
                    task,
                    turn,
                    "tool_budget_exceeded",
                    message,
                    model_output,
                    repair_allowed=False,
                    repairs_remaining=task.repair_budget - repairs_used,
                )
                yield _fail_task(task, outcome, "tool_budget_exceeded", message)
                return

            try:
                prepared_calls = _preflight_calls(
                    action,
                    allowed_tools=scoped.names,
                    allowed_sources=allowed_sources,
                    contracts=self._contracts,
                    task_evidence=task_evidence,
                )
            except ObservationBindingError as exc:
                can_repair = (
                    exc.repairable
                    and repairs_used < task.repair_budget
                    and turn < task.max_turns
                )
                if can_repair:
                    repairs_used += 1
                yield _action_rejected(
                    task,
                    turn,
                    exc.code,
                    str(exc),
                    model_output,
                    repair_allowed=can_repair,
                    repairs_remaining=task.repair_budget - repairs_used,
                )
                if can_repair:
                    transcript.append(
                        _repair_message(
                            exc.code,
                            str(exc),
                            task.repair_budget - repairs_used,
                        )
                    )
                    continue
                yield _fail_task(task, outcome, exc.code, str(exc))
                return

            request_repair = False
            for call_index, prepared in enumerate(prepared_calls):
                call_id = f"{spec.run_id}:{task.task_id}:t{turn}:c{call_index}"
                contract = self._contracts[prepared.name]
                call = ToolCall(
                    name=prepared.name,
                    arguments=prepared.arguments,
                    call_id=call_id,
                )
                outcome.tool_calls += 1
                yield EventDraft(
                    EventKind.TOOL_STARTED,
                    f"Calling {prepared.name} for task {task.task_id}",
                    agent_id=task.agent_id,
                    payload={
                        "task_id": task.task_id,
                        "turn": turn,
                        "call_index": call_index,
                        "call_id": call_id,
                        "tool": prepared.name,
                        "tool_version": str(contract.get("version", "unknown")),
                        "proposed_arguments": prepared.proposed_arguments,
                        "proposed_arguments_hash": _canonical_hash(
                            prepared.proposed_arguments
                        ),
                        "arguments": prepared.arguments,
                        "source_ids": _raw_source_ids(prepared.arguments),
                        "observation_bindings": prepared.observation_bindings,
                        "arguments_hash": _canonical_hash(prepared.arguments),
                    },
                )
                try:
                    tool_result = await scoped.execute(call)
                    _validate_tool_result(call, tool_result, allowed_sources)
                except Exception as exc:
                    error: dict[str, JsonValue] = {
                        "code": "tool_execution_error",
                        "message": f"{type(exc).__name__}: {exc}",
                    }
                    failure_envelope: dict[str, JsonValue] = {
                        "call_id": call_id,
                        "payload": {"ok": False, "tool": prepared.name, "error": error},
                        "source_ids": [],
                        "retryable": False,
                    }
                    yield EventDraft(
                        EventKind.TOOL_FAILED,
                        f"Tool {prepared.name} failed for task {task.task_id}",
                        agent_id=task.agent_id,
                        payload={
                            "task_id": task.task_id,
                            "turn": turn,
                            "call_index": call_index,
                            "call_id": call_id,
                            "tool": prepared.name,
                            "error": error,
                            "result_envelope": failure_envelope,
                            "result_hash": _canonical_hash(failure_envelope),
                        },
                    )
                    yield _fail_task(
                        task,
                        outcome,
                        "tool_execution_error",
                        str(error["message"]),
                    )
                    return
                exact_result = _tool_result_dict(tool_result)
                ok = tool_result.payload.get("ok") is True
                error = exact_result["payload"].get("error")
                yield EventDraft(
                    EventKind.TOOL_COMPLETED if ok else EventKind.TOOL_REJECTED,
                    (
                        f"Tool {prepared.name} completed for task {task.task_id}"
                        if ok
                        else f"Tool {prepared.name} rejected a call for task {task.task_id}"
                    ),
                    agent_id=task.agent_id,
                    payload={
                        "task_id": task.task_id,
                        "turn": turn,
                        "call_index": call_index,
                        "call_id": call_id,
                        "tool": prepared.name,
                        "result": exact_result["payload"],
                        "source_ids": exact_result["source_ids"],
                        "retryable": exact_result["retryable"],
                        "error": error,
                        "result_envelope": exact_result,
                        "result_hash": _canonical_hash(exact_result),
                    },
                )
                transcript.append(
                    {
                        "role": "tool",
                        "turn": turn,
                        "call_id": call_id,
                        "name": prepared.name,
                        "payload": exact_result["payload"],
                        "source_ids": exact_result["source_ids"],
                    }
                )
                if ok:
                    outcome.source_ids = tuple(
                        dict.fromkeys(
                            (*outcome.source_ids, *tool_result.source_ids)
                        )
                    )
                    continue
                error_code, error_message, repairable = _tool_error(error)
                can_repair = (
                    repairable
                    and repairs_used < task.repair_budget
                    and turn < task.max_turns
                )
                if can_repair:
                    repairs_used += 1
                yield _action_rejected(
                    task,
                    turn,
                    error_code,
                    error_message,
                    model_output,
                    repair_allowed=can_repair,
                    repairs_remaining=task.repair_budget - repairs_used,
                )
                if can_repair:
                    transcript.append(
                        _repair_message(
                            error_code,
                            error_message,
                            task.repair_budget - repairs_used,
                        )
                    )
                    request_repair = True
                    break
                yield _fail_task(task, outcome, error_code, error_message)
                return
            if request_repair:
                continue

        message = f"task did not produce a final action in {task.max_turns} turns"
        yield _fail_task(task, outcome, "turn_budget_exhausted", message)


def _preflight_calls(
    action: ToolCallsAction,
    *,
    allowed_tools: Sequence[str],
    allowed_sources: Sequence[str],
    contracts: Mapping[str, Mapping[str, JsonValue]],
    task_evidence: _TaskEvidence,
) -> tuple[_PreparedCall, ...]:
    prepared: list[_PreparedCall] = []
    tool_allowlist = frozenset(allowed_tools)
    source_allowlist = frozenset(allowed_sources)
    for proposal in action.calls:
        if proposal.name not in tool_allowlist:
            raise ObservationBindingError(
                "tool_not_allowed",
                f"tool is not allowed for this task: {proposal.name}",
            )
        proposed = _json_copy(proposal.arguments, "proposed tool arguments")
        proposed_sources = _strict_source_ids(proposed)
        unknown_sources = sorted(set(proposed_sources) - source_allowlist)
        if unknown_sources:
            raise ObservationBindingError(
                "source_not_allowed",
                f"tool call cites untrusted source IDs: {unknown_sources!r}",
            )
        if proposal.name in _OBSERVATION_BOUND_TOOLS:
            if task_evidence.packet is None:
                raise ObservationBindingError(
                    "normalized_evidence_required",
                    f"{proposal.name} requires a normalized EvidencePacket",
                )
            arguments, bindings = _bind_finance_arguments(
                proposal.name,
                proposed,
                task_evidence.packet,
            )
        else:
            arguments = proposed
            bindings = {}
        canonical_sources = _strict_source_ids(arguments)
        unknown_sources = sorted(set(canonical_sources) - source_allowlist)
        if unknown_sources:
            raise ObservationBindingError(
                "source_not_allowed",
                f"canonical tool call cites untrusted source IDs: {unknown_sources!r}",
            )
        contract = contracts.get(proposal.name)
        schema = None if contract is None else contract.get("input_schema")
        if not isinstance(schema, Mapping):
            raise ObservationBindingError(
                "tool_contract_unavailable",
                f"trusted input schema is unavailable for {proposal.name}",
            )
        try:
            validate_json(arguments, schema)
        except (SchemaValidationError, ValueError, TypeError, OverflowError) as exc:
            raise ObservationBindingError(
                "invalid_schema",
                str(exc),
                repairable=True,
            ) from exc
        prepared.append(
            _PreparedCall(
                name=proposal.name,
                proposed_arguments=proposed,
                arguments=arguments,
                observation_bindings=bindings,
            )
        )
    return tuple(prepared)


def _bind_finance_arguments(
    name: str,
    proposed: Mapping[str, JsonValue],
    packet: EvidencePacket,
) -> tuple[dict[str, JsonValue], dict[str, JsonValue]]:
    index = {observation.observation_id: observation for observation in packet.observations}
    if name == "finance.calculate":
        return _bind_calculate(proposed, index)
    if name == "finance.metrics":
        return _bind_metrics(proposed, index)
    if name == "finance.forecast_growth":
        return _bind_forecast(proposed, index)
    if name == "finance.discounted_cash_flow":
        return _bind_dcf(proposed, index)
    if name == "finance.market_statistics":
        return _bind_market_statistics(proposed, index)
    if name == "finance.rank":
        raise ObservationBindingError(
            "binding_not_supported", "finance.rank requires trusted task-output binding"
        )
    raise AssertionError(f"unhandled observation-bound tool: {name}")


def _bind_calculate(
    proposed: Mapping[str, JsonValue],
    index: Mapping[str, FinancialObservation],
) -> tuple[dict[str, JsonValue], dict[str, JsonValue]]:
    arguments = _json_copy(proposed, "finance.calculate proposal")
    _reject_controller_fields(arguments, "finance.calculate", {"source_ids", "context"})
    raw_operations = arguments.get("operations")
    if not isinstance(raw_operations, list):
        return arguments, {}
    bindings: dict[str, JsonValue] = {}
    selected: list[FinancialObservation] = []
    scalar_fields = ("current", "prior", "numerator", "denominator", "start", "end")
    for operation_index, operation in enumerate(raw_operations):
        if not isinstance(operation, dict):
            continue
        for field_name in scalar_fields:
            if field_name not in operation:
                continue
            path = f"operations[{operation_index}].{field_name}"
            observation = _resolve_observation(operation[field_name], index, path)
            operation[field_name] = observation.value
            bindings[path] = observation.observation_id
            selected.append(observation)
        if "values" in operation:
            values = operation["values"]
            if not isinstance(values, list):
                raise ObservationBindingError(
                    "invalid_observation_binding",
                    f"operations[{operation_index}].values must be observation references",
                )
            resolved_values: list[JsonValue] = []
            for value_index, value in enumerate(values):
                path = f"operations[{operation_index}].values[{value_index}]"
                observation = _resolve_observation(value, index, path)
                resolved_values.append(observation.value)
                bindings[path] = observation.observation_id
                selected.append(observation)
            operation["values"] = resolved_values
    _inject_observation_lineage(arguments, selected)
    return arguments, bindings


def _bind_metrics(
    proposed: Mapping[str, JsonValue],
    index: Mapping[str, FinancialObservation],
) -> tuple[dict[str, JsonValue], dict[str, JsonValue]]:
    arguments = _json_copy(proposed, "finance.metrics proposal")
    _reject_controller_fields(arguments, "finance.metrics", {"values", "source_ids", "context"})
    raw_inputs = arguments.pop("observation_inputs", None)
    if not isinstance(raw_inputs, dict) or not raw_inputs:
        raise ObservationBindingError(
            "invalid_observation_binding",
            "finance.metrics requires non-empty observation_inputs",
        )
    values: dict[str, JsonValue] = {}
    bindings: dict[str, JsonValue] = {}
    selected: list[FinancialObservation] = []
    for value_name, reference in raw_inputs.items():
        path = f"observation_inputs.{value_name}"
        observation = _resolve_observation(reference, index, path)
        values[value_name] = observation.value
        bindings[path] = observation.observation_id
        selected.append(observation)
    arguments["values"] = values
    _inject_observation_lineage(arguments, selected)
    return arguments, bindings


def _bind_forecast(
    proposed: Mapping[str, JsonValue],
    index: Mapping[str, FinancialObservation],
) -> tuple[dict[str, JsonValue], dict[str, JsonValue]]:
    arguments = _json_copy(proposed, "finance.forecast_growth proposal")
    _reject_controller_fields(
        arguments,
        "finance.forecast_growth",
        {"source_ids", "context", "unit"},
    )
    if "base_value" not in arguments:
        return arguments, {}
    observation = _resolve_observation(arguments["base_value"], index, "base_value")
    arguments["base_value"] = observation.value
    arguments["unit"] = _unit_label(observation)
    _inject_observation_lineage(arguments, [observation])
    return arguments, {"base_value": observation.observation_id}


def _bind_dcf(
    proposed: Mapping[str, JsonValue],
    index: Mapping[str, FinancialObservation],
) -> tuple[dict[str, JsonValue], dict[str, JsonValue]]:
    arguments = _json_copy(proposed, "finance.discounted_cash_flow proposal")
    _reject_controller_fields(
        arguments,
        "finance.discounted_cash_flow",
        {"source_ids", "context", "unit", "net_debt", "shares_outstanding"},
    )
    raw_inputs = arguments.pop("observation_inputs", None)
    if not isinstance(raw_inputs, dict) or not raw_inputs:
        raise ObservationBindingError(
            "invalid_observation_binding",
            "finance.discounted_cash_flow requires non-empty observation_inputs",
        )
    allowed_names = {"net_debt", "shares_outstanding", "terminal_metric_value"}
    unknown_names = sorted(set(raw_inputs) - allowed_names)
    if unknown_names:
        raise ObservationBindingError(
            "invalid_observation_binding",
            f"unknown DCF observation input names: {unknown_names!r}",
        )
    bindings: dict[str, JsonValue] = {}
    selected: list[FinancialObservation] = []
    for input_name, reference in raw_inputs.items():
        path = f"observation_inputs.{input_name}"
        observation = _resolve_observation(reference, index, path)
        bindings[path] = observation.observation_id
        selected.append(observation)
        if input_name == "terminal_metric_value":
            terminal = arguments.get("terminal")
            if not isinstance(terminal, dict):
                raise ObservationBindingError(
                    "invalid_observation_binding",
                    "terminal_metric_value requires a terminal object",
                )
            if "metric_value" in terminal:
                raise ObservationBindingError(
                    "unbound_numeric_input",
                    "terminal.metric_value is controller-derived; supply only its observation reference",
                )
            terminal["metric_value"] = observation.value
        else:
            arguments[input_name] = observation.value
    terminal = arguments.get("terminal")
    if isinstance(terminal, dict) and "metric_value" in terminal and "terminal_metric_value" not in raw_inputs:
        raise ObservationBindingError(
            "unbound_numeric_input",
            "terminal.metric_value must be bound to a normalized observation",
        )
    arguments["unit"] = _unit_label(selected[0])
    _inject_observation_lineage(arguments, selected)
    return arguments, bindings


def _bind_market_statistics(
    proposed: Mapping[str, JsonValue],
    index: Mapping[str, FinancialObservation],
) -> tuple[dict[str, JsonValue], dict[str, JsonValue]]:
    arguments = _json_copy(proposed, "finance.market_statistics proposal")
    _reject_controller_fields(
        arguments,
        "finance.market_statistics",
        {"prices", "returns", "benchmark_returns", "source_ids", "context"},
    )
    raw_inputs = arguments.pop("observation_inputs", None)
    if not isinstance(raw_inputs, dict) or not raw_inputs:
        raise ObservationBindingError(
            "invalid_observation_binding",
            "finance.market_statistics requires observation_inputs.prices",
        )
    unknown_names = sorted(set(raw_inputs) - {"prices", "benchmark_prices"})
    if unknown_names:
        raise ObservationBindingError(
            "invalid_observation_binding",
            f"unknown market observation input names: {unknown_names!r}",
        )
    primary, bindings = _resolve_observation_series(
        raw_inputs.get("prices"),
        index,
        "observation_inputs.prices",
    )
    arguments["prices"] = [observation.value for observation in primary]
    selected = list(primary)
    raw_benchmark = raw_inputs.get("benchmark_prices")
    if raw_benchmark is not None:
        benchmark, benchmark_bindings = _resolve_observation_series(
            raw_benchmark,
            index,
            "observation_inputs.benchmark_prices",
        )
        if [item.period.end for item in benchmark] != [item.period.end for item in primary]:
            raise ObservationBindingError(
                "incompatible_observations",
                "asset and benchmark price periods must align exactly",
            )
        arguments["benchmark_returns"] = _simple_returns(benchmark)
        bindings.update(benchmark_bindings)
        selected.extend(benchmark)
    arguments["source_ids"] = list(
        dict.fromkeys(
            evidence_id
            for observation in selected
            for evidence_id in observation.evidence_ids
        )
    )
    arguments["context"] = _observation_context(primary)
    return arguments, bindings


def _resolve_observation_series(
    references: JsonValue,
    index: Mapping[str, FinancialObservation],
    path: str,
) -> tuple[list[FinancialObservation], dict[str, JsonValue]]:
    if not isinstance(references, list) or len(references) < 2:
        raise ObservationBindingError(
            "invalid_observation_binding",
            f"{path} must contain at least two ordered observation references",
        )
    resolved: list[FinancialObservation] = []
    bindings: dict[str, JsonValue] = {}
    for item_index, reference in enumerate(references):
        item_path = f"{path}[{item_index}]"
        observation = _resolve_observation(reference, index, item_path)
        if observation.value <= 0:
            raise ObservationBindingError(
                "incompatible_observations",
                f"{item_path} must reference a positive price",
            )
        resolved.append(observation)
        bindings[item_path] = observation.observation_id
    _validate_observation_compatibility(resolved)
    periods = [observation.period.end for observation in resolved]
    if any(current <= prior for prior, current in zip(periods, periods[1:], strict=False)):
        raise ObservationBindingError(
            "incompatible_observations",
            f"{path} periods must be strictly increasing",
        )
    return resolved, bindings


def _simple_returns(observations: Sequence[FinancialObservation]) -> list[JsonValue]:
    values: list[JsonValue] = []
    try:
        for prior, current in zip(observations, observations[1:], strict=False):
            values.append(current.value / prior.value - 1)
    except (ArithmeticError, OverflowError) as exc:
        raise ObservationBindingError(
            "incompatible_observations",
            f"benchmark prices cannot be converted to finite returns: {exc}",
        ) from exc
    try:
        _json_copy(values, "benchmark returns")
    except ValueError as exc:
        raise ObservationBindingError(
            "incompatible_observations",
            str(exc),
        ) from exc
    return values


def _resolve_observation(
    reference: JsonValue,
    index: Mapping[str, FinancialObservation],
    path: str,
) -> FinancialObservation:
    if not isinstance(reference, dict) or set(reference) != {"observation_id"}:
        raise ObservationBindingError(
            "unbound_numeric_input",
            f"{path} must be exactly {{'observation_id': '<trusted-id>'}}",
        )
    observation_id = reference.get("observation_id")
    if not isinstance(observation_id, str) or not observation_id.strip():
        raise ObservationBindingError(
            "invalid_observation_binding",
            f"{path}.observation_id must be a non-empty string",
        )
    try:
        return index[observation_id]
    except KeyError as exc:
        raise ObservationBindingError(
            "observation_not_allowed",
            f"{path} references an observation outside the trusted packet: {observation_id!r}",
        ) from exc


def _reject_controller_fields(
    arguments: Mapping[str, JsonValue],
    tool_name: str,
    fields: set[str],
) -> None:
    supplied = sorted(set(arguments) & fields)
    if supplied:
        code = (
            "controller_field_not_allowed"
            if supplied[0] in {"source_ids", "context", "unit"}
            else "unbound_numeric_input"
        )
        raise ObservationBindingError(
            code,
            f"{tool_name} controller-derived fields must not be model supplied: {supplied!r}",
        )


def _inject_observation_lineage(
    arguments: dict[str, JsonValue],
    observations: Sequence[FinancialObservation],
) -> None:
    if not observations:
        return
    _validate_observation_compatibility(observations)
    arguments["source_ids"] = list(
        dict.fromkeys(
            evidence_id
            for observation in observations
            for evidence_id in observation.evidence_ids
        )
    )
    arguments["context"] = _observation_context(observations)


def _validate_observation_compatibility(
    observations: Sequence[FinancialObservation],
) -> None:
    entities = {observation.entity_id for observation in observations}
    if len(entities) != 1:
        raise ObservationBindingError(
            "incompatible_observations",
            "one finance calculation cannot mix entities",
        )
    scaled = {
        observation.unit.scale
        for observation in observations
        if observation.unit.kind is not UnitKind.RATIO
    }
    if len(scaled) > 1:
        raise ObservationBindingError(
            "incompatible_observations",
            f"observation scales do not match: {sorted(scaled)!r}",
        )
    currencies = {
        observation.unit.symbol
        for observation in observations
        if observation.unit.kind in {UnitKind.CURRENCY, UnitKind.CURRENCY_PER_SHARE}
    }
    if len(currencies) > 1:
        raise ObservationBindingError(
            "incompatible_observations",
            f"observation currencies do not match: {sorted(currencies)!r}",
        )


def _observation_context(
    observations: Sequence[FinancialObservation],
) -> dict[str, JsonValue]:
    first = observations[0]
    context: dict[str, JsonValue] = {
        "entity_id": first.entity_id,
        "as_of": max(observation.period.end for observation in observations).isoformat(),
    }
    tickers = {observation.ticker for observation in observations if observation.ticker}
    if len(tickers) == 1:
        context["ticker"] = next(iter(tickers))
    currency_observations = [
        observation
        for observation in observations
        if observation.unit.kind in {UnitKind.CURRENCY, UnitKind.CURRENCY_PER_SHARE}
    ]
    if currency_observations:
        context["currency"] = currency_observations[0].unit.symbol
        context["scale"] = currency_observations[0].unit.scale
    if len(observations) >= 1:
        context["current_period"] = observations[0].period.end.isoformat()
    if len(observations) >= 2:
        context["prior_period"] = observations[1].period.end.isoformat()
    period_types = {_period_type(observation) for observation in observations}
    if len(period_types) == 1:
        context["period_type"] = next(iter(period_types))
    return context


def _period_type(observation: FinancialObservation) -> str:
    fiscal_period = observation.period.fiscal_period
    if fiscal_period == "FY":
        return "year"
    if fiscal_period is not None and fiscal_period.startswith("Q"):
        return "quarter"
    if fiscal_period == "TTM":
        return "trailing_twelve_months"
    return "other"


def _unit_label(observation: FinancialObservation) -> str:
    return f"{observation.unit.symbol} {observation.unit.scale}"


def _strict_source_ids(arguments: Mapping[str, JsonValue]) -> tuple[str, ...]:
    if "source_ids" not in arguments:
        return ()
    value = arguments["source_ids"]
    if (
        not isinstance(value, list)
        or len(value) > MAX_CITATION_IDS
        or any(
            not isinstance(item, str) or not item.strip() or len(item) > 200
            for item in value
        )
        or len(value) != len(set(value))
    ):
        raise ObservationBindingError(
            "invalid_source_ids",
            "source_ids must be a unique bounded array of non-empty strings",
        )
    return tuple(value)


def _binding_contract(allowed_tools: Sequence[str]) -> dict[str, JsonValue]:
    bound_tools = sorted(set(allowed_tools) & _OBSERVATION_BOUND_TOOLS)
    return {
        "mode": "normalized_observation_references",
        "tools": bound_tools,
        "reference": {"observation_id": "<id from evidence_packet.observations>"},
        "rules": [
            "Never copy numeric facts from evidence into bound tool arguments.",
            "Use observation_inputs for finance.metrics and finance.discounted_cash_flow.",
            "Use observation references for calculate operands and forecast base_value.",
            "The controller validates values, units, periods, context, and source_ids.",
        ],
    }


def _task_source_ids(
    task: TaskDefinition,
    outcomes: Mapping[str, _TaskOutcome],
    direct_source_ids: Sequence[str],
) -> tuple[str, ...]:
    values: list[str] = list(direct_source_ids)
    for dependency in task.depends_on:
        values.extend(outcomes[dependency].source_ids)
    return tuple(dict.fromkeys(values))


def _action_rejected(
    task: TaskDefinition,
    turn: int,
    code: str,
    message: str,
    output: JsonValue,
    *,
    repair_allowed: bool,
    repairs_remaining: int,
) -> EventDraft:
    return EventDraft(
        EventKind.ACTION_REJECTED,
        f"Rejected action for task {task.task_id}: {code}",
        agent_id=task.agent_id,
        payload={
            "task_id": task.task_id,
            "turn": turn,
            "code": code,
            "error": message,
            "output": output,
            "repair_allowed": repair_allowed,
            "repairs_remaining": repairs_remaining,
        },
    )


def _fail_task(
    task: TaskDefinition,
    outcome: _TaskOutcome,
    code: str,
    message: str,
) -> EventDraft:
    outcome.status = TaskStatus.FAILED
    outcome.error = message
    return EventDraft(
        EventKind.TASK_FAILED,
        f"Fixed task {task.task_id} failed: {code}",
        agent_id=task.agent_id,
        payload={
            "task_id": task.task_id,
            "code": code,
            "error": message,
            "turns": outcome.turns,
            "tool_calls": outcome.tool_calls,
        },
    )


def _repair_message(code: str, message: str, remaining: int) -> dict[str, JsonValue]:
    return {
        "role": "controller",
        "content": {
            "repair": {
                "code": code,
                "message": message,
                "repairs_remaining": remaining,
                "instruction": "Return one corrected JSON action only.",
            }
        },
    }


def _model_result_dict(
    result: ActionModelResult,
    max_action_bytes: int,
) -> dict[str, JsonValue]:
    output_bytes = len(result.output_text.encode("utf-8"))
    truncated = output_bytes > max_action_bytes
    return _json_copy(
        {
            "request_id": result.request_id,
            "output_text": None if truncated else result.output_text,
            "output_bytes": output_bytes,
            "output_hash": _text_hash(result.output_text),
            "output_truncated": truncated,
            "prompt_ids": list(result.prompt_ids),
            "output_ids": list(result.output_ids),
            "prompt_ids_hash": _canonical_hash(list(result.prompt_ids)),
            "output_ids_hash": _canonical_hash(list(result.output_ids)),
            "finish_reason": result.finish_reason,
            "telemetry": dict(result.telemetry),
            "model_fingerprint": result.model_fingerprint,
            "tokenizer_fingerprint": result.tokenizer_fingerprint,
        },
        "model result",
    )


def _model_result_error(
    result: Any,
    expected_request_id: str,
    *,
    max_action_bytes: int,
) -> tuple[str, str] | None:
    if not isinstance(result, ActionModelResult):
        return "invalid_model_result", "model must return an ActionModelResult"
    if not isinstance(result.request_id, str) or len(result.request_id) > MAX_MODEL_LABEL_LENGTH:
        return "invalid_model_result", "model result request_id must be bounded text"
    if result.request_id != expected_request_id:
        return "request_id_mismatch", "model result request_id does not match the active request"
    if not isinstance(result.output_text, str):
        return "invalid_model_result", "model result output_text must be text"
    try:
        output_bytes = len(result.output_text.encode("utf-8"))
    except UnicodeEncodeError:
        return "invalid_model_result", "model result output_text must be valid UTF-8"
    if output_bytes > max_action_bytes:
        return (
            "action_too_large",
            f"model result is {output_bytes} bytes; limit is {max_action_bytes}",
        )
    token_error = _token_ids_error(
        result.prompt_ids,
        "prompt_ids",
        maximum=MAX_MODEL_PROMPT_IDS,
    ) or _token_ids_error(
        result.output_ids,
        "output_ids",
        maximum=MAX_MODEL_OUTPUT_IDS,
    )
    if token_error is not None:
        return "invalid_model_result", token_error
    if not isinstance(result.telemetry, Mapping):
        return "invalid_model_result", "model telemetry must be an object"
    try:
        telemetry = _json_copy(dict(result.telemetry), "model telemetry")
    except ValueError as exc:
        return "invalid_model_result", str(exc)
    if len(_canonical_bytes(telemetry)) > MAX_MODEL_TELEMETRY_BYTES:
        return "invalid_model_result", "model telemetry exceeds its byte limit"
    for value, label in (
        (result.finish_reason, "finish_reason"),
        (result.model_fingerprint, "model_fingerprint"),
        (result.tokenizer_fingerprint, "tokenizer_fingerprint"),
    ):
        if value is not None and (
            not isinstance(value, str) or len(value) > MAX_MODEL_LABEL_LENGTH
        ):
            return "invalid_model_result", f"model {label} must be bounded text or null"
    try:
        trace = _model_result_dict(result, max_action_bytes)
    except (TypeError, ValueError, OverflowError) as exc:
        return "invalid_model_result", f"model result is not strict JSON: {exc}"
    if len(_canonical_bytes(trace)) > MAX_MODEL_TRACE_BYTES:
        return "invalid_model_result", "model result trace exceeds its byte limit"
    if result.finish_reason not in _NATURAL_STOPS:
        return "non_natural_stop", f"model finish reason is {result.finish_reason!r}"
    return None


def _token_ids_error(value: Any, label: str, *, maximum: int) -> str | None:
    if not isinstance(value, (tuple, list)) or len(value) > maximum:
        return f"model {label} must be an array with at most {maximum} items"
    if any(
        isinstance(item, bool)
        or not isinstance(item, int)
        or item < 0
        or item > MAX_MODEL_TOKEN_ID
        for item in value
    ):
        return f"model {label} must contain bounded non-negative integers"
    return None


def _invalid_model_result_trace(
    request_id: str,
    error: tuple[str, str],
) -> dict[str, JsonValue]:
    code, message = error
    return {
        "request_id": request_id,
        "output_text": "",
        "output_bytes": 0,
        "output_hash": _text_hash(""),
        "output_truncated": False,
        "prompt_ids": [],
        "output_ids": [],
        "prompt_ids_hash": _canonical_hash([]),
        "output_ids_hash": _canonical_hash([]),
        "finish_reason": "invalid_model_result",
        "telemetry": {"validation_error": {"code": code, "message": message}},
        "model_fingerprint": "invalid",
        "tokenizer_fingerprint": "invalid",
    }


def _tool_result_dict(result: ToolResult) -> dict[str, JsonValue]:
    return _json_copy(
        {
            "call_id": result.call_id,
            "payload": dict(result.payload),
            "source_ids": list(result.source_ids),
            "retryable": result.retryable,
        },
        "tool result",
    )


def _validate_tool_result(
    call: ToolCall,
    result: ToolResult,
    allowed_sources: tuple[str, ...],
) -> None:
    if result.call_id != call.call_id:
        raise ValueError("tool result call_id does not match the active call")
    if any(source_id not in allowed_sources for source_id in result.source_ids):
        raise ValueError("tool result returned a source ID outside the task allowlist")
    if result.payload.get("ok") not in {True, False}:
        raise ValueError("tool result payload must contain a boolean ok field")
    _tool_result_dict(result)


def _tool_error(value: Any) -> tuple[str, str, bool]:
    if not isinstance(value, Mapping):
        return "malformed_tool_error", "tool returned a malformed error payload", False
    code = str(value.get("code", "tool_rejected"))
    message = str(value.get("message", "tool rejected the call"))
    details = value.get("details")
    phase = details.get("phase") if isinstance(details, Mapping) else None
    return code, message, code == "invalid_schema" or phase == "schema"


def _raw_source_ids(arguments: Mapping[str, JsonValue]) -> list[JsonValue]:
    value = arguments.get("source_ids", [])
    return list(value) if isinstance(value, list) else []


def _cited_source_ids(value: JsonValue) -> tuple[str, ...]:
    found: list[str] = []

    def visit(item: JsonValue) -> None:
        if isinstance(item, dict):
            for key, child in item.items():
                if key in {"source_ids", "evidence_ids"}:
                    if (
                        not isinstance(child, list)
                        or len(child) > MAX_CITATION_IDS
                        or any(
                            not isinstance(citation, str)
                            or not citation.strip()
                            or len(citation) > 200
                            for citation in child
                        )
                        or len(child) != len(set(child))
                    ):
                        raise ObservationBindingError(
                            "invalid_citations",
                            f"{key} must be a unique bounded array of non-empty strings",
                        )
                    found.extend(child)
                else:
                    visit(child)
        elif isinstance(item, list):
            for child in item:
                visit(child)

    visit(value)
    if len(found) > MAX_CITATION_IDS:
        raise ObservationBindingError(
            "invalid_citations",
            f"final output contains more than {MAX_CITATION_IDS} citations",
        )
    return tuple(dict.fromkeys(found))


def _json_copy(value: Any, label: str) -> Any:
    try:
        return json.loads(
            json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
        )
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{label} is not strict JSON: {exc}") from exc


def _canonical_bytes(value: JsonValue) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _canonical_hash(value: JsonValue) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _text_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
