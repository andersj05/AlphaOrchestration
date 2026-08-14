"""Pure event-to-state reduction with strict sequence checks."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from alpha_orchestration.domain import (
    AgentState,
    AgentStatus,
    Candidate,
    CandidateBucket,
    CandidateConfidence,
    CandidateDataQuality,
    CandidateFinancial,
    CandidateSourceMode,
    EventKind,
    Evidence,
    RunEvent,
    RunState,
    RunStatus,
    Stage,
    StateInvariantError,
    TaskState,
    TaskStatus,
)


def _require(payload: dict[str, Any], key: str) -> Any:
    if key not in payload:
        raise StateInvariantError(f"event payload is missing {key!r}")
    return payload[key]


def _agent(state: RunState, event: RunEvent) -> AgentState:
    if event.agent_id is None:
        raise StateInvariantError(f"{event.kind.value} requires agent_id")
    try:
        return state.agents[event.agent_id]
    except KeyError as exc:
        raise StateInvariantError(f"unknown agent: {event.agent_id}") from exc


_TASK_SUCCESS = frozenset({TaskStatus.COMPLETE, TaskStatus.PARTIAL})
_TASK_TERMINAL = frozenset(
    {
        TaskStatus.COMPLETE,
        TaskStatus.PARTIAL,
        TaskStatus.FAILED,
        TaskStatus.SKIPPED,
    }
)
_TASK_ACTIVE = frozenset(
    {
        TaskStatus.RUNNING,
        TaskStatus.WAITING_MODEL,
        TaskStatus.WAITING_TOOL,
    }
)


def _nonempty_string(payload: dict[str, Any], key: str) -> str:
    value = _require(payload, key)
    if not isinstance(value, str) or not value.strip():
        raise StateInvariantError(f"event payload {key!r} must be a non-empty string")
    return value.strip()


def _optional_nonempty_string(payload: dict[str, Any], key: str, default: str) -> str:
    if key not in payload:
        return default
    return _nonempty_string(payload, key)


def _optional_string(payload: dict[str, Any], key: str, default: str) -> str:
    if key not in payload:
        return default
    value = payload[key]
    if not isinstance(value, str):
        raise StateInvariantError(f"event payload {key!r} must be a string")
    return value.strip() or default


def _string_tuple(payload: dict[str, Any], key: str) -> tuple[str, ...]:
    raw_values = payload.get(key, [])
    if not isinstance(raw_values, list):
        raise StateInvariantError(f"event payload {key!r} must be a list")
    values: list[str] = []
    for raw_value in raw_values:
        if not isinstance(raw_value, str) or not raw_value.strip():
            raise StateInvariantError(f"event payload {key!r} must contain non-empty strings")
        values.append(raw_value.strip())
    if len(values) != len(set(values)):
        raise StateInvariantError(f"event payload {key!r} must contain unique values")
    return tuple(values)


def _candidate_financials(payload: dict[str, Any]) -> tuple[CandidateFinancial, ...]:
    raw_financials = payload.get("financials", [])
    if not isinstance(raw_financials, list):
        raise StateInvariantError("candidate financials must be a list")
    financials: list[CandidateFinancial] = []
    for raw_financial in raw_financials:
        if not isinstance(raw_financial, dict):
            raise StateInvariantError("candidate financials must contain objects")
        raw_value = _require(raw_financial, "value")
        if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)):
            raise StateInvariantError("candidate financial value must be numeric")
        try:
            financials.append(
                CandidateFinancial(
                    metric=_nonempty_string(raw_financial, "metric"),
                    label=_nonempty_string(raw_financial, "label"),
                    value=float(raw_value),
                    unit=_nonempty_string(raw_financial, "unit"),
                    period=_nonempty_string(raw_financial, "period"),
                    source_ids=_string_tuple(raw_financial, "source_ids"),
                )
            )
        except ValueError as exc:
            raise StateInvariantError(str(exc)) from exc
    return tuple(financials)


def _task(state: RunState, event: RunEvent) -> TaskState:
    task_id = _nonempty_string(event.payload, "task_id")
    try:
        task = state.tasks[task_id]
    except KeyError as exc:
        raise StateInvariantError(f"unknown task: {task_id}") from exc
    if event.agent_id is not None and event.agent_id != task.agent_id:
        raise StateInvariantError(f"event agent {event.agent_id!r} does not own task {task_id!r}")
    return task


def _transition(
    task: TaskState,
    event: RunEvent,
    allowed: frozenset[TaskStatus],
) -> None:
    if task.status not in allowed:
        expected = ", ".join(sorted(status.value for status in allowed))
        raise StateInvariantError(
            f"invalid {event.kind.value} transition for task {task.task_id!r}: {task.status.value}; expected {expected}"
        )


def _task_error(event: RunEvent) -> str:
    error = event.payload.get("error", event.payload.get("reason", event.message))
    return str(error)


def _planned_tasks(payload: dict[str, Any]) -> dict[str, TaskState]:
    raw_tasks = _require(payload, "tasks")
    if not isinstance(raw_tasks, list) or not raw_tasks:
        raise StateInvariantError("workflow_planned tasks must be a non-empty list")

    tasks: dict[str, TaskState] = {}
    for raw_task in raw_tasks:
        if not isinstance(raw_task, dict):
            raise StateInvariantError("workflow_planned tasks must be objects")
        task_id = _nonempty_string(raw_task, "task_id")
        if task_id in tasks:
            raise StateInvariantError(f"duplicate task: {task_id}")
        agent_id = _nonempty_string(raw_task, "agent_id")
        raw_dependencies = _require(raw_task, "depends_on")
        if not isinstance(raw_dependencies, list):
            raise StateInvariantError(f"task {task_id!r} depends_on must be a list")
        dependencies: list[str] = []
        for dependency in raw_dependencies:
            if not isinstance(dependency, str) or not dependency.strip():
                raise StateInvariantError(f"task {task_id!r} dependencies must be non-empty strings")
            dependencies.append(dependency.strip())
        if len(dependencies) != len(set(dependencies)):
            raise StateInvariantError(f"task {task_id!r} has duplicate dependencies")
        required = _require(raw_task, "required")
        if not isinstance(required, bool):
            raise StateInvariantError(f"task {task_id!r} required must be a boolean")
        allow_failed_dependencies = raw_task.get("allow_failed_dependencies", False)
        if not isinstance(allow_failed_dependencies, bool):
            raise StateInvariantError(f"task {task_id!r} allow_failed_dependencies must be a boolean")
        tasks[task_id] = TaskState(
            task_id=task_id,
            agent_id=agent_id,
            depends_on=tuple(dependencies),
            required=required,
            allow_failed_dependencies=allow_failed_dependencies,
        )

    for task in tasks.values():
        unknown = sorted(set(task.depends_on) - tasks.keys())
        if unknown:
            raise StateInvariantError(f"task {task.task_id!r} has unknown dependencies: {', '.join(unknown)}")

    pending = set(tasks)
    while pending:
        ready = {
            task_id for task_id in pending if all(dependency not in pending for dependency in tasks[task_id].depends_on)
        }
        if not ready:
            raise StateInvariantError("workflow task dependencies contain a cycle")
        pending.difference_update(ready)
    return tasks


def reduce_event(state: RunState, event: RunEvent) -> RunState:
    """Return the next immutable run state for one verified event."""

    if event.schema_version != 1:
        raise StateInvariantError(f"unsupported event schema: {event.schema_version}")
    if event.run_id != state.spec.run_id:
        raise StateInvariantError(f"event run_id {event.run_id!r} does not match {state.spec.run_id!r}")
    expected_sequence = state.last_sequence + 1
    if event.sequence != expected_sequence:
        raise StateInvariantError(f"event sequence {event.sequence} does not match expected {expected_sequence}")
    if state.terminal:
        raise StateInvariantError("cannot append an event after terminal run state")

    next_state = replace(
        state,
        last_sequence=event.sequence,
        recent_events=(*state.recent_events[-79:], event),
    )
    payload: dict[str, Any] = event.payload

    if event.kind is EventKind.RUN_CREATED:
        return replace(next_state, status=RunStatus.PLANNING)
    if event.kind is EventKind.RUN_STARTED:
        return replace(next_state, status=RunStatus.RUNNING)
    if event.kind is EventKind.RUN_PAUSED:
        return replace(next_state, status=RunStatus.PAUSED)
    if event.kind is EventKind.RUN_RESUMED:
        return replace(next_state, status=RunStatus.RUNNING)
    if event.kind is EventKind.RUN_SYNTHESIZING:
        return replace(
            next_state,
            status=RunStatus.SYNTHESIZING,
            current_stage=Stage.SYNTHESIS,
            progress=max(next_state.progress, int(payload.get("progress", 82))),
        )
    if event.kind is EventKind.RUN_COMPLETED:
        return replace(
            next_state,
            status=RunStatus.COMPLETE,
            current_stage=Stage.REVIEW,
            completed_stages=tuple(dict.fromkeys((*state.completed_stages, Stage.SYNTHESIS))),
            progress=100,
        )
    if event.kind is EventKind.RUN_CANCELLED:
        agents = {
            key: replace(agent, status=AgentStatus.CANCELLED)
            if agent.status in {AgentStatus.QUEUED, AgentStatus.RUNNING, AgentStatus.WAITING_TOOL}
            else agent
            for key, agent in state.agents.items()
        }
        return replace(next_state, status=RunStatus.CANCELLED, agents=agents)
    if event.kind is EventKind.RUN_FAILED:
        return replace(
            next_state,
            status=RunStatus.FAILED,
            failure=str(payload.get("error", event.message)),
        )
    if event.kind is EventKind.STAGE_STARTED:
        return replace(
            next_state,
            current_stage=Stage(str(_require(payload, "stage"))),
            progress=max(0, min(100, int(payload.get("progress", state.progress)))),
        )
    if event.kind is EventKind.STAGE_COMPLETED:
        stage = Stage(str(_require(payload, "stage")))
        completed = tuple(dict.fromkeys((*state.completed_stages, stage)))
        return replace(
            next_state,
            completed_stages=completed,
            progress=max(0, min(100, int(payload.get("progress", state.progress)))),
        )
    if event.kind is EventKind.WORKFLOW_PLANNED:
        if state.workflow_id is not None or state.tasks:
            raise StateInvariantError("workflow already planned")
        workflow_id = _nonempty_string(payload, "workflow_id")
        workflow_version = _nonempty_string(payload, "workflow_version")
        return replace(
            next_state,
            workflow_id=workflow_id,
            workflow_version=workflow_version,
            tasks=_planned_tasks(payload),
        )

    if event.kind is EventKind.WORKFLOW_COMPLETED:
        if state.workflow_id is None:
            raise StateInvariantError("workflow_completed requires a planned workflow")
        incomplete = sorted(task.task_id for task in state.tasks.values() if task.status not in _TASK_TERMINAL)
        if incomplete:
            raise StateInvariantError(f"workflow completed with non-terminal tasks: {', '.join(incomplete)}")
        return next_state

    task_events = {
        EventKind.TASK_STARTED,
        EventKind.MODEL_TURN_STARTED,
        EventKind.MODEL_TURN_COMPLETED,
        EventKind.ACTION_REJECTED,
        EventKind.TASK_COMPLETED,
        EventKind.TASK_FAILED,
        EventKind.TASK_SKIPPED,
    }
    task_tool_events = {
        EventKind.TOOL_STARTED,
        EventKind.TOOL_COMPLETED,
        EventKind.TOOL_REJECTED,
        EventKind.TOOL_FAILED,
    }
    if event.kind in task_events or (event.kind in task_tool_events and "task_id" in payload):
        task = _task(state, event)
        updated = task

        if event.kind is EventKind.TASK_STARTED:
            _transition(task, event, frozenset({TaskStatus.QUEUED}))
            allowed_dependency_states = _TASK_TERMINAL if task.allow_failed_dependencies else _TASK_SUCCESS
            blocked = sorted(
                dependency
                for dependency in task.depends_on
                if state.tasks[dependency].status not in allowed_dependency_states
            )
            if blocked:
                raise StateInvariantError(
                    f"task {task.task_id!r} started before dependencies completed: {', '.join(blocked)}"
                )
            updated = replace(task, status=TaskStatus.RUNNING, error=None)
        elif event.kind is EventKind.MODEL_TURN_STARTED:
            _transition(task, event, frozenset({TaskStatus.RUNNING}))
            updated = replace(
                task,
                status=TaskStatus.WAITING_MODEL,
                turns=task.turns + 1,
                error=None,
            )
        elif event.kind is EventKind.MODEL_TURN_COMPLETED:
            _transition(task, event, frozenset({TaskStatus.WAITING_MODEL}))
            updated = replace(
                task,
                status=TaskStatus.RUNNING,
                output=payload.get("output", task.output),
                error=None,
            )
        elif event.kind is EventKind.ACTION_REJECTED:
            _transition(task, event, frozenset({TaskStatus.RUNNING}))
            updated = replace(
                task,
                output=payload.get("output", task.output),
                error=_task_error(event),
            )
        elif event.kind is EventKind.TOOL_STARTED:
            _transition(task, event, frozenset({TaskStatus.RUNNING}))
            updated = replace(
                task,
                status=TaskStatus.WAITING_TOOL,
                tool_calls=task.tool_calls + 1,
                error=None,
            )
        elif event.kind is EventKind.TOOL_COMPLETED:
            _transition(task, event, frozenset({TaskStatus.WAITING_TOOL}))
            updated = replace(task, status=TaskStatus.RUNNING, error=None)
        elif event.kind in {EventKind.TOOL_REJECTED, EventKind.TOOL_FAILED}:
            _transition(task, event, frozenset({TaskStatus.WAITING_TOOL}))
            updated = replace(
                task,
                status=TaskStatus.RUNNING,
                error=_task_error(event),
            )
        elif event.kind is EventKind.TASK_COMPLETED:
            _transition(task, event, frozenset({TaskStatus.RUNNING}))
            partial = payload.get("partial", False)
            if not isinstance(partial, bool):
                raise StateInvariantError("task_completed partial must be a boolean")
            error = str(payload["error"]) if "error" in payload else task.error if partial else None
            updated = replace(
                task,
                status=TaskStatus.PARTIAL if partial else TaskStatus.COMPLETE,
                output=payload.get("output", task.output),
                error=error,
            )
        elif event.kind is EventKind.TASK_FAILED:
            _transition(task, event, _TASK_ACTIVE)
            updated = replace(
                task,
                status=TaskStatus.FAILED,
                output=payload.get("output", task.output),
                error=_task_error(event),
            )
        else:
            _transition(task, event, frozenset({TaskStatus.QUEUED}))
            updated = replace(
                task,
                status=TaskStatus.SKIPPED,
                error=_task_error(event),
            )

        tasks = dict(state.tasks)
        tasks[task.task_id] = updated
        agents = dict(state.agents)
        current_agent = agents.get(task.agent_id)
        if current_agent is not None:
            waiting_for_tool = event.kind is EventKind.TOOL_STARTED
            agents[task.agent_id] = replace(
                current_agent,
                status=(AgentStatus.WAITING_TOOL if waiting_for_tool else AgentStatus.RUNNING),
                current_task=event.message,
                tool_calls=(current_agent.tool_calls + 1 if waiting_for_tool else current_agent.tool_calls),
            )
        return replace(next_state, tasks=tasks, agents=agents)

    if event.kind is EventKind.AGENT_REGISTERED:
        if event.agent_id is None:
            raise StateInvariantError("agent_registered requires agent_id")
        if event.agent_id in state.agents:
            raise StateInvariantError(f"agent already registered: {event.agent_id}")
        agents = dict(state.agents)
        agents[event.agent_id] = AgentState(
            agent_id=event.agent_id,
            role=str(_require(payload, "role")),
            lane=str(_require(payload, "lane")),
        )
        return replace(next_state, agents=agents)

    if event.kind in {
        EventKind.AGENT_STARTED,
        EventKind.AGENT_PROGRESS,
        EventKind.AGENT_COMPLETED,
        EventKind.AGENT_FAILED,
        EventKind.TOOL_STARTED,
        EventKind.TOOL_COMPLETED,
        EventKind.TOOL_REJECTED,
        EventKind.TOOL_FAILED,
        EventKind.EVIDENCE_ADDED,
    }:
        current = _agent(state, event)
        agents = dict(state.agents)
        if event.kind is EventKind.AGENT_STARTED:
            agents[current.agent_id] = replace(
                current,
                status=AgentStatus.RUNNING,
                current_task=event.message,
                progress=max(current.progress, int(payload.get("progress", 5))),
            )
        elif event.kind is EventKind.AGENT_PROGRESS:
            agents[current.agent_id] = replace(
                current,
                status=AgentStatus.RUNNING,
                current_task=event.message,
                progress=max(0, min(100, int(payload.get("progress", current.progress)))),
            )
        elif event.kind is EventKind.AGENT_COMPLETED:
            agents[current.agent_id] = replace(
                current,
                status=AgentStatus.COMPLETE,
                current_task=event.message,
                progress=100,
            )
        elif event.kind is EventKind.AGENT_FAILED:
            agents[current.agent_id] = replace(
                current,
                status=AgentStatus.FAILED,
                current_task=event.message,
            )
        elif event.kind is EventKind.TOOL_STARTED:
            agents[current.agent_id] = replace(
                current,
                status=AgentStatus.WAITING_TOOL,
                current_task=event.message,
                tool_calls=current.tool_calls + 1,
            )
        elif event.kind in {
            EventKind.TOOL_COMPLETED,
            EventKind.TOOL_REJECTED,
            EventKind.TOOL_FAILED,
        }:
            agents[current.agent_id] = replace(
                current,
                status=AgentStatus.RUNNING,
                current_task=event.message,
            )
        else:
            evidence = Evidence(
                evidence_id=str(_require(payload, "evidence_id")),
                title=str(_require(payload, "title")),
                source=str(_require(payload, "source")),
                source_kind=str(_require(payload, "source_kind")),
                summary=str(_require(payload, "summary")),
                observed_at=str(_require(payload, "observed_at")),
                synthetic=bool(payload.get("synthetic", False)),
                retrieved_at=_optional_string(payload, "retrieved_at", "not provided"),
                source_url=_optional_string(payload, "source_url", ""),
            )
            evidence_map = dict(state.evidence)
            evidence_map[evidence.evidence_id] = evidence
            agents[current.agent_id] = replace(
                current,
                evidence_count=current.evidence_count + 1,
                current_task=event.message,
            )
            return replace(next_state, agents=agents, evidence=evidence_map)
        return replace(next_state, agents=agents)

    if event.kind is EventKind.CANDIDATE_UPDATED:
        try:
            candidate = Candidate(
                candidate_id=_nonempty_string(payload, "candidate_id"),
                ticker=_nonempty_string(payload, "ticker"),
                company=_nonempty_string(payload, "company"),
                bucket=CandidateBucket(_nonempty_string(payload, "bucket")),
                priority_score=int(_require(payload, "priority_score")),
                variant_wedge=_nonempty_string(payload, "variant_wedge"),
                why_now=_nonempty_string(payload, "why_now"),
                first_rejection=_nonempty_string(payload, "first_rejection"),
                investable_if=_nonempty_string(payload, "investable_if"),
                kill_if=_nonempty_string(payload, "kill_if"),
                next_workflow=_nonempty_string(payload, "next_workflow"),
                evidence_ids=_string_tuple(payload, "evidence_ids"),
                financials=_candidate_financials(payload),
                confidence=CandidateConfidence(str(payload.get("confidence", CandidateConfidence.NOT_ASSESSED.value))),
                data_quality=CandidateDataQuality(
                    str(payload.get("data_quality", CandidateDataQuality.NOT_ASSESSED.value))
                ),
                as_of=_optional_nonempty_string(payload, "as_of", "not provided"),
                source_mode=CandidateSourceMode(str(payload.get("source_mode", CandidateSourceMode.UNSPECIFIED.value))),
                evidence_gaps=_string_tuple(payload, "evidence_gaps"),
            )
        except (TypeError, ValueError) as exc:
            raise StateInvariantError(f"invalid candidate payload: {exc}") from exc
        candidates = dict(state.candidates)
        candidates[candidate.candidate_id] = candidate
        return replace(next_state, candidates=candidates)

    raise StateInvariantError(f"unhandled event kind: {event.kind.value}")
