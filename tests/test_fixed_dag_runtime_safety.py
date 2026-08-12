from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from alpha_orchestration.controller import RunController
from alpha_orchestration.dag import TaskDefinition, WorkflowDefinition
from alpha_orchestration.data import (
    DataProvider,
    EvidencePacket,
    EvidenceRecord,
    FinancialObservation,
    FinancialPeriod,
    FinancialUnit,
    PeriodKind,
    UnitKind,
)
from alpha_orchestration.domain import EventKind, JsonValue, RunSpec, RunStatus, TaskStatus
from alpha_orchestration.fixed_dag import FixedDagRuntime
from alpha_orchestration.journal import JsonlJournal, MemoryJournal, load_events, replay
from alpha_orchestration.ports import ActionModelRequest, ActionModelResult
from alpha_orchestration.tools.finance import build_financial_tool_registry

EMPTY_SCHEMA: dict[str, JsonValue] = {
    "type": "object",
    "properties": {},
    "additionalProperties": False,
}
FINAL_SCHEMA: dict[str, JsonValue] = {
    "type": "object",
    "required": ["summary", "source_ids"],
    "properties": {
        "summary": {"type": "string", "minLength": 1},
        "source_ids": {
            "type": "array",
            "items": {"type": "string", "minLength": 1},
            "uniqueItems": True,
        },
    },
    "additionalProperties": False,
}
PERMISSIVE_CITATION_SCHEMA: dict[str, JsonValue] = {
    "type": "object",
    "additionalProperties": True,
}
EVIDENCE_ID = "ev:revenue"
CURRENT_OBSERVATION_ID = "obs:revenue:current"
PRIOR_OBSERVATION_ID = "obs:revenue:prior"


class ScriptedModel:
    def __init__(self, scripts: Mapping[str, Sequence[str]]) -> None:
        self.scripts = {task_id: tuple(outputs) for task_id, outputs in scripts.items()}
        self.requests: list[ActionModelRequest] = []

    async def complete(self, request: ActionModelRequest) -> ActionModelResult:
        self.requests.append(request)
        output = self.scripts[request.task_id][request.turn - 1]
        return valid_result(request, output)


class CallbackModel:
    def __init__(self, callback: Callable[[ActionModelRequest], Any]) -> None:
        self.callback = callback
        self.requests: list[ActionModelRequest] = []

    async def complete(self, request: ActionModelRequest) -> Any:
        self.requests.append(request)
        return self.callback(request)


class PoisoningModel:
    """Mutate every model-visible policy/data copy before returning actions."""

    def __init__(self, outputs: Sequence[str]) -> None:
        self.outputs = tuple(outputs)

    async def complete(self, request: ActionModelRequest) -> ActionModelResult:
        output_schema = cast(dict[str, JsonValue], request.output_schema)
        output_schema.clear()
        output_schema.update({"type": "object", "additionalProperties": True})

        for raw_contract in request.tool_contracts:
            contract = cast(dict[str, JsonValue], raw_contract)
            contract["input_schema"] = {
                "type": "object",
                "additionalProperties": True,
            }

        packet = cast(dict[str, JsonValue], request.evidence_packet)
        observations = cast(list[dict[str, JsonValue]], packet["observations"])
        for observation in observations:
            observation["value"] = 999
            observation["evidence_ids"] = ["ev:forged"]
        evidence = cast(list[dict[str, JsonValue]], packet["evidence"])
        evidence[0]["evidence_id"] = "ev:forged"
        return valid_result(request, self.outputs[request.turn - 1])


def valid_result(request: ActionModelRequest, output: str) -> ActionModelResult:
    return ActionModelResult(
        request_id=request.request_id,
        output_text=output,
        prompt_ids=(request.turn, 10),
        output_ids=tuple(output.encode("utf-8")),
        finish_reason="stop",
        telemetry={"fixture": True},
        model_fingerprint="safety-fixture-v1",
        tokenizer_fingerprint="bytes-v1",
    )


def final(payload: Mapping[str, JsonValue]) -> str:
    return json.dumps(
        {"kind": "final", "payload": dict(payload)},
        separators=(",", ":"),
    )


def calculate_action(*, precision: JsonValue | None = None) -> str:
    arguments: dict[str, JsonValue] = {
        "operations": [
            {
                "id": "growth",
                "operation": "percent_change",
                "current": {"observation_id": CURRENT_OBSERVATION_ID},
                "prior": {"observation_id": PRIOR_OBSERVATION_ID},
            }
        ]
    }
    if precision is not None:
        arguments["precision"] = precision
    return json.dumps(
        {
            "kind": "tool_calls",
            "calls": [{"name": "finance.calculate", "arguments": arguments}],
        },
        separators=(",", ":"),
    )


def evidence_packet() -> EvidencePacket:
    locator: dict[str, JsonValue] = {"filing": "fixture-10-k", "ticker": "ABC"}
    evidence = EvidenceRecord(
        evidence_id=EVIDENCE_ID,
        provider=DataProvider.SEC,
        source_kind="fixture_filing",
        source_locator=locator,
        source_url="https://example.test/fixture-10-k",
        observed_at=datetime(2024, 12, 31, tzinfo=UTC),
        retrieved_at=datetime(2025, 1, 2, tzinfo=UTC),
        content_hash=hashlib.sha256(
            json.dumps(locator, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    )

    def observation(
        observation_id: str,
        value: int,
        start: date,
        end: date,
    ) -> FinancialObservation:
        return FinancialObservation(
            observation_id=observation_id,
            entity_id="ticker:ABC",
            ticker="ABC",
            name="revenue",
            value=value,
            unit=FinancialUnit(UnitKind.CURRENCY, "USD", "millions"),
            period=FinancialPeriod(
                PeriodKind.DURATION,
                start=start,
                end=end,
                fiscal_year=end.year,
                fiscal_period="FY",
            ),
            evidence_ids=(EVIDENCE_ID,),
        )

    return EvidencePacket(
        observations=(
            observation(
                PRIOR_OBSERVATION_ID,
                100,
                date(2023, 1, 1),
                date(2023, 12, 31),
            ),
            observation(
                CURRENT_OBSERVATION_ID,
                125,
                date(2024, 1, 1),
                date(2024, 12, 31),
            ),
        ),
        evidence=(evidence,),
    )


def one_task(
    *,
    output_schema: Mapping[str, JsonValue] = FINAL_SCHEMA,
    allowed_tools: tuple[str, ...] = (),
    required: bool = True,
    max_turns: int = 1,
    repair_budget: int = 0,
) -> WorkflowDefinition:
    return WorkflowDefinition(
        "runtime-safety",
        "1.0.0",
        (
            TaskDefinition(
                "task",
                "agent",
                allowed_tools=allowed_tools,
                output_schema=output_schema,
                required=required,
                max_turns=max_turns,
                max_tool_calls=3,
                max_calls_per_turn=3,
                repair_budget=repair_budget,
            ),
        ),
        active_slots=4,
    )


def execute(
    workflow: WorkflowDefinition,
    model: Any,
    *,
    packets: Mapping[str, EvidencePacket] | None = None,
    journal: MemoryJournal | JsonlJournal | None = None,
    active_slots: int = 1,
):
    async def run():
        selected_journal = journal or MemoryJournal()
        runtime = FixedDagRuntime(
            workflow,
            model,
            build_financial_tool_registry(),
            evidence_packets_by_task=packets,
        )
        controller = RunController(
            RunSpec(
                run_id="run-runtime-safety",
                mode="fixed_fixture",
                agent_budget=4,
                active_slots=active_slots,
            ),
            runtime,
            selected_journal,
        )
        state = await controller.run()
        return state, selected_journal

    return asyncio.run(run())


def events_of(journal: MemoryJournal, kind: EventKind):
    return [event for event in journal.events if event.kind is kind]


def test_model_visible_policy_mutation_cannot_weaken_trusted_validation() -> None:
    model = PoisoningModel(
        (
            calculate_action(precision=True),
            final({}),
        )
    )

    state, journal = execute(
        one_task(
            allowed_tools=("finance.calculate",),
            max_turns=2,
            repair_budget=1,
        ),
        model,
        packets={"task": evidence_packet()},
    )

    assert isinstance(journal, MemoryJournal)
    rejections = events_of(journal, EventKind.ACTION_REJECTED)
    assert state.status is RunStatus.FAILED
    assert state.tasks["task"].status is TaskStatus.FAILED
    assert [event.payload["code"] for event in rejections] == [
        "invalid_schema",
        "invalid_final_output",
    ]
    assert events_of(journal, EventKind.TOOL_STARTED) == []


def test_model_visible_packet_mutation_cannot_change_bound_values_or_lineage() -> None:
    model = PoisoningModel(
        (
            calculate_action(),
            final({"summary": "Revenue grew 25%.", "source_ids": [EVIDENCE_ID]}),
        )
    )

    state, journal = execute(
        one_task(
            allowed_tools=("finance.calculate",),
            max_turns=2,
        ),
        model,
        packets={"task": evidence_packet()},
    )

    assert isinstance(journal, MemoryJournal)
    assert state.status is RunStatus.COMPLETE
    started = events_of(journal, EventKind.TOOL_STARTED)[0].payload
    operation = started["arguments"]["operations"][0]
    assert operation["current"] == 125
    assert operation["prior"] == 100
    assert started["arguments"]["source_ids"] == [EVIDENCE_ID]
    completed = events_of(journal, EventKind.TASK_COMPLETED)[0].payload
    assert completed["used_source_ids"] == [EVIDENCE_ID]


def malformed_result(case: str, request: ActionModelRequest) -> Any:
    if case == "wrong_type":
        return {"request_id": request.request_id, "output_text": final({})}
    if case == "wrong_request_id":
        return ActionModelResult(request_id="wrong-request", output_text=final({}))
    if case == "non_string_output":
        return ActionModelResult(
            request_id=request.request_id,
            output_text=cast(Any, 17),
        )
    if case == "surrogate_output":
        return ActionModelResult(request_id=request.request_id, output_text="\ud800")
    if case == "nan_telemetry":
        return ActionModelResult(
            request_id=request.request_id,
            output_text=final({}),
            telemetry={"latency": float("nan")},
        )
    if case == "oversized_token_id":
        return ActionModelResult(
            request_id=request.request_id,
            output_text=final({}),
            prompt_ids=(2**31,),
        )
    raise AssertionError(f"unknown malformed result fixture: {case}")


@pytest.mark.parametrize(
    ("case", "expected_code"),
    [
        ("wrong_type", "invalid_model_result"),
        ("wrong_request_id", "request_id_mismatch"),
        ("non_string_output", "invalid_model_result"),
        ("surrogate_output", "invalid_model_result"),
        ("nan_telemetry", "invalid_model_result"),
        ("oversized_token_id", "invalid_model_result"),
    ],
)
def test_malformed_model_result_terminalizes_with_replayable_contiguous_journal(
    tmp_path: Path,
    case: str,
    expected_code: str,
) -> None:
    path = tmp_path / f"{case}.jsonl"
    model = CallbackModel(lambda request: malformed_result(case, request))

    state, _ = execute(
        one_task(output_schema=EMPTY_SCHEMA),
        model,
        journal=JsonlJournal(path),
    )

    events = load_events(path)
    restored = replay(path)
    rejection = next(event for event in events if event.kind is EventKind.ACTION_REJECTED)
    assert state.status is RunStatus.FAILED
    assert state.tasks["task"].status is TaskStatus.FAILED
    assert rejection.payload["code"] == expected_code
    assert sum(event.kind is EventKind.TASK_FAILED for event in events) == 1
    assert [event.sequence for event in events] == list(range(len(events)))
    assert restored.status is state.status
    assert restored.tasks == state.tasks
    assert restored.last_sequence == state.last_sequence


def test_partial_status_contaminates_all_downstream_descendants() -> None:
    workflow = WorkflowDefinition(
        "partial-contagion",
        "1",
        (
            TaskDefinition(
                "a",
                "agent-a",
                output_schema=EMPTY_SCHEMA,
                required=False,
                max_turns=1,
                repair_budget=0,
            ),
            TaskDefinition(
                "b",
                "agent-b",
                depends_on=("a",),
                output_schema=EMPTY_SCHEMA,
                allow_failed_dependencies=True,
                max_turns=1,
                repair_budget=0,
            ),
            TaskDefinition(
                "c",
                "agent-c",
                depends_on=("b",),
                output_schema=EMPTY_SCHEMA,
                max_turns=1,
                repair_budget=0,
            ),
        ),
        active_slots=3,
    )
    model = ScriptedModel(
        {
            "a": ("not-json",),
            "b": (final({}),),
            "c": (final({}),),
        }
    )

    state, journal = execute(workflow, model, active_slots=3)

    assert isinstance(journal, MemoryJournal)
    assert state.status is RunStatus.COMPLETE
    assert state.tasks["a"].status is TaskStatus.FAILED
    assert state.tasks["b"].status is TaskStatus.PARTIAL
    assert state.tasks["c"].status is TaskStatus.PARTIAL
    completed = {
        event.payload["task_id"]: event.payload
        for event in events_of(journal, EventKind.TASK_COMPLETED)
    }
    assert completed["b"]["degraded_ancestry"] == ["a"]
    assert completed["c"]["degraded_ancestry"] == ["b", "a"]
    assert events_of(journal, EventKind.WORKFLOW_COMPLETED)[0].payload["partial"] is True


@pytest.mark.parametrize(
    "citations",
    [
        {"source_ids": EVIDENCE_ID},
        {"source_ids": [EVIDENCE_ID, EVIDENCE_ID]},
        {"nested": {"evidence_ids": [""]}},
        {"nested": {"evidence_ids": ["x" * 201]}},
        {
            "left": {"source_ids": [f"ev:left:{index}" for index in range(60)]},
            "right": {"evidence_ids": [f"ev:right:{index}" for index in range(60)]},
        },
    ],
    ids=[
        "not-an-array",
        "duplicate",
        "blank",
        "too-long",
        "aggregate-limit",
    ],
)
def test_malformed_citations_fail_closed(citations: Mapping[str, JsonValue]) -> None:
    model = ScriptedModel({"task": (final(citations),)})

    state, journal = execute(
        one_task(output_schema=PERMISSIVE_CITATION_SCHEMA),
        model,
    )

    assert isinstance(journal, MemoryJournal)
    rejection = events_of(journal, EventKind.ACTION_REJECTED)[0].payload
    assert state.status is RunStatus.FAILED
    assert state.tasks["task"].status is TaskStatus.FAILED
    assert rejection["code"] == "invalid_citations"
    assert rejection["repair_allowed"] is False


def test_workflow_plan_event_hashes_exact_nested_plan_and_reports_real_concurrency() -> None:
    workflow = one_task(output_schema=EMPTY_SCHEMA)
    model = ScriptedModel({"task": (final({}),)})

    state, journal = execute(workflow, model, active_slots=3)

    assert isinstance(journal, MemoryJournal)
    assert state.status is RunStatus.COMPLETE
    planned = events_of(journal, EventKind.WORKFLOW_PLANNED)[0].payload
    canonical_plan = workflow.to_dict()
    expected_hash = hashlib.sha256(
        json.dumps(
            canonical_plan,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    assert planned["plan"] == canonical_plan
    assert planned["tasks"] == canonical_plan["tasks"]
    assert planned["plan_hash"] == expected_hash == workflow.plan_hash
    assert planned["effective_active_slots"] == 3
    assert planned["active_slots"] == 3
    assert planned["actual_active_slots"] == 1


def test_optional_failure_completes_run_but_marks_workflow_partial() -> None:
    workflow = one_task(
        output_schema=EMPTY_SCHEMA,
        required=False,
    )
    model = ScriptedModel({"task": ("not-json",)})

    state, journal = execute(workflow, model)

    assert isinstance(journal, MemoryJournal)
    summary = events_of(journal, EventKind.WORKFLOW_COMPLETED)[0].payload
    assert state.status is RunStatus.COMPLETE
    assert state.tasks["task"].status is TaskStatus.FAILED
    assert summary["required_failures"] == []
    assert summary["counts"]["failed"] == 1
    assert summary["partial"] is True


def test_huge_integer_final_action_is_rejected_without_escaping_lifecycle() -> None:
    huge_integer = "9" * 5_000
    output = (
        '{"kind":"final","payload":{"summary":'
        + huge_integer
        + ',"source_ids":[]}}'
    )
    model = CallbackModel(
        lambda request: ActionModelResult(
            request_id=request.request_id,
            output_text=output,
            finish_reason="stop",
        )
    )

    state, journal = execute(one_task(), model)

    assert isinstance(journal, MemoryJournal)
    rejection = events_of(journal, EventKind.ACTION_REJECTED)[0].payload
    assert state.status is RunStatus.FAILED
    assert state.tasks["task"].status is TaskStatus.FAILED
    assert rejection["code"] == "invalid_json"
    assert rejection["repair_allowed"] is False
    assert len(events_of(journal, EventKind.TASK_FAILED)) == 1
