import asyncio
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path

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
from alpha_orchestration.journal import JsonlJournal, MemoryJournal, replay
from alpha_orchestration.ports import ActionModelRequest, ActionModelResult
from alpha_orchestration.tools.finance import build_financial_tool_registry

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
EMPTY_SCHEMA: dict[str, JsonValue] = {
    "type": "object",
    "properties": {},
    "additionalProperties": False,
}
EVIDENCE_ID = "ev:revenue"
CURRENT_OBSERVATION_ID = "obs:revenue:current"
PRIOR_OBSERVATION_ID = "obs:revenue:prior"



class ScriptedActionModel:
    def __init__(self, scripts: Mapping[str, Sequence[str]]) -> None:
        self.scripts = {task_id: tuple(outputs) for task_id, outputs in scripts.items()}
        self.requests: list[ActionModelRequest] = []

    async def complete(self, request: ActionModelRequest) -> ActionModelResult:
        self.requests.append(request)
        output = self.scripts[request.task_id][request.turn - 1]
        return ActionModelResult(
            request_id=request.request_id,
            output_text=output,
            prompt_ids=(request.turn, 10),
            output_ids=tuple(output.encode("utf-8")),
            finish_reason="stop",
            telemetry={"fixture": True, "turn": request.turn},
            model_fingerprint="scripted-v1",
            tokenizer_fingerprint="bytes-v1",
        )


class PoisonActionModel:
    def __init__(self) -> None:
        self.calls = 0

    async def complete(self, request: ActionModelRequest) -> ActionModelResult:
        del request
        self.calls += 1
        raise AssertionError("replay must never execute a model")


def final(summary: str, *source_ids: str) -> str:
    return json.dumps(
        {
            "kind": "final",
            "payload": {"summary": summary, "source_ids": list(source_ids)},
        },
        separators=(",", ":"),
    )


def evidence_packet(*, current: int | float = 125, prior: int | float = 100) -> EvidencePacket:
    locator: dict[str, JsonValue] = {"filing": "fixture-10-k", "ticker": "ABC"}
    evidence = EvidenceRecord(
        evidence_id=EVIDENCE_ID,
        provider=DataProvider.SEC,
        source_kind="fixture_filing",
        source_locator=locator,
        source_url="https://example.test/fixture-10-k",
        observed_at=datetime(2024, 12, 31, tzinfo=UTC),
        retrieved_at=datetime(2025, 1, 2, tzinfo=UTC),
        content_hash=sha256(
            json.dumps(locator, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    )

    def observation(
        observation_id: str,
        value: int | float,
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
                prior,
                date(2023, 1, 1),
                date(2023, 12, 31),
            ),
            observation(
                CURRENT_OBSERVATION_ID,
                current,
                date(2024, 1, 1),
                date(2024, 12, 31),
            ),
        ),
        evidence=(evidence,),
    )


def tool_action(
    *,
    source_ids: Sequence[str] | None = None,
    tool: str = "finance.calculate",
    current: JsonValue = None,
    precision: JsonValue | None = None,
) -> str:
    if current is None:
        current = {"observation_id": CURRENT_OBSERVATION_ID}
    arguments: dict[str, JsonValue] = {
        "operations": [
            {
                "id": "growth",
                "operation": "percent_change",
                "current": current,
                "prior": {"observation_id": PRIOR_OBSERVATION_ID},
            }
        ]
    }
    if source_ids is not None:
        arguments["source_ids"] = list(source_ids)
    if precision is not None:
        arguments["precision"] = precision
    return json.dumps(
        {
            "kind": "tool_calls",
            "calls": [{"name": tool, "arguments": arguments}],
        },
        separators=(",", ":"),
    )


def one_task(
    *,
    max_turns: int = 3,
    repair_budget: int = 1,
    allowed_tools: tuple[str, ...] = ("finance.calculate",),
) -> WorkflowDefinition:
    return WorkflowDefinition(
        "fixed-test",
        "1.0.0",
        (
            TaskDefinition(
                "metrics",
                "fundamentals",
                allowed_tools=allowed_tools,
                output_schema=FINAL_SCHEMA,
                max_turns=max_turns,
                max_tool_calls=3,
                max_calls_per_turn=3,
                repair_budget=repair_budget,
            ),
        ),
        active_slots=1,
    )


def execute(
    workflow: WorkflowDefinition,
    scripts: Mapping[str, Sequence[str]],
    *,
    packets: Mapping[str, EvidencePacket] | None = None,
    journal: MemoryJournal | JsonlJournal | None = None,
):
    async def run():
        model = ScriptedActionModel(scripts)
        selected_journal = journal or MemoryJournal()
        runtime = FixedDagRuntime(
            workflow,
            model,
            build_financial_tool_registry(),
            evidence_packets_by_task=packets,
        )
        controller = RunController(
            RunSpec(run_id="run-fixed", mode="fixed_fixture"),
            runtime,
            selected_journal,
        )
        state = await controller.run()
        return state, selected_journal, model

    return asyncio.run(run())


def events_of(journal: MemoryJournal, kind: EventKind):
    return [event for event in journal.events if event.kind is kind]


def test_success_journals_exact_model_and_tool_lifecycle() -> None:
    state, journal, model = execute(
        one_task(),
        {"metrics": (tool_action(), final("Revenue grew 25%.", "ev:revenue"))},
        packets={"metrics": evidence_packet()},
    )

    assert isinstance(journal, MemoryJournal)
    assert state.status is RunStatus.COMPLETE
    assert state.tasks["metrics"].status is TaskStatus.COMPLETE
    assert state.tasks["metrics"].turns == 2
    assert state.tasks["metrics"].tool_calls == 1
    started = events_of(journal, EventKind.TOOL_STARTED)[0].payload
    completed = events_of(journal, EventKind.TOOL_COMPLETED)[0].payload
    assert started["call_id"] == "run-fixed:metrics:t1:c0"
    assert started["tool"] == "finance.calculate"
    assert started["tool_version"] == "1.0.0"
    assert started["arguments"]["source_ids"] == ["ev:revenue"]
    assert len(started["arguments_hash"]) == 64
    assert completed["call_id"] == started["call_id"]
    assert completed["source_ids"] == ["ev:revenue"]
    assert completed["result"]["data"]["results"][0]["value"] == 0.25
    assert len(completed["result_hash"]) == 64
    model_start = events_of(journal, EventKind.MODEL_TURN_STARTED)[0].payload
    model_end = events_of(journal, EventKind.MODEL_TURN_COMPLETED)[0].payload
    assert model_start["request_id"] == "run-fixed:metrics:t1"
    assert len(model_start["request_hash"]) == 64
    assert model_end["trace"]["prompt_ids"] == [1, 10]
    assert model_end["trace"]["model_fingerprint"] == "scripted-v1"
    assert len(model_end["trace_hash"]) == 64
    assert model.requests[1].transcript[-1]["role"] == "tool"
    summary = events_of(journal, EventKind.WORKFLOW_COMPLETED)[0].payload
    assert summary["counts"]["complete"] == 1
    assert summary["required_failures"] == []


def test_one_shared_repair_allows_syntax_recovery() -> None:
    state, journal, model = execute(
        one_task(max_turns=2),
        {"metrics": ("not-json", final("Recovered.", "ev:revenue"))},
        packets={"metrics": evidence_packet()},
    )

    rejection = events_of(journal, EventKind.ACTION_REJECTED)[0].payload
    assert state.status is RunStatus.COMPLETE
    assert rejection["code"] == "invalid_json"
    assert rejection["repair_allowed"] is True
    assert rejection["repairs_remaining"] == 0
    repair = model.requests[1].transcript[-1]["content"]["repair"]
    assert repair["repairs_remaining"] == 0


def test_shared_repair_exhaustion_fails_required_task_and_run() -> None:
    state, journal, _ = execute(
        one_task(max_turns=2),
        {"metrics": ("not-json", '{"kind":"final","payload":{}}')},
        packets={"metrics": evidence_packet()},
    )

    rejections = events_of(journal, EventKind.ACTION_REJECTED)
    assert state.status is RunStatus.FAILED
    assert state.tasks["metrics"].status is TaskStatus.FAILED
    assert [event.payload["code"] for event in rejections] == [
        "invalid_json",
        "invalid_final_output",
    ]
    assert rejections[1].payload["repair_allowed"] is False
    assert events_of(journal, EventKind.WORKFLOW_COMPLETED)[0].payload[
        "required_failures"
    ] == ["metrics"]


def test_tool_schema_error_can_spend_the_single_repair() -> None:
    state, journal, _ = execute(
        one_task(max_turns=2),
        {
            "metrics": (
                tool_action(precision=True),
                final("Used the repair.", "ev:revenue"),
            )
        },
        packets={"metrics": evidence_packet()},
    )

    rejection = events_of(journal, EventKind.ACTION_REJECTED)[0].payload
    assert state.status is RunStatus.COMPLETE
    assert events_of(journal, EventKind.TOOL_STARTED) == []
    assert rejection["code"] == "invalid_schema"
    assert rejection["repair_allowed"] is True


def test_semantic_tool_error_is_not_repaired() -> None:
    action = json.dumps(
        {
            "kind": "tool_calls",
            "calls": [
                {
                    "name": "finance.calculate",
                    "arguments": {
                        "operations": [
                            {
                                "id": "bad-ratio",
                                "operation": "ratio",
                                "numerator": {
                                    "observation_id": CURRENT_OBSERVATION_ID
                                },
                                "denominator": {
                                    "observation_id": PRIOR_OBSERVATION_ID
                                },
                            }
                        ],
                    },
                }
            ],
        },
        separators=(",", ":"),
    )
    state, journal, model = execute(
        one_task(max_turns=2),
        {"metrics": (action, final("must not run", "ev:revenue"))},
        packets={"metrics": evidence_packet(current=5, prior=0)},
    )

    tool_error = events_of(journal, EventKind.TOOL_REJECTED)[0].payload["error"]
    rejection = events_of(journal, EventKind.ACTION_REJECTED)[0].payload
    assert state.status is RunStatus.FAILED
    assert tool_error["code"] == "invalid_arguments"
    assert tool_error["details"]["phase"] == "semantic"
    assert rejection["code"] == "invalid_arguments"
    assert rejection["repair_allowed"] is False
    assert rejection["repairs_remaining"] == 1
    assert len(model.requests) == 1


@pytest.mark.parametrize(
    ("action", "expected_code"),
    [
        (tool_action(tool="finance.metrics"), "tool_not_allowed"),
        (tool_action(source_ids=("ev:untrusted",)), "source_not_allowed"),
    ],
)
def test_tool_and_source_allowlists_fail_closed(
    action: str,
    expected_code: str,
) -> None:
    state, journal, model = execute(
        one_task(max_turns=2),
        {"metrics": (action, final("must not run", "ev:revenue"))},
        packets={"metrics": evidence_packet()},
    )

    rejection = events_of(journal, EventKind.ACTION_REJECTED)[0].payload
    assert state.status is RunStatus.FAILED
    assert rejection["code"] == expected_code
    assert rejection["repair_allowed"] is False
    assert len(model.requests) == 1


def test_dependency_failure_skips_normal_task_but_allows_explicit_partial() -> None:
    workflow = WorkflowDefinition(
        "partial-test",
        "1",
        (
            TaskDefinition(
                "facts",
                "facts-agent",
                output_schema=EMPTY_SCHEMA,
                required=False,
                max_turns=1,
                repair_budget=0,
            ),
            TaskDefinition(
                "blocked",
                "blocked-agent",
                depends_on=("facts",),
                output_schema=EMPTY_SCHEMA,
                required=False,
            ),
            TaskDefinition(
                "synthesis",
                "lead",
                depends_on=("facts",),
                output_schema=EMPTY_SCHEMA,
                allow_failed_dependencies=True,
                max_turns=1,
            ),
        ),
        active_slots=1,
    )
    state, journal, model = execute(
        workflow,
        {
            "facts": ("not-json",),
            "synthesis": ('{"kind":"final","payload":{}}',),
        },
    )

    assert state.status is RunStatus.COMPLETE
    assert state.tasks["facts"].status is TaskStatus.FAILED
    assert state.tasks["blocked"].status is TaskStatus.SKIPPED
    assert state.tasks["synthesis"].status is TaskStatus.PARTIAL
    assert [request.task_id for request in model.requests] == ["facts", "synthesis"]
    started = events_of(journal, EventKind.TASK_STARTED)
    synthesis = next(event for event in started if event.payload["task_id"] == "synthesis")
    assert synthesis.payload["failed_dependencies"] == ["facts"]
    assert events_of(journal, EventKind.WORKFLOW_COMPLETED)[0].payload["partial"] is True


def test_jsonl_replay_is_pure_and_restores_task_projection(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    original, _, _ = execute(
        one_task(),
        {"metrics": (tool_action(), final("Replayable.", "ev:revenue"))},
        packets={"metrics": evidence_packet()},
        journal=JsonlJournal(path),
    )
    poison = PoisonActionModel()

    restored = replay(path)

    assert poison.calls == 0
    assert restored.status == original.status
    assert restored.workflow_id == original.workflow_id
    assert restored.workflow_version == original.workflow_version
    assert restored.tasks == original.tasks
    assert restored.last_sequence == original.last_sequence
