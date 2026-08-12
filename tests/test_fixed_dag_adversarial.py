"""Adversarial trust-boundary tests for fixed-DAG observation binding."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import UTC, date, datetime
from hashlib import sha256

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
from alpha_orchestration.domain import EventKind, JsonValue, RunSpec, RunStatus
from alpha_orchestration.fixed_dag import FixedDagRuntime
from alpha_orchestration.journal import MemoryJournal
from alpha_orchestration.ports import ActionModelRequest, ActionModelResult
from alpha_orchestration.tools.finance import (
    build_financial_tool_registry,
    financial_tool_definitions,
)
from alpha_orchestration.tools.registry import ToolRegistry

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
EVIDENCE_ID = "ev:trusted-packet"
CURRENT_ID = "obs:revenue:2024"
PRIOR_ID = "obs:revenue:2023"


class ScriptedModel:
    def __init__(self, outputs: Sequence[str]) -> None:
        self.outputs = tuple(outputs)
        self.requests: list[ActionModelRequest] = []

    async def complete(self, request: ActionModelRequest) -> ActionModelResult:
        self.requests.append(request)
        output = self.outputs[request.turn - 1]
        return ActionModelResult(
            request_id=request.request_id,
            output_text=output,
            prompt_ids=(request.turn,),
            output_ids=tuple(output.encode()),
            finish_reason="stop",
        )


def _evidence() -> EvidenceRecord:
    return EvidenceRecord(
        evidence_id=EVIDENCE_ID,
        provider=DataProvider.SEC,
        source_kind="adversarial_fixture",
        source_locator={"fixture": "binder"},
        source_url="https://example.test/binder",
        observed_at=datetime(2025, 1, 1, tzinfo=UTC),
        retrieved_at=datetime(2025, 1, 2, tzinfo=UTC),
        content_hash=sha256(b"binder-fixture").hexdigest(),
    )


def _observation(
    observation_id: str,
    value: int | float,
    *,
    name: str,
    end: date,
    entity_id: str = "ticker:ABC",
    ticker: str = "ABC",
    currency: str = "USD",
    instant: bool = True,
) -> FinancialObservation:
    period = (
        FinancialPeriod(PeriodKind.INSTANT, end=end)
        if instant
        else FinancialPeriod(
            PeriodKind.DURATION,
            start=date(end.year, 1, 1),
            end=end,
            fiscal_year=end.year,
            fiscal_period="FY",
        )
    )
    return FinancialObservation(
        observation_id=observation_id,
        entity_id=entity_id,
        ticker=ticker,
        name=name,
        value=value,
        unit=FinancialUnit(
            UnitKind.CURRENCY_PER_SHARE if instant else UnitKind.CURRENCY,
            currency,
            "units" if instant else "millions",
        ),
        period=period,
        evidence_ids=(EVIDENCE_ID,),
    )


def _accounting_packet() -> EvidencePacket:
    return EvidencePacket(
        observations=(
            _observation(
                PRIOR_ID,
                100,
                name="revenue",
                end=date(2023, 12, 31),
                instant=False,
            ),
            _observation(
                CURRENT_ID,
                125,
                name="revenue",
                end=date(2024, 12, 31),
                instant=False,
            ),
        ),
        evidence=(_evidence(),),
    )


def _market_packet(
    *,
    primary_entities: Sequence[str] = ("ticker:ABC", "ticker:ABC", "ticker:ABC"),
    primary_currencies: Sequence[str] = ("USD", "USD", "USD"),
    benchmark_dates: Sequence[date] = (
        date(2025, 1, 1),
        date(2025, 1, 2),
        date(2025, 1, 3),
    ),
) -> EvidencePacket:
    primary_dates = (date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 3))
    observations: list[FinancialObservation] = []
    for index, (value, end, entity_id, currency) in enumerate(
        zip(
            (100, 110, 121),
            primary_dates,
            primary_entities,
            primary_currencies,
            strict=True,
        )
    ):
        observations.append(
            _observation(
                f"obs:price:{index}",
                value,
                name="adjusted_close",
                end=end,
                entity_id=entity_id,
                currency=currency,
            )
        )
    for index, (value, end) in enumerate(
        zip((200, 204, 208), benchmark_dates, strict=True)
    ):
        observations.append(
            _observation(
                f"obs:benchmark:{index}",
                value,
                name="adjusted_close",
                end=end,
                entity_id="index:SPX",
                ticker="SPX",
            )
        )
    return EvidencePacket(observations=tuple(observations), evidence=(_evidence(),))


def _workflow(
    tool: str,
    *,
    max_turns: int = 2,
    max_tool_calls: int = 3,
    repair_budget: int = 0,
) -> WorkflowDefinition:
    return WorkflowDefinition(
        "adversarial-binder",
        "1",
        (
            TaskDefinition(
                "task",
                "tester",
                allowed_tools=(tool,),
                output_schema=FINAL_SCHEMA,
                max_turns=max_turns,
                max_tool_calls=max_tool_calls,
                max_calls_per_turn=3,
                repair_budget=repair_budget,
            ),
        ),
        active_slots=1,
    )


def _tool_calls(*calls: tuple[str, Mapping[str, JsonValue]]) -> str:
    return json.dumps(
        {
            "kind": "tool_calls",
            "calls": [
                {"name": name, "arguments": dict(arguments)}
                for name, arguments in calls
            ],
        },
        separators=(",", ":"),
    )


def _final(summary: str, *source_ids: str) -> str:
    return json.dumps(
        {
            "kind": "final",
            "payload": {"summary": summary, "source_ids": list(source_ids)},
        },
        separators=(",", ":"),
    )


def _calculate_arguments(
    *,
    current: JsonValue | None = None,
    extra: Mapping[str, JsonValue] | None = None,
) -> dict[str, JsonValue]:
    arguments: dict[str, JsonValue] = {
        "operations": [
            {
                "id": "growth",
                "operation": "percent_change",
                "current": (
                    {"observation_id": CURRENT_ID} if current is None else current
                ),
                "prior": {"observation_id": PRIOR_ID},
            }
        ]
    }
    arguments.update(extra or {})
    return arguments


def _market_arguments(*, include_benchmark: bool = True) -> dict[str, JsonValue]:
    inputs: dict[str, JsonValue] = {
        "prices": [
            {"observation_id": f"obs:price:{index}"} for index in range(3)
        ]
    }
    if include_benchmark:
        inputs["benchmark_prices"] = [
            {"observation_id": f"obs:benchmark:{index}"} for index in range(3)
        ]
    return {
        "observation_inputs": inputs,
        "periods_per_year": 252,
        "annual_risk_free_rate": 0,
    }


def _run(
    workflow: WorkflowDefinition,
    outputs: Sequence[str],
    *,
    packet: EvidencePacket | None = None,
    registry: ToolRegistry | None = None,
    source_ids: Sequence[str] | None = None,
    allow_unverified_sources: bool = False,
):
    async def execute():
        model = ScriptedModel(outputs)
        journal = MemoryJournal()
        runtime = FixedDagRuntime(
            workflow,
            model,
            registry or build_financial_tool_registry(),
            evidence_packets_by_task=None if packet is None else {"task": packet},
            source_ids_by_task=None if source_ids is None else {"task": source_ids},
            allow_unverified_sources=allow_unverified_sources,
        )
        state = await RunController(
            RunSpec(run_id="run-adversarial", mode="fixed_fixture"),
            runtime,
            journal,
        ).run()
        return state, journal, model

    return asyncio.run(execute())


def _events(journal: MemoryJournal, kind: EventKind):
    return [event for event in journal.events if event.kind is kind]


@pytest.mark.parametrize(
    ("current", "expected_code"),
    [
        (999, "unbound_numeric_input"),
        ({"observation_id": "obs:not-in-packet"}, "observation_not_allowed"),
    ],
)
def test_fabricated_or_unknown_observation_is_rejected_before_tool_start(
    current: JsonValue,
    expected_code: str,
) -> None:
    state, journal, _ = _run(
        _workflow("finance.calculate", max_turns=1),
        (_tool_calls(("finance.calculate", _calculate_arguments(current=current))),),
        packet=_accounting_packet(),
    )

    assert state.status is RunStatus.FAILED
    assert _events(journal, EventKind.TOOL_STARTED) == []
    assert _events(journal, EventKind.ACTION_REJECTED)[0].payload["code"] == expected_code


@pytest.mark.parametrize(
    "controller_field",
    [
        {"source_ids": [EVIDENCE_ID]},
        {"context": {"entity_id": "ticker:ABC"}},
    ],
)
def test_model_authored_controller_fields_are_rejected(
    controller_field: Mapping[str, JsonValue],
) -> None:
    state, journal, _ = _run(
        _workflow("finance.calculate", max_turns=1),
        (
            _tool_calls(
                (
                    "finance.calculate",
                    _calculate_arguments(extra=controller_field),
                )
            ),
        ),
        packet=_accounting_packet(),
    )

    assert state.status is RunStatus.FAILED
    assert _events(journal, EventKind.TOOL_STARTED) == []
    assert _events(journal, EventKind.ACTION_REJECTED)[0].payload["code"] == (
        "controller_field_not_allowed"
    )


def test_whole_batch_preflight_executes_nothing_until_every_call_is_valid() -> None:
    executions: list[Mapping[str, JsonValue]] = []
    calculate = next(
        definition
        for definition in financial_tool_definitions()
        if definition.name == "finance.calculate"
    )
    real_handler = calculate.handler

    def counted_handler(arguments: Mapping[str, JsonValue]) -> Mapping[str, JsonValue]:
        executions.append(arguments)
        return real_handler(arguments)

    registry = ToolRegistry((replace(calculate, handler=counted_handler),))
    invalid_second = _calculate_arguments(extra={"precision": True})
    first_turn = _tool_calls(
        ("finance.calculate", _calculate_arguments()),
        ("finance.calculate", invalid_second),
    )
    repaired_turn = _tool_calls(("finance.calculate", _calculate_arguments()))

    state, journal, _ = _run(
        _workflow(
            "finance.calculate",
            max_turns=3,
            max_tool_calls=3,
            repair_budget=1,
        ),
        (first_turn, repaired_turn, _final("repaired", EVIDENCE_ID)),
        packet=_accounting_packet(),
        registry=registry,
    )

    assert state.status is RunStatus.COMPLETE
    assert len(executions) == 1
    assert [event.payload["turn"] for event in _events(journal, EventKind.TOOL_STARTED)] == [2]
    rejection = _events(journal, EventKind.ACTION_REJECTED)[0].payload
    assert rejection["code"] == "invalid_schema"
    assert rejection["repair_allowed"] is True


def test_market_series_is_bound_from_observations_and_executes() -> None:
    state, journal, _ = _run(
        _workflow("finance.market_statistics", max_turns=2),
        (
            _tool_calls(("finance.market_statistics", _market_arguments())),
            _final("market statistics", EVIDENCE_ID),
        ),
        packet=_market_packet(),
    )

    assert state.status is RunStatus.COMPLETE
    started = _events(journal, EventKind.TOOL_STARTED)[0].payload
    assert started["proposed_arguments"]["observation_inputs"]["prices"][0] == {
        "observation_id": "obs:price:0"
    }
    assert started["arguments"]["prices"] == [100, 110, 121]
    assert started["arguments"]["benchmark_returns"] == pytest.approx(
        [0.02, 0.019607843137254832]
    )
    assert started["arguments"]["source_ids"] == [EVIDENCE_ID]
    assert started["observation_bindings"]["observation_inputs.prices[2]"] == "obs:price:2"


@pytest.mark.parametrize(
    ("packet", "include_benchmark"),
    [
        (
            _market_packet(
                primary_entities=("ticker:ABC", "ticker:XYZ", "ticker:ABC")
            ),
            False,
        ),
        (
            _market_packet(primary_currencies=("USD", "EUR", "USD")),
            False,
        ),
        (
            _market_packet(
                benchmark_dates=(
                    date(2025, 1, 1),
                    date(2025, 1, 2),
                    date(2025, 1, 4),
                )
            ),
            True,
        ),
    ],
)
def test_market_series_rejects_cross_entity_currency_or_period_inputs(
    packet: EvidencePacket,
    include_benchmark: bool,
) -> None:
    state, journal, _ = _run(
        _workflow("finance.market_statistics", max_turns=1),
        (
            _tool_calls(
                (
                    "finance.market_statistics",
                    _market_arguments(include_benchmark=include_benchmark),
                )
            ),
        ),
        packet=packet,
    )

    assert state.status is RunStatus.FAILED
    assert _events(journal, EventKind.TOOL_STARTED) == []
    assert _events(journal, EventKind.ACTION_REJECTED)[0].payload["code"] == (
        "incompatible_observations"
    )


def test_rank_fails_closed_until_trusted_task_output_binding_exists() -> None:
    rank_arguments: dict[str, JsonValue] = {
        "rows": [{"id": "ABC", "metrics": {"growth": 0.25}}],
        "criteria": [{"metric": "growth", "direction": "higher", "weight": 1}],
        "missing_policy": "exclude",
    }
    state, journal, _ = _run(
        _workflow("finance.rank", max_turns=1),
        (_tool_calls(("finance.rank", rank_arguments)),),
        packet=_accounting_packet(),
    )

    assert state.status is RunStatus.FAILED
    assert _events(journal, EventKind.TOOL_STARTED) == []
    assert _events(journal, EventKind.ACTION_REJECTED)[0].payload["code"] == (
        "binding_not_supported"
    )


def test_manual_source_ids_are_denied_by_default() -> None:
    workflow = _workflow("finance.calculate", max_turns=1)
    model = ScriptedModel((_final("legacy", "ev:legacy"),))

    with pytest.raises(ValueError, match="allow_unverified_sources=True"):
        FixedDagRuntime(
            workflow,
            model,
            build_financial_tool_registry(),
            source_ids_by_task={"task": ("ev:legacy",)},
        )


def test_explicit_legacy_flag_is_visible_in_journal() -> None:
    state, journal, _ = _run(
        _workflow("finance.calculate", max_turns=1),
        (_final("legacy", "ev:legacy"),),
        source_ids=("ev:legacy",),
        allow_unverified_sources=True,
    )

    assert state.status is RunStatus.COMPLETE
    started = _events(journal, EventKind.TASK_STARTED)[0].payload
    assert started["evidence_mode"] == "manual_unverified_source_ids"
    assert started["allowed_source_ids"] == ["ev:legacy"]


def test_journal_preserves_proposed_and_controller_resolved_arguments() -> None:
    state, journal, model = _run(
        _workflow("finance.calculate", max_turns=2),
        (
            _tool_calls(("finance.calculate", _calculate_arguments())),
            _final("bound", EVIDENCE_ID),
        ),
        packet=_accounting_packet(),
    )

    assert state.status is RunStatus.COMPLETE
    started = _events(journal, EventKind.TOOL_STARTED)[0].payload
    proposed_operation = started["proposed_arguments"]["operations"][0]
    resolved_operation = started["arguments"]["operations"][0]
    assert proposed_operation["current"] == {"observation_id": CURRENT_ID}
    assert proposed_operation["prior"] == {"observation_id": PRIOR_ID}
    assert resolved_operation["current"] == 125
    assert resolved_operation["prior"] == 100
    assert started["source_ids"] == [EVIDENCE_ID]
    assert started["arguments"]["source_ids"] == [EVIDENCE_ID]
    assert started["observation_bindings"] == {
        "operations[0].current": CURRENT_ID,
        "operations[0].prior": PRIOR_ID,
    }
    assert started["proposed_arguments_hash"] != started["arguments_hash"]
    completed = _events(journal, EventKind.TOOL_COMPLETED)[0].payload
    assert completed["source_ids"] == [EVIDENCE_ID]
    assert model.requests[1].transcript[-1]["source_ids"] == [EVIDENCE_ID]
