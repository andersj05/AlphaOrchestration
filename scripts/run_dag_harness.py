"""Run and replay a deterministic offline fixed-DAG fixture."""

from __future__ import annotations

import argparse
import asyncio
import json
import tempfile
from collections.abc import Sequence
from datetime import UTC, date, datetime
from pathlib import Path

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
from alpha_orchestration.data.observations import (
    canonical_content_hash,
    evidence_id_for,
    observation_id_for,
)
from alpha_orchestration.domain import JsonValue, RunSpec
from alpha_orchestration.fixed_dag import FixedDagRuntime
from alpha_orchestration.journal import JsonlJournal, replay
from alpha_orchestration.ports import ActionModelRequest, ActionModelResult
from alpha_orchestration.tools.finance import build_financial_tool_registry

OUTPUT_SCHEMA: dict[str, JsonValue] = {
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


class HarnessModel:
    def __init__(
        self,
        *,
        current_observation_id: str,
        prior_observation_id: str,
        source_ids: Sequence[str],
    ) -> None:
        self.calls = 0
        self.outputs = (
            json.dumps(
                {
                    "kind": "tool_calls",
                    "calls": [
                        {
                            "name": "finance.calculate",
                            "arguments": {
                                "operations": [
                                    {
                                        "id": "growth",
                                        "operation": "percent_change",
                                        "current": {
                                            "observation_id": current_observation_id
                                        },
                                        "prior": {
                                            "observation_id": prior_observation_id
                                        },
                                    }
                                ],
                            },
                        }
                    ],
                },
                separators=(",", ":"),
            ),
            json.dumps(
                {
                    "kind": "final",
                    "payload": {
                        "summary": "Revenue increased 25% in the fixture.",
                        "source_ids": list(source_ids),
                    },
                },
                separators=(",", ":"),
            ),
        )

    async def complete(self, request: ActionModelRequest) -> ActionModelResult:
        output = self.outputs[request.turn - 1]
        self.calls += 1
        return ActionModelResult(
            request_id=request.request_id,
            output_text=output,
            prompt_ids=(request.turn, 101),
            output_ids=tuple(output.encode("utf-8")),
            finish_reason="stop",
            telemetry={"fixture": "offline"},
            model_fingerprint="dag-harness-v1",
            tokenizer_fingerprint="bytes-v1",
        )


def build_evidence_packet() -> tuple[EvidencePacket, str, str]:
    pairs = (
        _revenue_observation(
            value=100,
            start=date(2023, 1, 1),
            end=date(2023, 12, 31),
            suffix="prior",
        ),
        _revenue_observation(
            value=125,
            start=date(2024, 1, 1),
            end=date(2024, 12, 31),
            suffix="current",
        ),
    )
    packet = EvidencePacket(
        observations=tuple(pair[0] for pair in pairs),
        evidence=tuple(pair[1] for pair in pairs),
    )
    return packet, pairs[1][0].observation_id, pairs[0][0].observation_id


def _revenue_observation(
    *, value: int, start: date, end: date, suffix: str
) -> tuple[FinancialObservation, EvidenceRecord]:
    period = FinancialPeriod(
        PeriodKind.DURATION,
        start=start,
        end=end,
        fiscal_year=end.year,
        fiscal_period="FY",
    )
    locator: dict[str, JsonValue] = {
        "ticker": "ABC",
        "field": "revenue",
        "period_end": end.isoformat(),
        "suffix": suffix,
        "value": value,
    }
    evidence_id = evidence_id_for(DataProvider.SEC, "fixture_fact", locator)
    evidence = EvidenceRecord(
        evidence_id=evidence_id,
        provider=DataProvider.SEC,
        source_kind="fixture_fact",
        source_locator=locator,
        source_url=f"https://example.test/{suffix}",
        observed_at=datetime.combine(end, datetime.min.time(), tzinfo=UTC),
        retrieved_at=datetime(2025, 3, 3, 20, tzinfo=UTC),
        content_hash=canonical_content_hash(locator),
    )
    observation = FinancialObservation(
        observation_id=observation_id_for(
            DataProvider.SEC,
            entity_id="ticker:ABC",
            name="revenue",
            period=period,
            evidence_id=evidence_id,
        ),
        entity_id="ticker:ABC",
        ticker="ABC",
        name="revenue",
        value=value,
        unit=FinancialUnit(UnitKind.CURRENCY, "USD"),
        period=period,
        evidence_ids=(evidence_id,),
        metadata={"fixture": suffix},
    )
    return observation, evidence


def build_workflow() -> WorkflowDefinition:
    return WorkflowDefinition(
        "offline-harness",
        "1.0.0",
        (
            TaskDefinition(
                "calculate-growth",
                "fundamentals",
                allowed_tools=("finance.calculate",),
                output_schema=OUTPUT_SCHEMA,
                max_turns=2,
                max_tool_calls=2,
                max_calls_per_turn=2,
            ),
        ),
        active_slots=1,
    )


async def run_harness(path: Path) -> dict[str, JsonValue]:
    packet, current_observation_id, prior_observation_id = build_evidence_packet()
    model = HarnessModel(
        current_observation_id=current_observation_id,
        prior_observation_id=prior_observation_id,
        source_ids=packet.source_ids,
    )
    workflow = build_workflow()
    controller = RunController(
        RunSpec(
            sector="Offline Fixture",
            run_id="run-dag-harness",
            mode="offline_harness",
        ),
        FixedDagRuntime(
            workflow,
            model,
            build_financial_tool_registry(),
            evidence_packets_by_task={"calculate-growth": packet},
        ),
        JsonlJournal(path),
    )
    original = await controller.run()
    restored = replay(path)
    if (
        restored.status != original.status
        or restored.tasks != original.tasks
        or restored.workflow_id != original.workflow_id
        or restored.workflow_version != original.workflow_version
        or restored.last_sequence != original.last_sequence
    ):
        raise RuntimeError("replayed state does not match the executed state")
    task = restored.tasks["calculate-growth"]
    return {
        "ok": True,
        "run_id": restored.spec.run_id,
        "status": restored.status.value,
        "events": restored.last_sequence + 1,
        "model_turns": model.calls,
        "task_status": task.status.value,
        "tool_calls": task.tool_calls,
        "journal": str(path.resolve()),
        "replay_equivalent": True,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Execute and replay Alpha's deterministic offline DAG harness."
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="New JSONL path to retain; it must not already exist.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.output is not None:
        summary = asyncio.run(run_harness(args.output))
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0
    with tempfile.TemporaryDirectory(prefix="alpha-dag-harness-") as directory:
        summary = asyncio.run(run_harness(Path(directory) / "events.jsonl"))
        summary["journal"] = "temporary (removed after verification)"
        print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
