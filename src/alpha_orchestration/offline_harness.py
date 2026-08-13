"""Deterministic multi-issuer execution/replay fixture for the offline gate."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

from alpha_orchestration.calculations.ranking import rank_entities
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
from alpha_orchestration.domain import (
    Candidate,
    CandidateBucket,
    CandidateConfidence,
    CandidateDataQuality,
    CandidateFinancial,
    CandidateSourceMode,
    EventKind,
    JsonValue,
    RunSpec,
    RunStatus,
    TaskStatus,
)
from alpha_orchestration.fixed_dag import FixedDagRuntime
from alpha_orchestration.journal import JsonlJournal, load_events, replay
from alpha_orchestration.ports import (
    ActionModelRequest,
    ActionModelResult,
    EventDraft,
    OrchestratorRuntime,
)
from alpha_orchestration.tools.finance import build_financial_tool_registry

BRANCH_TASK_IDS = ("analyze-alp", "analyze-bet", "analyze-gam")
VALIDATOR_TASK_ID = "validate-ranked-results"
ACTIVE_SLOTS = 3

BRANCH_OUTPUT_SCHEMA: dict[str, JsonValue] = {
    "type": "object",
    "required": ["entity_id", "ticker", "company", "summary", "source_ids"],
    "properties": {
        "entity_id": {"type": "string", "minLength": 1},
        "ticker": {"type": "string", "minLength": 1},
        "company": {"type": "string", "minLength": 1},
        "summary": {"type": "string", "minLength": 1},
        "source_ids": {
            "type": "array",
            "items": {"type": "string", "minLength": 1},
            "minItems": 1,
            "uniqueItems": True,
        },
    },
    "additionalProperties": False,
}

BRIEF_SCHEMA: dict[str, JsonValue] = {
    "type": "object",
    "required": [
        "ticker",
        "variant_wedge",
        "why_now",
        "first_rejection",
        "investable_if",
        "kill_if",
        "next_workflow",
        "source_ids",
    ],
    "properties": {
        "ticker": {"type": "string", "enum": ["ALP", "BET", "GAM"]},
        "variant_wedge": {"type": "string", "minLength": 1},
        "why_now": {"type": "string", "minLength": 1},
        "first_rejection": {"type": "string", "minLength": 1},
        "investable_if": {"type": "string", "minLength": 1},
        "kill_if": {"type": "string", "minLength": 1},
        "next_workflow": {"type": "string", "minLength": 1},
        "source_ids": {
            "type": "array",
            "items": {"type": "string", "minLength": 1},
            "minItems": 1,
            "uniqueItems": True,
        },
    },
    "additionalProperties": False,
}

VALIDATOR_OUTPUT_SCHEMA: dict[str, JsonValue] = {
    "type": "object",
    "required": ["proposed_ranked_ids", "briefs"],
    "properties": {
        "proposed_ranked_ids": {
            "type": "array",
            "items": {"type": "string", "minLength": 1},
            "minItems": 3,
            "maxItems": 3,
            "uniqueItems": True,
        },
        "briefs": {
            "type": "array",
            "items": BRIEF_SCHEMA,
            "minItems": 3,
            "maxItems": 3,
        },
    },
    "additionalProperties": False,
}


@dataclass(frozen=True, slots=True)
class IssuerFixture:
    task_id: str
    agent_id: str
    entity_id: str
    ticker: str
    company: str
    prior_revenue: float
    current_revenue: float
    net_income: float
    variant_wedge: str
    why_now: str
    first_rejection: str
    investable_if: str
    kill_if: str
    next_workflow: str


ISSUERS = (
    IssuerFixture(
        task_id="analyze-alp",
        agent_id="issuer-alp",
        entity_id="ticker:ALP",
        ticker="ALP",
        company="Alpha Logic",
        prior_revenue=100.0,
        current_revenue=140.0,
        net_income=21.0,
        variant_wedge="The fixture combines the strongest revenue acceleration with a solid current margin.",
        why_now="The normalized FY2024 evidence shows a clear step-up from the FY2023 base.",
        first_rejection="The growth step may reflect a concentrated program rather than durable breadth.",
        investable_if="Customer concentration and backlog durability survive deeper filing review.",
        kill_if="Subsequent evidence shows the program was pulled forward or non-recurring.",
        next_workflow="customer_concentration_review",
    ),
    IssuerFixture(
        task_id="analyze-bet",
        agent_id="issuer-bet",
        entity_id="ticker:BET",
        ticker="BET",
        company="Beta Systems",
        prior_revenue=100.0,
        current_revenue=120.0,
        net_income=24.0,
        variant_wedge="The fixture pairs moderate growth with the universe's best current net margin.",
        why_now="Profit conversion is stronger than the two comparison issuers in the same period.",
        first_rejection="A high current margin may already be embedded in expectations.",
        investable_if="The margin advantage persists without under-investing in future growth.",
        kill_if="Normalized margins retreat while revenue growth remains mid-pack.",
        next_workflow="margin_durability_review",
    ),
    IssuerFixture(
        task_id="analyze-gam",
        agent_id="issuer-gam",
        entity_id="ticker:GAM",
        ticker="GAM",
        company="Gamma Devices",
        prior_revenue=100.0,
        current_revenue=110.0,
        net_income=11.0,
        variant_wedge="The fixture provides positive growth but trails both peers on the selected measures.",
        why_now="The name remains observable as a control case for the bounded ranking policy.",
        first_rejection="Neither growth nor margin currently supplies a differentiated research signal.",
        investable_if="New evidence identifies an inflection not present in the normalized FY2024 facts.",
        kill_if="Growth and margin remain below the peer fixture on the next comparable period.",
        next_workflow="inflection_evidence_review",
    ),
)


@dataclass(frozen=True, slots=True)
class IssuerEvidence:
    issuer: IssuerFixture
    packet: EvidencePacket
    observation_ids: Mapping[str, str]
    evidence_ids: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class BranchMetricResult:
    issuer: IssuerFixture
    revenue_growth: float
    net_margin: float
    source_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if not all(math.isfinite(value) for value in (self.revenue_growth, self.net_margin)):
            raise ValueError("branch metrics must be finite")
        if not self.source_ids or len(self.source_ids) != len(set(self.source_ids)):
            raise ValueError("branch metrics require unique source IDs")


@dataclass(frozen=True, slots=True)
class RankedCandidateResult:
    rank: int
    composite_score: float
    revenue_growth: float
    net_margin: float
    candidate: Candidate

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "rank": self.rank,
            "candidate_id": self.candidate.candidate_id,
            "ticker": self.candidate.ticker,
            "company": self.candidate.company,
            "bucket": self.candidate.bucket.value,
            "priority_score": self.candidate.priority_score,
            "composite_score": self.composite_score,
            "metrics": {
                "revenue_growth": self.revenue_growth,
                "net_margin": self.net_margin,
            },
            "data_quality": self.candidate.data_quality.value,
            "source_mode": self.candidate.source_mode.value,
            "source_ids": list(self.candidate.evidence_ids),
        }


@dataclass(frozen=True, slots=True)
class RankedResultsArtifact:
    candidates: tuple[RankedCandidateResult, ...]
    source_ids: tuple[str, ...]
    validator_proposed_ranked_ids: tuple[str, ...]
    validator_rank_override_blocked: bool
    formula_version: str = "entity-ranking-v1"

    def __post_init__(self) -> None:
        if tuple(item.rank for item in self.candidates) != tuple(range(1, len(self.candidates) + 1)):
            raise ValueError("candidate ranks must be contiguous")
        ranked_ids = self.ranked_ids
        if len(ranked_ids) != len(set(ranked_ids)):
            raise ValueError("ranked candidate IDs must be unique")
        cited = {
            source_id
            for result in self.candidates
            for source_id in result.candidate.evidence_ids
        }
        if cited != set(self.source_ids):
            raise ValueError("artifact source coverage must exactly match candidate citations")
        if not self.validator_rank_override_blocked:
            raise ValueError("fixture must exercise the controller-owned ranking boundary")

    @property
    def ranked_ids(self) -> tuple[str, ...]:
        return tuple(item.candidate.candidate_id for item in self.candidates)

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "formula_version": self.formula_version,
            "ranked_ids": list(self.ranked_ids),
            "candidates": [candidate.to_dict() for candidate in self.candidates],
            "source_ids": list(self.source_ids),
            "data_quality_posture": {
                "complete": sum(
                    item.candidate.data_quality is CandidateDataQuality.COMPLETE
                    for item in self.candidates
                ),
                "total": len(self.candidates),
            },
            "validator_proposed_ranked_ids": list(self.validator_proposed_ranked_ids),
            "validator_rank_override_blocked": self.validator_rank_override_blocked,
        }


class MultiIssuerHarnessModel:
    """Offline action fixture with a deterministic first-turn concurrency barrier."""

    def __init__(self, evidence_by_task: Mapping[str, IssuerEvidence]) -> None:
        self._evidence_by_task = dict(evidence_by_task)
        self._release = asyncio.Event()
        self._ordered_release = [asyncio.Event() for _ in BRANCH_TASK_IDS]
        self._ordered_release[0].set()
        self._arrivals = 0
        self._active = 0
        self.peak_active_calls = 0
        self.calls = 0
        self.validator_dependencies: tuple[str, ...] = ()

    async def complete(self, request: ActionModelRequest) -> ActionModelResult:
        self.calls += 1
        if request.task_id in self._evidence_by_task:
            if request.turn == 1:
                output = await self._branch_tool_action(request.task_id)
            elif request.turn == 2:
                output = self._branch_final(request.task_id)
            else:  # pragma: no cover - bounded workflow policy
                raise RuntimeError(f"unexpected branch turn: {request.turn}")
        elif request.task_id == VALIDATOR_TASK_ID and request.turn == 1:
            self.validator_dependencies = _dependency_ids(request)
            if self.validator_dependencies != tuple(sorted(BRANCH_TASK_IDS)):
                raise RuntimeError("validator did not receive every completed issuer dependency")
            output = self._validator_final()
        else:  # pragma: no cover - fixed workflow owns all task IDs
            raise RuntimeError(f"unexpected harness request: {request.task_id} turn {request.turn}")
        return ActionModelResult(
            request_id=request.request_id,
            output_text=output,
            prompt_ids=(request.turn, len(request.task_id), 202),
            output_ids=tuple(output.encode("utf-8")),
            finish_reason="stop",
            telemetry={"fixture": "offline-multi-issuer", "task_id": request.task_id},
            model_fingerprint="multi-issuer-harness-v1",
            tokenizer_fingerprint="bytes-v1",
        )

    async def _branch_tool_action(self, task_id: str) -> str:
        position = BRANCH_TASK_IDS.index(task_id)
        self._active += 1
        self._arrivals += 1
        self.peak_active_calls = max(self.peak_active_calls, self._active)
        if self._arrivals == len(BRANCH_TASK_IDS):
            self._release.set()
        try:
            await asyncio.wait_for(self._release.wait(), timeout=3)
            await asyncio.wait_for(self._ordered_release[position].wait(), timeout=3)
            evidence = self._evidence_by_task[task_id]
            return _json_action(
                {
                    "kind": "tool_calls",
                    "calls": [
                        {
                            "name": "finance.metrics",
                            "arguments": {
                                "observation_inputs": {
                                    "revenue": {
                                        "observation_id": evidence.observation_ids["current_revenue"]
                                    },
                                    "prior_revenue": {
                                        "observation_id": evidence.observation_ids["prior_revenue"]
                                    },
                                    "net_income": {
                                        "observation_id": evidence.observation_ids["net_income"]
                                    },
                                },
                                "metrics": ["revenue_growth", "net_margin"],
                                "precision": 4,
                            },
                        }
                    ],
                }
            )
        finally:
            self._active -= 1
            if position + 1 < len(self._ordered_release):
                self._ordered_release[position + 1].set()

    def _branch_final(self, task_id: str) -> str:
        evidence = self._evidence_by_task[task_id]
        issuer = evidence.issuer
        return _json_action(
            {
                "kind": "final",
                "payload": {
                    "entity_id": issuer.entity_id,
                    "ticker": issuer.ticker,
                    "company": issuer.company,
                    "summary": f"{issuer.ticker} normalized metrics are ready for controller ranking.",
                    "source_ids": list(evidence.packet.source_ids),
                },
            }
        )

    def _validator_final(self) -> str:
        # The deliberately forged ID proves the validator cannot control identities or rank.
        proposed_ids = ["ticker:FORGED", ISSUERS[2].entity_id, ISSUERS[1].entity_id]
        briefs = []
        for issuer in ISSUERS:
            evidence = self._evidence_by_task[issuer.task_id]
            briefs.append(
                {
                    "ticker": issuer.ticker,
                    "variant_wedge": issuer.variant_wedge,
                    "why_now": issuer.why_now,
                    "first_rejection": issuer.first_rejection,
                    "investable_if": issuer.investable_if,
                    "kill_if": issuer.kill_if,
                    "next_workflow": issuer.next_workflow,
                    "source_ids": list(evidence.packet.source_ids),
                }
            )
        return _json_action(
            {
                "kind": "final",
                "payload": {
                    "proposed_ranked_ids": proposed_ids,
                    "briefs": briefs,
                },
            }
        )


class RankedResultsProjector(OrchestratorRuntime):
    """Project validated fixed-DAG tool results before workflow completion.

    This boundary relies on ``FixedDagRuntime`` emitting controller-bound
    ``TOOL_COMPLETED`` payloads; it is not a generic runtime result projector.
    """

    def __init__(
        self,
        runtime: FixedDagRuntime,
        evidence_by_task: Mapping[str, IssuerEvidence],
    ) -> None:
        self._runtime = runtime
        self._evidence_by_task = dict(evidence_by_task)
        self._branch_results: dict[str, BranchMetricResult] = {}
        self._validator_output: dict[str, JsonValue] | None = None
        self.artifact: RankedResultsArtifact | None = None

    async def stream(self, spec: RunSpec) -> AsyncIterator[EventDraft]:
        evidence_emitted = False
        async for draft in self._runtime.stream(spec):
            if draft.kind is EventKind.TOOL_COMPLETED and draft.payload.get("task_id") in BRANCH_TASK_IDS:
                self._capture_branch_result(draft)
            elif draft.kind is EventKind.TASK_COMPLETED and draft.payload.get("task_id") == VALIDATOR_TASK_ID:
                output = draft.payload.get("output")
                if not isinstance(output, dict):
                    raise RuntimeError("validator output must be an object")
                self._validator_output = output

            if draft.kind is EventKind.WORKFLOW_PLANNED and not evidence_emitted:
                yield draft
                for evidence in self._evidence_by_task.values():
                    for record in evidence.packet.evidence:
                        yield _evidence_event(evidence.issuer, record)
                evidence_emitted = True
                continue

            if draft.kind is EventKind.WORKFLOW_COMPLETED:
                self.artifact = self._build_artifact()
                for result in self.artifact.candidates:
                    yield _candidate_event(result, self.artifact.formula_version)
                yield draft
                continue
            yield draft

    def _capture_branch_result(self, draft: EventDraft) -> None:
        task_id = str(draft.payload["task_id"])
        if task_id in self._branch_results:
            raise RuntimeError(f"duplicate metrics result for {task_id}")
        evidence = self._evidence_by_task[task_id]
        if draft.payload.get("tool") != "finance.metrics":
            raise RuntimeError(f"unexpected branch tool for {task_id}")
        result = draft.payload.get("result")
        if not isinstance(result, dict) or result.get("ok") is not True:
            raise RuntimeError(f"branch metrics did not complete for {task_id}")
        data = result.get("data")
        if not isinstance(data, dict) or data.get("formula_version") != "finance-metrics-v1":
            raise RuntimeError(f"branch metrics formula is untrusted for {task_id}")
        context = data.get("context")
        if not isinstance(context, dict) or context.get("entity_id") != evidence.issuer.entity_id:
            raise RuntimeError(f"branch metrics entity mismatch for {task_id}")
        values = data.get("values")
        if not isinstance(values, dict):
            raise RuntimeError(f"branch metrics values are missing for {task_id}")
        growth = _finite_number(values.get("revenue_growth"), "revenue_growth")
        margin = _finite_number(values.get("net_margin"), "net_margin")
        raw_sources = draft.payload.get("source_ids")
        if not isinstance(raw_sources, list) or any(not isinstance(item, str) for item in raw_sources):
            raise RuntimeError(f"branch metrics sources are invalid for {task_id}")
        source_ids = tuple(raw_sources)
        if set(source_ids) != set(evidence.packet.source_ids):
            raise RuntimeError(f"branch metrics source coverage mismatch for {task_id}")
        self._branch_results[task_id] = BranchMetricResult(
            issuer=evidence.issuer,
            revenue_growth=growth,
            net_margin=margin,
            source_ids=source_ids,
        )

    def _build_artifact(self) -> RankedResultsArtifact:
        if set(self._branch_results) != set(BRANCH_TASK_IDS):
            raise RuntimeError("controller ranking requires all three trusted branch results")
        if self._validator_output is None:
            raise RuntimeError("candidate projection requires the validator output")
        proposed = self._validator_output.get("proposed_ranked_ids")
        briefs = self._validator_output.get("briefs")
        if not isinstance(proposed, list) or any(not isinstance(item, str) for item in proposed):
            raise RuntimeError("validator proposed_ranked_ids must be strings")
        if not isinstance(briefs, list) or any(not isinstance(item, dict) for item in briefs):
            raise RuntimeError("validator briefs must be objects")
        briefs_by_ticker = {str(brief["ticker"]): brief for brief in briefs}
        expected_tickers = {issuer.ticker for issuer in ISSUERS}
        if set(briefs_by_ticker) != expected_tickers or len(briefs_by_ticker) != len(briefs):
            raise RuntimeError("validator briefs must cover each controller-owned issuer exactly once")

        validated_source_ids: dict[str, tuple[str, ...]] = {}
        for issuer in ISSUERS:
            evidence = self._evidence_by_task[issuer.task_id]
            raw_source_ids = briefs_by_ticker[issuer.ticker].get("source_ids")
            if not isinstance(raw_source_ids, list) or any(
                not isinstance(source_id, str) for source_id in raw_source_ids
            ):
                raise RuntimeError(
                    f"validator brief source_ids must be a string list for {issuer.ticker}"
                )
            source_ids = tuple(raw_source_ids)
            if len(source_ids) != len(set(source_ids)):
                raise RuntimeError(
                    f"validator brief source_ids must be unique for {issuer.ticker}"
                )
            if source_ids != evidence.packet.source_ids:
                raise RuntimeError(
                    "validator brief source_ids must exactly match the issuer evidence "
                    f"packet for {issuer.ticker}"
                )
            validated_source_ids[issuer.ticker] = source_ids

        ranking = rank_entities(
            {
                "rows": [
                    {
                        "id": result.issuer.entity_id,
                        "metrics": {
                            "revenue_growth": result.revenue_growth,
                            "net_margin": result.net_margin,
                        },
                    }
                    for result in self._branch_results.values()
                ],
                "criteria": [
                    {"metric": "revenue_growth", "direction": "higher", "weight": 0.6},
                    {"metric": "net_margin", "direction": "higher", "weight": 0.4},
                ],
                "missing_policy": "exclude",
                "top_n": len(ISSUERS),
                "precision": 4,
                "context": {"fixture": "offline-multi-issuer-v1", "as_of": "2024-12-31"},
            }
        )
        raw_ranked = ranking.get("ranked")
        if not isinstance(raw_ranked, list) or len(raw_ranked) != len(ISSUERS):
            raise RuntimeError("controller ranking did not return the full fixture universe")
        metrics_by_entity = {
            result.issuer.entity_id: result for result in self._branch_results.values()
        }
        issuer_by_entity = {issuer.entity_id: issuer for issuer in ISSUERS}
        buckets = (
            CandidateBucket.ADVANCE,
            CandidateBucket.VALUATION_GATED,
            CandidateBucket.DEPRIORITIZED,
        )
        ranked_candidates: list[RankedCandidateResult] = []
        for index, row in enumerate(raw_ranked):
            if not isinstance(row, dict):
                raise RuntimeError("controller ranking row must be an object")
            entity_id = str(row["id"])
            metrics = metrics_by_entity[entity_id]
            issuer = issuer_by_entity[entity_id]
            evidence = self._evidence_by_task[issuer.task_id]
            brief = briefs_by_ticker[issuer.ticker]
            composite = _finite_number(row.get("composite_score"), "composite_score")
            financials = (
                CandidateFinancial(
                    metric="revenue_growth",
                    label="Revenue growth",
                    value=metrics.revenue_growth,
                    unit="ratio",
                    period="FY2024 vs FY2023",
                    source_ids=(
                        evidence.evidence_ids["current_revenue"],
                        evidence.evidence_ids["prior_revenue"],
                    ),
                ),
                CandidateFinancial(
                    metric="net_margin",
                    label="Net margin",
                    value=metrics.net_margin,
                    unit="ratio",
                    period="FY2024",
                    source_ids=(
                        evidence.evidence_ids["net_income"],
                        evidence.evidence_ids["current_revenue"],
                    ),
                ),
            )
            candidate = Candidate(
                candidate_id=issuer.entity_id,
                ticker=issuer.ticker,
                company=issuer.company,
                bucket=buckets[index],
                priority_score=int(round(composite)),
                variant_wedge=str(brief["variant_wedge"]),
                why_now=str(brief["why_now"]),
                first_rejection=str(brief["first_rejection"]),
                investable_if=str(brief["investable_if"]),
                kill_if=str(brief["kill_if"]),
                next_workflow=str(brief["next_workflow"]),
                evidence_ids=validated_source_ids[issuer.ticker],
                financials=financials,
                confidence=CandidateConfidence.MEDIUM,
                data_quality=CandidateDataQuality.COMPLETE,
                as_of="2024-12-31",
                source_mode=CandidateSourceMode.SYNTHETIC,
                evidence_gaps=(),
            )
            ranked_candidates.append(
                RankedCandidateResult(
                    rank=index + 1,
                    composite_score=composite,
                    revenue_growth=metrics.revenue_growth,
                    net_margin=metrics.net_margin,
                    candidate=candidate,
                )
            )
        trusted_ids = tuple(result.candidate.candidate_id for result in ranked_candidates)
        proposed_ids = tuple(proposed)
        override_blocked = proposed_ids != trusted_ids and not set(proposed_ids).issubset(trusted_ids)
        source_ids = tuple(
            dict.fromkeys(
                source_id
                for result in ranked_candidates
                for source_id in result.candidate.evidence_ids
            )
        )
        return RankedResultsArtifact(
            candidates=tuple(ranked_candidates),
            source_ids=source_ids,
            validator_proposed_ranked_ids=proposed_ids,
            validator_rank_override_blocked=override_blocked,
        )


def build_issuer_evidence() -> dict[str, IssuerEvidence]:
    result: dict[str, IssuerEvidence] = {}
    for issuer in ISSUERS:
        pairs = {
            "current_revenue": _observation(issuer, "revenue", issuer.current_revenue, 2024),
            "prior_revenue": _observation(issuer, "revenue", issuer.prior_revenue, 2023),
            "net_income": _observation(issuer, "net_income", issuer.net_income, 2024),
        }
        packet = EvidencePacket(
            observations=tuple(pair[0] for pair in pairs.values()),
            evidence=tuple(pair[1] for pair in pairs.values()),
        )
        result[issuer.task_id] = IssuerEvidence(
            issuer=issuer,
            packet=packet,
            observation_ids={name: pair[0].observation_id for name, pair in pairs.items()},
            evidence_ids={name: pair[1].evidence_id for name, pair in pairs.items()},
        )
    return result


def build_workflow() -> WorkflowDefinition:
    branch_tasks = tuple(
        TaskDefinition(
            issuer.task_id,
            issuer.agent_id,
            allowed_tools=("finance.metrics",),
            prompt_key="offline_issuer_metrics",
            output_schema=BRANCH_OUTPUT_SCHEMA,
            max_turns=2,
            max_tool_calls=1,
            max_calls_per_turn=1,
        )
        for issuer in ISSUERS
    )
    validator = TaskDefinition(
        VALIDATOR_TASK_ID,
        "validator",
        depends_on=BRANCH_TASK_IDS,
        prompt_key="offline_ranked_results_validator",
        output_schema=VALIDATOR_OUTPUT_SCHEMA,
        max_turns=1,
        max_tool_calls=0,
        max_calls_per_turn=1,
        repair_budget=0,
    )
    return WorkflowDefinition(
        "offline-multi-issuer-harness",
        "2.0.0",
        (*branch_tasks, validator),
        active_slots=ACTIVE_SLOTS,
    )


async def run_harness(path: Path) -> dict[str, JsonValue]:
    evidence_by_task = build_issuer_evidence()
    model = MultiIssuerHarnessModel(evidence_by_task)
    workflow = build_workflow()
    projector = RankedResultsProjector(
        FixedDagRuntime(
            workflow,
            model,
            build_financial_tool_registry(),
            evidence_packets_by_task={
                task_id: evidence.packet for task_id, evidence in evidence_by_task.items()
            },
        ),
        evidence_by_task,
    )
    spec = RunSpec(
        sector="Offline Multi-Issuer Fixture",
        universe_size=len(ISSUERS),
        agent_budget=4,
        active_slots=ACTIVE_SLOTS,
        run_id="run-dag-harness",
        mode="offline_harness",
    )
    controller = RunController(spec, projector, JsonlJournal(path))
    original = await controller.run()
    events = load_events(path)
    restored = replay(path)
    artifact = projector.artifact
    if original.status is not RunStatus.COMPLETE or artifact is None:
        raise RuntimeError(f"offline harness did not complete: {original.failure}")
    if (
        restored.status != original.status
        or restored.tasks != original.tasks
        or restored.evidence != original.evidence
        or restored.candidates != original.candidates
        or restored.workflow_id != original.workflow_id
        or restored.workflow_version != original.workflow_version
        or restored.last_sequence != original.last_sequence
    ):
        raise RuntimeError("replayed state does not match the executed state")

    planned = next(event for event in events if event.kind is EventKind.WORKFLOW_PLANNED)
    completed = next(event for event in events if event.kind is EventKind.WORKFLOW_COMPLETED)
    slot_limit = int(planned.payload["effective_active_slots"])
    scheduler_peak = int(completed.payload["observed_peak_active_tasks"])
    if not (model.peak_active_calls == scheduler_peak == ACTIVE_SLOTS <= slot_limit):
        raise RuntimeError("harness did not prove bounded three-way overlap")
    branch_terminal_sequences = {
        str(event.payload["task_id"]): event.sequence
        for event in events
        if event.kind is EventKind.TASK_COMPLETED and event.payload.get("task_id") in BRANCH_TASK_IDS
    }
    validator_started = next(
        event.sequence
        for event in events
        if event.kind is EventKind.TASK_STARTED and event.payload.get("task_id") == VALIDATOR_TASK_ID
    )
    if set(branch_terminal_sequences) != set(BRANCH_TASK_IDS) or any(
        sequence >= validator_started for sequence in branch_terminal_sequences.values()
    ):
        raise RuntimeError("validator started before every issuer branch completed")
    if model.validator_dependencies != tuple(sorted(BRANCH_TASK_IDS)):
        raise RuntimeError("validator dependency context is incomplete")
    candidate_sequence = max(
        event.sequence for event in events if event.kind is EventKind.CANDIDATE_UPDATED
    )
    workflow_completed_sequence = completed.sequence
    if candidate_sequence >= workflow_completed_sequence:
        raise RuntimeError("ranked candidates were not projected before workflow completion")
    if tuple(restored.candidates) != artifact.ranked_ids:
        raise RuntimeError("replayed candidates do not retain controller-owned ranking order")
    if "ticker:FORGED" in restored.candidates:
        raise RuntimeError("validator altered a controller-owned candidate identity")
    expected_sources = {
        source_id
        for evidence in evidence_by_task.values()
        for source_id in evidence.packet.source_ids
    }
    cited_sources = {
        source_id for candidate in restored.candidates.values() for source_id in candidate.evidence_ids
    }
    if cited_sources != expected_sources or cited_sources != set(restored.evidence):
        raise RuntimeError("candidate source coverage is incomplete")
    branch_statuses = {
        task_id: restored.tasks[task_id].status.value for task_id in BRANCH_TASK_IDS
    }
    if set(branch_statuses.values()) != {TaskStatus.COMPLETE.value}:
        raise RuntimeError("not every issuer branch completed")

    artifact_dict = artifact.to_dict()
    return {
        "ok": True,
        "run_id": restored.spec.run_id,
        "status": restored.status.value,
        "events": restored.last_sequence + 1,
        "model_turns": model.calls,
        "tool_calls": sum(restored.tasks[task_id].tool_calls for task_id in BRANCH_TASK_IDS),
        "branches_completed": len(branch_statuses),
        "branch_statuses": branch_statuses,
        "slot_limit": slot_limit,
        "observed_peak_active_tasks": scheduler_peak,
        "model_peak_active_calls": model.peak_active_calls,
        "slot_bound_verified": scheduler_peak <= slot_limit,
        "dependency_order_verified": True,
        "candidate_count": len(restored.candidates),
        "ranked_ids": list(artifact.ranked_ids),
        "validator_proposed_ranked_ids": list(artifact.validator_proposed_ranked_ids),
        "validator_rank_override_blocked": artifact.validator_rank_override_blocked,
        "source_coverage": {
            "available": len(restored.evidence),
            "cited": len(cited_sources),
            "complete": cited_sources == set(restored.evidence),
        },
        "data_quality_posture": artifact_dict["data_quality_posture"],
        "results_artifact_hash": _canonical_hash(artifact_dict),
        "results_ready": True,
        "journal": str(path.resolve()),
        "replay_equivalent": True,
    }


def _observation(
    issuer: IssuerFixture,
    metric: str,
    value: float,
    fiscal_year: int,
) -> tuple[FinancialObservation, EvidenceRecord]:
    start = date(fiscal_year, 1, 1)
    end = date(fiscal_year, 12, 31)
    period = FinancialPeriod(
        PeriodKind.DURATION,
        start=start,
        end=end,
        fiscal_year=fiscal_year,
        fiscal_period="FY",
    )
    locator: dict[str, JsonValue] = {
        "fixture": "offline-multi-issuer-v1",
        "ticker": issuer.ticker,
        "entity_id": issuer.entity_id,
        "field": metric,
        "period_start": start.isoformat(),
        "period_end": end.isoformat(),
        "value": value,
    }
    evidence_id = evidence_id_for(DataProvider.SEC, "fixture_fact", locator)
    evidence = EvidenceRecord(
        evidence_id=evidence_id,
        provider=DataProvider.SEC,
        source_kind="fixture_fact",
        source_locator=locator,
        source_url=f"https://example.test/{issuer.ticker}/{metric}/{fiscal_year}",
        observed_at=datetime.combine(end, datetime.min.time(), tzinfo=UTC),
        retrieved_at=datetime(2025, 3, 3, 20, tzinfo=UTC),
        content_hash=canonical_content_hash(locator),
    )
    observation = FinancialObservation(
        observation_id=observation_id_for(
            DataProvider.SEC,
            entity_id=issuer.entity_id,
            name=metric,
            period=period,
            evidence_id=evidence_id,
        ),
        entity_id=issuer.entity_id,
        ticker=issuer.ticker,
        name=metric,
        value=value,
        unit=FinancialUnit(UnitKind.CURRENCY, "USD"),
        period=period,
        evidence_ids=(evidence_id,),
        metadata={"fixture": "offline-multi-issuer-v1"},
    )
    return observation, evidence


def _dependency_ids(request: ActionModelRequest) -> tuple[str, ...]:
    first = request.transcript[0]
    content = first.get("content")
    if not isinstance(content, dict):
        raise RuntimeError("validator controller transcript is malformed")
    dependencies = content.get("dependencies")
    if not isinstance(dependencies, dict):
        raise RuntimeError("validator dependency context is malformed")
    for dependency in dependencies.values():
        if not isinstance(dependency, dict) or dependency.get("status") != TaskStatus.COMPLETE.value:
            raise RuntimeError("validator observed a non-complete issuer dependency")
    return tuple(sorted(str(task_id) for task_id in dependencies))


def _evidence_event(issuer: IssuerFixture, record: EvidenceRecord) -> EventDraft:
    metric = str(record.source_locator["field"])
    period_end = str(record.source_locator["period_end"])
    value = record.source_locator["value"]
    return EventDraft(
        EventKind.EVIDENCE_ADDED,
        f"Recorded normalized fixture evidence for {issuer.ticker} {metric}",
        agent_id=issuer.agent_id,
        payload={
            "evidence_id": record.evidence_id,
            "title": f"{issuer.ticker} {metric.replace('_', ' ')} · {period_end}",
            "source": "Offline SEC-shaped fixture",
            "source_kind": record.source_kind,
            "summary": f"Normalized {metric.replace('_', ' ')} is {value} USD.",
            "observed_at": record.observed_at.isoformat(),
            "synthetic": True,
        },
    )


def _candidate_event(result: RankedCandidateResult, formula_version: str) -> EventDraft:
    candidate = result.candidate
    return EventDraft(
        EventKind.CANDIDATE_UPDATED,
        f"Projected controller-ranked result #{result.rank}: {candidate.ticker}",
        payload={
            "candidate_id": candidate.candidate_id,
            "ticker": candidate.ticker,
            "company": candidate.company,
            "bucket": candidate.bucket.value,
            "priority_score": candidate.priority_score,
            "variant_wedge": candidate.variant_wedge,
            "why_now": candidate.why_now,
            "first_rejection": candidate.first_rejection,
            "investable_if": candidate.investable_if,
            "kill_if": candidate.kill_if,
            "next_workflow": candidate.next_workflow,
            "evidence_ids": list(candidate.evidence_ids),
            "financials": [
                {
                    "metric": financial.metric,
                    "label": financial.label,
                    "value": financial.value,
                    "unit": financial.unit,
                    "period": financial.period,
                    "source_ids": list(financial.source_ids),
                }
                for financial in candidate.financials
            ],
            "confidence": candidate.confidence.value,
            "data_quality": candidate.data_quality.value,
            "as_of": candidate.as_of,
            "source_mode": candidate.source_mode.value,
            "evidence_gaps": list(candidate.evidence_gaps),
            "rank": result.rank,
            "composite_score": result.composite_score,
            "ranking_formula_version": formula_version,
        },
    )


def _finite_number(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise RuntimeError(f"{name} must be a finite number")
    return float(value)


def _json_action(value: Mapping[str, object]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _canonical_hash(value: JsonValue) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
