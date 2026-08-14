"""Trusted rule-based live SEC/yfinance research runtime.

Providers supply evidence; controller-owned calculations supply metrics and
ranking. The first live prototype intentionally labels its analysis as
rule-based instead of implying that a language model made an investment call.
"""

from __future__ import annotations

import asyncio
import importlib.util
import math
import os
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

from alpha_orchestration.calculations.metrics import calculate_metrics
from alpha_orchestration.calculations.ranking import rank_entities
from alpha_orchestration.data.cache import ContentAddressedJsonCache
from alpha_orchestration.data.live import (
    LiveCollection,
    LiveCollectionError,
    LiveDataCollector,
    LiveIssuerEvidence,
    MarketLiveProvider,
    SecLiveProvider,
    normalize_live_tickers,
)
from alpha_orchestration.data.observations import EvidenceRecord, FinancialObservation
from alpha_orchestration.data.sec import SecDataClient
from alpha_orchestration.data.yfinance import YFinanceClient
from alpha_orchestration.domain import (
    CandidateBucket,
    CandidateConfidence,
    CandidateDataQuality,
    CandidateSourceMode,
    EventKind,
    JsonValue,
    RunSpec,
    Stage,
)
from alpha_orchestration.ports import EventDraft

ANALYSIS_MODE = "RULE-BASED"
WORKFLOW_ID = "live-equity-triage"
WORKFLOW_VERSION = "1.0.0"
RANKING_FORMULA_VERSION = "live-equity-ranking-v1"


@dataclass(frozen=True, slots=True)
class IssuerAnalysis:
    evidence: LiveIssuerEvidence
    metrics: Mapping[str, float | None]
    tool_payload: Mapping[str, JsonValue]
    source_ids: tuple[str, ...]


def live_environment_readiness(*, env_file: Path = Path(".env")) -> dict[str, bool]:
    """Return non-secret local readiness facts without making network calls."""

    return {
        "sec_identity_configured": bool(_sec_identity(env_file)),
        "yfinance_installed": importlib.util.find_spec("yfinance") is not None,
    }


def build_live_runtime(
    tickers: Sequence[str],
    *,
    cache_root: Path = Path("artifacts/live-cache"),
    cache_max_age: timedelta = timedelta(hours=6),
    ticker_map_max_age: timedelta = timedelta(days=7),
    provider_slots: int = 4,
    provider_timeout_seconds: float = 30.0,
    sec: SecLiveProvider | None = None,
    market: MarketLiveProvider | None = None,
    now: Any | None = None,
    env_file: Path = Path(".env"),
) -> LiveRuntime:
    """Build a fail-closed live runtime; no fixture fallback is permitted."""

    normalized = normalize_live_tickers(tickers)
    owned_sec: SecDataClient | None = None
    if sec is None:
        identity = _sec_identity(env_file)
        if not identity:
            raise RuntimeError("Live SEC access requires ALPHA_SEC_USER_AGENT in the environment or .env")
        owned_sec = SecDataClient(identity)
        sec = owned_sec
    if market is None:
        if importlib.util.find_spec("yfinance") is None:
            raise RuntimeError('Live market data requires the optional data extra: pip install -e ".[data]"')
        market = YFinanceClient()
    collector = LiveDataCollector(
        sec,
        market,
        ContentAddressedJsonCache(cache_root),
        cache_max_age=cache_max_age,
        ticker_map_max_age=ticker_map_max_age,
        provider_slots=provider_slots,
        provider_timeout_seconds=provider_timeout_seconds,
        now=now,
    )
    return LiveRuntime(normalized, collector, owned_sec=owned_sec)


class LiveRuntime:
    """Collect live evidence, analyze issuers in parallel, and project candidates."""

    def __init__(
        self,
        tickers: Sequence[str],
        collector: LiveDataCollector,
        *,
        owned_sec: SecDataClient | None = None,
    ) -> None:
        self.tickers = normalize_live_tickers(tickers)
        self.collector = collector
        self._owned_sec = owned_sec

    async def stream(self, spec: RunSpec) -> AsyncIterator[EventDraft]:
        if spec.mode != "live":
            raise ValueError("LiveRuntime requires RunSpec.mode='live'")
        if spec.universe_size != len(self.tickers):
            raise ValueError("RunSpec universe_size must equal the explicit live ticker count")
        if len(self.tickers) > spec.agent_budget:
            raise ValueError("live ticker count must not exceed the logical agent budget")
        try:
            async for draft in self._stream(spec):
                yield draft
        finally:
            if self._owned_sec is not None:
                await self._owned_sec.close()

    async def _stream(self, spec: RunSpec) -> AsyncIterator[EventDraft]:
        yield _stage_started(Stage.UNIVERSE, "Resolving official SEC issuer identities", 4)
        yield _stage_completed(
            Stage.UNIVERSE,
            "Ticker universe fixed by the operator",
            8,
            {"requested_tickers": list(self.tickers), "requested_count": len(self.tickers)},
        )
        yield _stage_started(
            Stage.EVIDENCE,
            "Collecting SEC filings and yfinance market snapshots",
            12,
            {
                "live_collection": {
                    "requested_tickers": list(self.tickers),
                    "requested_count": len(self.tickers),
                    "ready_count": 0,
                    "status": "collecting",
                }
            },
        )
        collection = await self.collector.collect(self.tickers)
        snapshot = collection.snapshot()
        if not collection.issuers:
            raise LiveCollectionError("No requested ticker produced a trusted SEC evidence packet")
        yield _stage_completed(
            Stage.EVIDENCE,
            _collection_message(collection),
            35,
            {"live_collection": snapshot},
        )

        yield EventDraft(
            EventKind.WORKFLOW_PLANNED,
            f"Planned {len(collection.issuers)} issuer analyses plus trusted ranking",
            payload={
                "workflow_id": WORKFLOW_ID,
                "workflow_version": WORKFLOW_VERSION,
                "tasks": _planned_tasks(collection),
                "configured_active_slots": spec.active_slots,
                "requested_active_slots": spec.active_slots,
                "effective_active_slots": min(spec.active_slots, len(collection.issuers)),
                "actual_active_slots": None,
                "analysis_mode": ANALYSIS_MODE,
                "live_collection": snapshot,
            },
        )
        for issuer in collection.issuers:
            agent_id = _agent_id(issuer)
            yield EventDraft(
                EventKind.AGENT_REGISTERED,
                f"Registered {issuer.issuer.ticker} research lane",
                agent_id=agent_id,
                payload={"role": "Live issuer analyst", "lane": issuer.issuer.ticker},
            )
            yield EventDraft(
                EventKind.AGENT_STARTED,
                f"Reviewing {issuer.issuer.ticker} source packet",
                agent_id=agent_id,
                payload={"progress": 10},
            )
            for record in (issuer.identity_evidence, *issuer.packet.evidence):
                yield _evidence_event(agent_id, issuer, record)

        yield _stage_started(Stage.ANALYSIS, "Calculating comparable issuer metrics", 40)
        for issuer in collection.issuers:
            yield EventDraft(
                EventKind.TASK_STARTED,
                f"Analyzing {issuer.issuer.ticker}",
                agent_id=_agent_id(issuer),
                payload={"task_id": issuer.task_id},
            )
            yield EventDraft(
                EventKind.TOOL_STARTED,
                "Running controller-owned finance.metrics",
                agent_id=_agent_id(issuer),
                payload={
                    "issuer_task_id": issuer.task_id,
                    "tool_name": "finance.metrics",
                    "source_ids": list(issuer.packet.source_ids),
                },
            )

        analyses, peak = await _analyze_issuers(collection.issuers, spec.active_slots)
        for analysis in analyses:
            issuer = analysis.evidence
            yield EventDraft(
                EventKind.TOOL_COMPLETED,
                "Financial metrics calculated from bound observations",
                agent_id=_agent_id(issuer),
                payload={
                    "issuer_task_id": issuer.task_id,
                    "tool_name": "finance.metrics",
                    "result": dict(analysis.tool_payload),
                    "source_ids": list(analysis.source_ids),
                },
            )
            gaps = _analysis_gaps(analysis)
            yield EventDraft(
                EventKind.TASK_COMPLETED,
                f"Completed {issuer.issuer.ticker} rule-based analysis",
                agent_id=_agent_id(issuer),
                payload=_task_output_payload(issuer.task_id, gaps, {"metrics": _json_metrics(analysis.metrics)}),
            )

        yield _stage_completed(Stage.ANALYSIS, "Issuer calculations complete", 72)
        yield EventDraft(EventKind.RUN_SYNTHESIZING, "Ranking live research priorities")
        yield _stage_started(Stage.SYNTHESIS, "Applying deterministic cross-issuer rank", 76)
        validator_task = _validator_task_id()
        lead_agent = _agent_id(collection.issuers[0])
        yield EventDraft(
            EventKind.TASK_STARTED,
            "Validating issuer-bound ranking inputs",
            agent_id=lead_agent,
            payload={"task_id": validator_task},
        )
        ranked = _rank_analyses(analyses)
        yield EventDraft(
            EventKind.TASK_COMPLETED,
            "Trusted live ranking complete",
            agent_id=lead_agent,
            payload=_task_output_payload(
                validator_task,
                ["one or more live sources were unavailable"] if snapshot["partial"] else [],
                ranked,
            ),
        )
        ranked_rows = ranked["ranked"]
        assert isinstance(ranked_rows, list)
        for rank, raw_row in enumerate(ranked_rows, 1):
            assert isinstance(raw_row, dict)
            analysis = next(item for item in analyses if item.evidence.issuer.entity_id == raw_row["id"])
            yield EventDraft(
                EventKind.CANDIDATE_UPDATED,
                f"Ranked {analysis.evidence.issuer.ticker} as research priority {rank}",
                payload=_candidate_payload(analysis, raw_row, rank, len(analyses)),
            )
        for issuer in collection.issuers:
            yield EventDraft(
                EventKind.AGENT_COMPLETED,
                f"{issuer.issuer.ticker} lane complete",
                agent_id=_agent_id(issuer),
            )
        yield EventDraft(
            EventKind.WORKFLOW_COMPLETED,
            _collection_message(collection),
            payload={
                "workflow_id": WORKFLOW_ID,
                "observed_peak_active_tasks": peak,
                "analysis_mode": ANALYSIS_MODE,
                "ranking_formula_version": RANKING_FORMULA_VERSION,
                "live_collection": snapshot,
            },
        )
        yield _stage_completed(Stage.SYNTHESIS, "Live research priorities ready", 96)


async def _analyze_issuers(
    issuers: Sequence[LiveIssuerEvidence],
    active_slots: int,
) -> tuple[tuple[IssuerAnalysis, ...], int]:
    semaphore = asyncio.Semaphore(min(active_slots, len(issuers)))
    active = 0
    peak = 0
    lock = asyncio.Lock()

    async def analyze(evidence: LiveIssuerEvidence) -> IssuerAnalysis:
        nonlocal active, peak
        async with semaphore:
            async with lock:
                active += 1
                peak = max(peak, active)
            try:
                await asyncio.sleep(0)
                return _analyze_one(evidence)
            finally:
                async with lock:
                    active -= 1

    results = await asyncio.gather(*(analyze(issuer) for issuer in issuers))
    return tuple(results), peak


def _analyze_one(evidence: LiveIssuerEvidence) -> IssuerAnalysis:
    selected = evidence.observations_by_name
    values = {name: observation.value for name, observation in selected.items()}
    requested = (
        "revenue_growth",
        "net_margin",
        "free_cash_flow",
        "free_cash_flow_margin",
        "price_to_sales",
    )
    source_ids = tuple(
        dict.fromkeys(source_id for observation in selected.values() for source_id in observation.evidence_ids)
    )
    payload = calculate_metrics(
        {
            "values": values,
            "metrics": list(requested),
            "precision": 6,
            "context": {
                "entity_id": evidence.issuer.entity_id,
                "ticker": evidence.issuer.ticker,
                "source_ids": list(source_ids),
            },
        }
    )
    raw_metrics = payload["values"]
    assert isinstance(raw_metrics, dict)
    metrics = {
        name: (
            float(raw_metrics[name]) if name in raw_metrics and isinstance(raw_metrics[name], (int, float)) else None
        )
        for name in requested
    }
    return IssuerAnalysis(evidence, metrics, payload, source_ids)


def _rank_analyses(analyses: Sequence[IssuerAnalysis]) -> dict[str, JsonValue]:
    ranked = rank_entities(
        {
            "rows": [
                {
                    "id": item.evidence.issuer.entity_id,
                    "metrics": {
                        "revenue_growth": item.metrics["revenue_growth"],
                        "net_margin": item.metrics["net_margin"],
                        "free_cash_flow_margin": item.metrics["free_cash_flow_margin"],
                        "price_to_sales": item.metrics["price_to_sales"],
                    },
                }
                for item in analyses
            ],
            "criteria": [
                {"metric": "revenue_growth", "direction": "higher", "weight": 0.35},
                {"metric": "net_margin", "direction": "higher", "weight": 0.25},
                {
                    "metric": "free_cash_flow_margin",
                    "direction": "higher",
                    "weight": 0.15,
                },
                {"metric": "price_to_sales", "direction": "lower", "weight": 0.25},
            ],
            "missing_policy": "worst",
            "precision": 6,
            "context": {
                "analysis_mode": ANALYSIS_MODE,
                "ranking_formula_version": RANKING_FORMULA_VERSION,
            },
        }
    )
    ranked["trusted_input_source"] = "controller-owned finance.metrics outputs"
    ranked["ranking_formula_version"] = RANKING_FORMULA_VERSION
    return ranked


def _candidate_payload(
    analysis: IssuerAnalysis,
    row: Mapping[str, JsonValue],
    rank: int,
    count: int,
) -> dict[str, JsonValue]:
    issuer = analysis.evidence
    ticker = issuer.issuer.ticker
    composite = row.get("composite_score")
    priority = int(round(float(composite))) if isinstance(composite, (int, float)) else 0
    gaps = _analysis_gaps(analysis)
    source_ids = tuple(dict.fromkeys((issuer.identity_evidence.evidence_id, *analysis.source_ids)))
    has_market = issuer.provider_status.get("yfinance", {}).get("status") == "ok"
    available = sum(value is not None for value in analysis.metrics.values())
    confidence = (
        CandidateConfidence.HIGH
        if has_market and available >= 4
        else CandidateConfidence.MEDIUM
        if available >= 2
        else CandidateConfidence.LOW
    )
    quality = (
        CandidateDataQuality.COMPLETE
        if not gaps
        else CandidateDataQuality.PARTIAL
        if available >= 2
        else CandidateDataQuality.LIMITED
    )
    if not has_market:
        bucket = CandidateBucket.EXPOSURE_UNPROVEN
    elif rank == 1:
        bucket = CandidateBucket.ADVANCE
    elif rank <= max(2, math.ceil(count / 2)):
        bucket = CandidateBucket.VALUATION_GATED
    else:
        bucket = CandidateBucket.DEPRIORITIZED
    growth = _metric_text(analysis.metrics["revenue_growth"], percent=True)
    margin = _metric_text(analysis.metrics["net_margin"], percent=True)
    valuation = _metric_text(analysis.metrics["price_to_sales"], multiple=True)
    period_end = issuer.observations_by_name["revenue"].period.end.isoformat()
    return {
        "candidate_id": issuer.issuer.entity_id,
        "ticker": ticker,
        "company": issuer.issuer.company,
        "bucket": bucket.value,
        "priority_score": max(0, min(100, priority)),
        "variant_wedge": (
            f"Rule screen rank {rank}/{count}: revenue growth {growth}, net margin {margin}, price/sales {valuation}."
        ),
        "why_now": (
            f"Latest comparable SEC fiscal-year evidence ends {period_end}; "
            "market fields use separately timestamped yfinance evidence when available."
        ),
        "first_rejection": (
            "A deterministic screen can surface accounting or valuation outliers without "
            "establishing estimate durability, business quality, or a true market mispricing."
        ),
        "investable_if": (
            "Deeper filing, earnings, and valuation work confirms the metric signal and "
            "identifies a source-backed variant view."
        ),
        "kill_if": (
            "Restated periods, weak cash conversion, stale market data, or follow-up evidence "
            "invalidate the apparent cross-issuer advantage."
        ),
        "next_workflow": "company_tearsheet_and_earnings_diligence",
        "evidence_ids": list(source_ids),
        "financials": _candidate_financials(analysis),
        "confidence": confidence.value,
        "data_quality": quality.value,
        "as_of": period_end,
        "source_mode": CandidateSourceMode.LIVE.value,
        "evidence_gaps": gaps,
    }


def _candidate_financials(analysis: IssuerAnalysis) -> list[JsonValue]:
    observations = analysis.evidence.observations_by_name
    revenue = observations["revenue"]
    financials: list[JsonValue] = [
        _financial_record(
            "revenue",
            "Revenue",
            _currency_millions(revenue),
            f"{revenue.unit.symbol} millions",
            revenue.period.end.isoformat(),
            revenue.evidence_ids,
        )
    ]
    metric_specs = (
        ("revenue_growth", "Revenue growth", "ratio", ("revenue", "prior_revenue")),
        ("net_margin", "Net margin", "ratio", ("revenue", "net_income")),
        (
            "free_cash_flow_margin",
            "Free cash flow margin",
            "ratio",
            ("revenue", "operating_cash_flow", "capital_expenditures"),
        ),
        ("price_to_sales", "Price / sales", "x", ("revenue", "market_cap")),
    )
    for metric, label, unit, names in metric_specs:
        value = analysis.metrics.get(metric)
        if value is None or not all(name in observations for name in names):
            continue
        cited = tuple(dict.fromkeys(source_id for name in names for source_id in observations[name].evidence_ids))
        financials.append(
            _financial_record(
                metric,
                label,
                value,
                unit,
                revenue.period.end.isoformat(),
                cited,
            )
        )
    share_price = observations.get("share_price")
    if share_price is not None:
        financials.append(
            _financial_record(
                "share_price",
                "Share price",
                share_price.value,
                share_price.unit.symbol,
                share_price.period.end.isoformat(),
                share_price.evidence_ids,
            )
        )
    return financials


def _financial_record(
    metric: str,
    label: str,
    value: float | int,
    unit: str,
    period: str,
    source_ids: Sequence[str],
) -> dict[str, JsonValue]:
    return {
        "metric": metric,
        "label": label,
        "value": float(value),
        "unit": unit,
        "period": period,
        "source_ids": list(source_ids),
    }


def _analysis_gaps(analysis: IssuerAnalysis) -> list[str]:
    gaps: list[str] = []
    required = {
        "revenue_growth": "prior comparable revenue",
        "net_margin": "same-period net income",
        "free_cash_flow_margin": "same-period cash flow and capital expenditure",
        "price_to_sales": "current market capitalization",
    }
    for metric, description in required.items():
        if analysis.metrics.get(metric) is None:
            gaps.append(f"Missing {description}; {metric.replace('_', ' ')} unavailable")
    for provider, status in analysis.evidence.provider_status.items():
        if status.get("status") == "failed":
            gaps.append(f"{provider} provider unavailable for this issuer")
    return gaps


def _planned_tasks(collection: LiveCollection) -> list[JsonValue]:
    branches = [
        {
            "task_id": issuer.task_id,
            "agent_id": _agent_id(issuer),
            "depends_on": [],
            "required": True,
            "allow_failed_dependencies": False,
        }
        for issuer in collection.issuers
    ]
    branches.append(
        {
            "task_id": _validator_task_id(),
            "agent_id": _agent_id(collection.issuers[0]),
            "depends_on": [issuer.task_id for issuer in collection.issuers],
            "required": True,
            "allow_failed_dependencies": False,
        }
    )
    return branches


def _evidence_event(
    agent_id: str,
    issuer: LiveIssuerEvidence,
    record: EvidenceRecord,
) -> EventDraft:
    return EventDraft(
        EventKind.EVIDENCE_ADDED,
        f"Added {record.provider.value.upper()} evidence for {issuer.issuer.ticker}",
        agent_id=agent_id,
        payload={
            "evidence_id": record.evidence_id,
            "title": f"{issuer.issuer.ticker} · {record.source_kind.replace('_', ' ')}",
            "source": record.provider.value.upper(),
            "source_kind": record.source_kind,
            "summary": (f"Live {record.provider.value} record retrieved {record.retrieved_at.isoformat()}"),
            "observed_at": record.observed_at.isoformat(),
            "retrieved_at": record.retrieved_at.isoformat(),
            "source_url": record.source_url,
            "content_hash": record.content_hash,
            "synthetic": False,
        },
    )


def _stage_started(
    stage: Stage,
    message: str,
    progress: int,
    extra: Mapping[str, JsonValue] | None = None,
) -> EventDraft:
    payload: dict[str, JsonValue] = {"stage": stage.value, "progress": progress}
    payload.update(extra or {})
    return EventDraft(EventKind.STAGE_STARTED, message, payload=payload)


def _stage_completed(
    stage: Stage,
    message: str,
    progress: int,
    extra: Mapping[str, JsonValue] | None = None,
) -> EventDraft:
    payload: dict[str, JsonValue] = {"stage": stage.value, "progress": progress}
    payload.update(extra or {})
    return EventDraft(EventKind.STAGE_COMPLETED, message, payload=payload)


def _collection_message(collection: LiveCollection) -> str:
    ready = len(collection.issuers)
    requested = len(collection.requested_tickers)
    if collection.failures:
        return f"Live collection partial: {ready}/{requested} issuers ready"
    return f"Live collection complete: {ready}/{requested} issuers ready"


def _agent_id(evidence: LiveIssuerEvidence) -> str:
    return f"live-{evidence.issuer.ticker.lower().replace('.', '-')}"


def _validator_task_id() -> str:
    return "validate-live-ranking"


def _task_output_payload(
    task_id: str,
    gaps: Sequence[str],
    output: Mapping[str, JsonValue],
) -> dict[str, JsonValue]:
    payload: dict[str, JsonValue] = {
        "task_id": task_id,
        "partial": bool(gaps),
        "output": dict(output),
    }
    if gaps:
        payload["error"] = "; ".join(gaps)
    return payload


def _json_metrics(metrics: Mapping[str, float | None]) -> dict[str, JsonValue]:
    return {name: value for name, value in metrics.items()}


def _metric_text(
    value: float | None,
    *,
    percent: bool = False,
    multiple: bool = False,
) -> str:
    if value is None:
        return "unavailable"
    if percent:
        return f"{value * 100:.1f}%"
    if multiple:
        return f"{value:.1f}x"
    return f"{value:.2f}"


def _currency_millions(observation: FinancialObservation) -> float:
    factors = {"units": 1e-6, "thousands": 1e-3, "millions": 1.0, "billions": 1e3}
    return float(observation.value) * factors[observation.unit.scale]


def _sec_identity(env_file: Path) -> str:
    configured = os.getenv("ALPHA_SEC_USER_AGENT", "").strip()
    if configured:
        return configured
    try:
        lines = env_file.read_text(encoding="utf-8").splitlines()
    except OSError:
        return ""
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.strip() != "ALPHA_SEC_USER_AGENT":
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        return value.strip()
    return ""
