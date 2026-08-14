"""Deterministic plans and bounded projections for automatic live research."""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from alpha_orchestration.data.live import CollectionFailure, LiveCollection, LiveIssuerEvidence
from alpha_orchestration.data.observations import DataProvider, evidence_id_for
from alpha_orchestration.data.universe import UniverseManifest, UniversePolicy
from alpha_orchestration.data.universe_mapping import manifest_market_batch
from alpha_orchestration.domain import EventKind, JsonValue, Stage
from alpha_orchestration.ports import EventDraft

LOGICAL_AGENT_LANES = 8
AUTOMATIC_ANALYSIS_MODE = "RULE-BASED SCREEN"
MODEL_DILIGENCE_MODE = "RULE-BASED SCREEN + OPTIONAL MODEL DILIGENCE"


@dataclass(slots=True)
class FunnelProgress:
    manifest: UniverseManifest
    analysis_mode: str
    configured_provider_slots: int
    batches_total: int
    stage: str = "discovery"
    batches_completed: int = 0
    eligible: int = 0
    screened: int = 0
    deep_reviewed: int = 0
    surfaced: int = 0
    failed_tickers: set[str] = field(default_factory=set)
    observed_peak_provider_requests: int = 0
    observed_peak_analysis_tasks: int = 0
    universe_rows: list[JsonValue] | None = None

    def snapshot(self, *, include_rows: bool = False) -> dict[str, JsonValue]:
        selected = len(self.manifest.members)
        inspected = self.manifest.fetched_row_count
        provider_matches = self.manifest.provider_reported_total
        sources = (self.manifest.sec_source, *self.manifest.market_sources)
        retrieved_at = max(source.retrieved_at for source in sources)
        access_modes = {source.access_mode for source in sources}
        cache_posture = "cache" if access_modes == {"cache"} else "network" if access_modes == {"network"} else "mixed"
        snapshot: dict[str, JsonValue] = {
            "profile": self.manifest.policy.profile_id,
            "stage": self.stage,
            "total": selected,
            "provider_matches": provider_matches,
            "provider_total": provider_matches,
            "inspected": inspected,
            "screened_unique": self.manifest.screened_unique_count,
            "uninspected": max(0, provider_matches - inspected),
            "selected": selected,
            "discovered": selected,
            "eligible": self.eligible,
            "screened": self.screened,
            "deep_reviewed": self.deep_reviewed,
            "surfaced": self.surfaced,
            # Fetched provider rows not retained in the selected cohort; uninspected rows stay separate.
            "excluded": max(0, inspected - selected),
            "failed": len(self.failed_tickers),
            "source": "SEC identity and company facts; yfinance equity-screen market fields",
            "source_posture": (
                "SEC identity and comparable company facts required; source-bound screener market fields"
            ),
            "as_of": self.manifest.discovered_at.date().isoformat(),
            "retrieved_at": retrieved_at.isoformat(),
            "cache": cache_posture,
            "cache_posture": cache_posture,
            "batches_completed": self.batches_completed,
            "batches_total": self.batches_total,
            "configured_agent_lanes": LOGICAL_AGENT_LANES,
            "configured_provider_slots": self.configured_provider_slots,
            "observed_peak_provider_requests": self.observed_peak_provider_requests,
            "observed_peak_analysis_tasks": self.observed_peak_analysis_tasks,
            "analysis_mode": self.analysis_mode,
        }
        if include_rows and self.universe_rows is not None:
            snapshot["universe_rows"] = list(self.universe_rows)
        return snapshot


def empty_funnel(policy: UniversePolicy, model_configured: bool, batch_size: int = 25) -> dict[str, JsonValue]:
    return {
        "profile": policy.profile_id,
        "stage": "discovery",
        "total": policy.target_size,
        "provider_matches": 0,
        "provider_total": 0,
        "inspected": 0,
        "uninspected": 0,
        "selected": 0,
        "discovered": 0,
        "eligible": 0,
        "screened": 0,
        "deep_reviewed": 0,
        "surfaced": 0,
        "excluded": 0,
        "failed": 0,
        "source": "SEC and yfinance live providers",
        "source_posture": "awaiting source-bound automatic discovery",
        "as_of": "pending",
        "retrieved_at": "pending",
        "cache": "pending",
        "cache_posture": "pending",
        "batches_completed": 0,
        "batches_total": math.ceil(policy.target_size / batch_size),
        "configured_agent_lanes": LOGICAL_AGENT_LANES,
        "configured_provider_slots": 0,
        "observed_peak_provider_requests": 0,
        "observed_peak_analysis_tasks": 0,
        "analysis_mode": MODEL_DILIGENCE_MODE if model_configured else AUTOMATIC_ANALYSIS_MODE,
    }


def stage_event(
    kind: EventKind,
    stage: Stage,
    message: str,
    progress: int,
    funnel: Mapping[str, JsonValue],
    extra: Mapping[str, JsonValue] | None = None,
) -> EventDraft:
    payload: dict[str, JsonValue] = {
        "stage": stage.value,
        "progress": progress,
        "universe_funnel": dict(funnel),
    }
    payload.update(extra or {})
    return EventDraft(kind, message, payload=payload)


def aggregate_live_snapshot(
    manifest: UniverseManifest,
    collections: Sequence[LiveCollection],
) -> dict[str, JsonValue]:
    issuers = [issuer for collection in collections for issuer in collection.issuers]
    failures = [failure for collection in collections for failure in collection.failures]
    by_ticker = {issuer.issuer.ticker: issuer for issuer in issuers}
    provider_successes = Counter({"sec": 0, "yfinance": 0})
    provider_failures = Counter({"sec": 0, "yfinance": 0})
    for issuer in issuers:
        for provider in ("sec", "yfinance"):
            if issuer.provider_status.get(provider, {}).get("status") == "ok":
                provider_successes[provider] += 1
    for failure in failures:
        if failure.provider in provider_failures:
            provider_failures[failure.provider] += 1
    issuer_rows: list[JsonValue] = []
    for member in manifest.members:
        issuer = by_ticker.get(member.ticker)
        if issuer is None:
            issuer_rows.append({"ticker": member.ticker, "status": "failed"})
            continue
        issuer_rows.append(
            {
                "ticker": member.ticker,
                "cik": member.cik,
                "company": member.company,
                "status": "ready",
                "providers": {key: dict(value) for key, value in issuer.provider_status.items()},
                "observation_count": len(issuer.packet.observations),
                "evidence_count": len(issuer.source_ids),
                "normalization_issue_count": len(issuer.normalization_issues),
            }
        )
    return {
        "requested_tickers": list(manifest.tickers),
        "requested_count": len(manifest.members),
        "ready_count": len(by_ticker),
        "failed_count": len(manifest.members) - len(by_ticker),
        "partial": len(by_ticker) != len(manifest.members) or bool(failures),
        "issuers": issuer_rows,
        "failures": [failure.to_dict() for failure in failures],
        "provider_successes": dict(provider_successes),
        "provider_failures": dict(provider_failures),
        "mapping": manifest.sec_source.to_dict(),
        "configured_provider_slots": collections[0].configured_provider_slots if collections else 0,
        "observed_peak_provider_requests": max(
            (collection.observed_peak_provider_requests for collection in collections),
            default=0,
        ),
    }


def failure_reason(ticker: str, failures: Sequence[CollectionFailure]) -> str:
    relevant = [failure for failure in failures if failure.ticker == ticker]
    if not relevant:
        return "trusted SEC evidence packet unavailable"
    return "; ".join(f"{failure.provider}/{failure.phase}: {failure.error}" for failure in relevant)


def scaled_progress(completed: int, total: int, start: int, end: int) -> int:
    return start + int((end - start) * completed / max(1, total))


def analysis_mode(model_configured: bool) -> str:
    return MODEL_DILIGENCE_MODE if model_configured else AUTOMATIC_ANALYSIS_MODE


def lane_id(index: int) -> str:
    return f"automatic-lane-{index % LOGICAL_AGENT_LANES + 1:02d}"


def _slug(ticker: str) -> str:
    return ticker.lower().replace(".", "-")


def screen_task_id(index: int, ticker: str) -> str:
    return f"screen-{index + 1:04d}-{_slug(ticker)}"


def validator_task_id() -> str:
    return "validate-automatic-ranking"


def diligence_lane_task_id(lane_number: int) -> str:
    return f"diligence-lane-{lane_number + 1:02d}"


def scaled_planned_tasks(
    manifest: UniverseManifest,
    lane_by_ticker: Mapping[str, str],
    *,
    include_diligence: bool,
    diligence_limit: int = LOGICAL_AGENT_LANES,
) -> list[JsonValue]:
    if not 1 <= diligence_limit <= LOGICAL_AGENT_LANES:
        raise ValueError("diligence_limit must be between 1 and 8")
    tasks: list[JsonValue] = []
    screen_ids: list[str] = []
    for index, member in enumerate(manifest.members):
        task_id = screen_task_id(index, member.ticker)
        screen_ids.append(task_id)
        tasks.append(
            {
                "task_id": task_id,
                "agent_id": lane_by_ticker[member.ticker],
                "depends_on": [],
                "required": False,
                "allow_failed_dependencies": False,
            }
        )
    diligence_ids: list[str] = []
    if include_diligence:
        for lane_number in range(diligence_limit):
            task_id = diligence_lane_task_id(lane_number)
            diligence_ids.append(task_id)
            tasks.append(
                {
                    "task_id": task_id,
                    "agent_id": lane_id(lane_number),
                    "depends_on": list(screen_ids),
                    "required": False,
                    "allow_failed_dependencies": True,
                }
            )
    tasks.append(
        {
            "task_id": validator_task_id(),
            "agent_id": lane_id(0),
            "depends_on": [*screen_ids, *diligence_ids],
            "required": True,
            "allow_failed_dependencies": True,
        }
    )
    return tasks


def audit_universe_rows(
    manifest: UniverseManifest,
    ready_by_ticker: Mapping[str, LiveIssuerEvidence],
    failed_tickers: set[str],
    failures: Mapping[str, str],
    score_by_entity: Mapping[str, JsonValue],
    data_quality_by_entity: Mapping[str, str],
    surfaced_entities: set[str],
    diligence_entities: set[str],
) -> list[JsonValue]:
    rows: list[JsonValue] = []
    for member in manifest.members:
        entity_id = member.entity_id
        issuer = ready_by_ticker.get(member.ticker)
        failed = member.ticker in failed_tickers
        score = score_by_entity.get(entity_id)
        screen_score = int(round(float(score))) if isinstance(score, (int, float)) else None
        if issuer is None:
            locator: dict[str, JsonValue] = {
                "ticker": member.ticker,
                "cik": member.cik,
                "company": member.company,
                "mapping_content_hash": member.sec_content_hash,
            }
            identity_id = evidence_id_for(DataProvider.SEC, "company_ticker_identity", locator)
            source_ids = [identity_id, *(item.evidence_id for item in manifest_market_batch(member).evidence)]
        else:
            source_ids = list(issuer.source_ids)
        rows.append(
            {
                "universe_rank": member.rank,
                "ticker": member.ticker,
                "company": member.company,
                "cik": member.cik,
                "exchange": member.sec_exchange,
                "market_cap": member.market_cap,
                "currency": member.currency,
                "status": "failed" if failed else "screened",
                "screen_score": screen_score,
                "deep_reviewed": entity_id in diligence_entities,
                "surfaced": entity_id in surfaced_entities,
                "data_quality": "failed" if failed else data_quality_by_entity.get(entity_id, "partial"),
                "failure": failures.get(member.ticker),
                "source_ids": source_ids,
            }
        )
    return rows
