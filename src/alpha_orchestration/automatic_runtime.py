"""Automatic broad-universe live research over eight reusable logical lanes."""

from __future__ import annotations

import asyncio
import importlib.util
import math
import os
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from alpha_orchestration.automatic_diligence import (
    DILIGENCE_OUTPUT_SCHEMA,
    BoundedDiligenceRunner,
    DiligenceJob,
    canonical_hash,
)
from alpha_orchestration.automatic_projection import (
    LOGICAL_AGENT_LANES,
    FunnelProgress,
    aggregate_live_snapshot,
    analysis_mode,
    audit_universe_rows,
    diligence_lane_task_id,
    empty_funnel,
    failure_reason,
    lane_id,
    scaled_planned_tasks,
    scaled_progress,
    screen_task_id,
    stage_event,
    validator_task_id,
)
from alpha_orchestration.data.cache import ContentAddressedJsonCache
from alpha_orchestration.data.live import LiveCollection, LiveDataCollector, LiveIssuerEvidence
from alpha_orchestration.data.sec import SecDataClient
from alpha_orchestration.data.universe import (
    AutomaticUniverseDiscovery,
    UniverseCoverageError,
    UniverseDiscoveryError,
    UniverseManifest,
    UniversePolicy,
)
from alpha_orchestration.data.yfinance import YFinanceClient
from alpha_orchestration.domain import EventKind, JsonValue, RunSpec, Stage
from alpha_orchestration.live_runtime import (
    IssuerAnalysis,
    _analysis_gaps,
    _analyze_one,
    _candidate_payload,
    _evidence_event,
    _json_metrics,
    _rank_analyses,
    _task_output_payload,
)
from alpha_orchestration.ports import ActionModel, ActionModelRequest, EventDraft

WORKFLOW_ID = "automatic-live-equity-research"
WORKFLOW_VERSION = "2.0.0"
DEFAULT_AUTOMATIC_POLICY = UniversePolicy(target_size=300, minimum_size=200)
MAX_AUTOMATIC_UNIVERSE = 500


class AutomaticCoverageError(RuntimeError):
    """Raised after journaling when too little of the selected cohort was screened."""


@dataclass(slots=True)
class _Execution:
    manifest: UniverseManifest
    progress: FunnelProgress
    lane_by_ticker: dict[str, str]
    index_by_ticker: dict[str, int]
    collections: list[LiveCollection]
    ready_by_ticker: dict[str, LiveIssuerEvidence]
    collection_errors: dict[str, str]
    live_snapshot: dict[str, JsonValue]


class AutomaticLiveRuntime:
    """Discover and screen hundreds of issuers through eight persistent lanes."""

    def __init__(
        self,
        discovery: AutomaticUniverseDiscovery,
        collector: LiveDataCollector,
        *,
        policy: UniversePolicy | None = None,
        collection_batch_size: int = 25,
        candidate_limit: int = 25,
        diligence_model: ActionModel | None = None,
        diligence_limit: int = 8,
        diligence_slots: int = 4,
        diligence_timeout_seconds: float = 60.0,
        minimum_screened_ratio: float = 0.70,
        minimum_screened_count: int = 100,
        analysis_function: Callable[[LiveIssuerEvidence], IssuerAnalysis] = _analyze_one,
        owned_sec: SecDataClient | None = None,
    ) -> None:
        selected_policy = policy or DEFAULT_AUTOMATIC_POLICY
        if selected_policy.target_size > MAX_AUTOMATIC_UNIVERSE:
            raise ValueError(f"automatic runtime supports at most {MAX_AUTOMATIC_UNIVERSE} selected issuers")
        if not 1 <= collection_batch_size <= 100:
            raise ValueError("collection_batch_size must be between 1 and 100")
        if not 1 <= candidate_limit <= min(100, selected_policy.target_size):
            raise ValueError("candidate_limit must be between 1 and 100 and no larger than the universe")
        if not 1 <= diligence_limit <= min(candidate_limit, LOGICAL_AGENT_LANES):
            raise ValueError("diligence_limit must be between 1 and min(candidate_limit, 8)")
        if not 1 <= diligence_slots <= LOGICAL_AGENT_LANES:
            raise ValueError("diligence_slots must be between 1 and 8")
        if not 0 < diligence_timeout_seconds <= 120:
            raise ValueError("diligence_timeout_seconds must be in (0, 120]")
        if not 0 < minimum_screened_ratio <= 1:
            raise ValueError("minimum_screened_ratio must be in (0, 1]")
        if not 1 <= minimum_screened_count <= selected_policy.target_size:
            raise ValueError("minimum_screened_count must be between 1 and target_size")
        self.discovery = discovery
        self.collector = collector
        self.policy = selected_policy
        self.collection_batch_size = collection_batch_size
        self.candidate_limit = candidate_limit
        self.diligence_model = diligence_model
        self.diligence_limit = diligence_limit
        self.diligence_slots = diligence_slots
        self.diligence_timeout_seconds = diligence_timeout_seconds
        self.minimum_screened_ratio = minimum_screened_ratio
        self.minimum_screened_count = minimum_screened_count
        self.analysis_function = analysis_function
        self._owned_sec = owned_sec

    async def stream(self, spec: RunSpec) -> AsyncIterator[EventDraft]:
        if spec.mode != "automatic_live":
            raise ValueError("AutomaticLiveRuntime requires RunSpec.mode='automatic_live'")
        if spec.universe_size != self.policy.target_size:
            raise ValueError("RunSpec universe_size must equal the automatic policy target_size")
        if spec.agent_budget != LOGICAL_AGENT_LANES:
            raise ValueError("automatic live research requires all 8 logical agent lanes")
        try:
            async for draft in self._stream(spec):
                yield draft
        finally:
            if self._owned_sec is not None:
                await self._owned_sec.close()

    async def _stream(self, spec: RunSpec) -> AsyncIterator[EventDraft]:
        yield stage_event(
            EventKind.STAGE_STARTED,
            Stage.UNIVERSE,
            "Discovering a broad, liquid US equity universe",
            3,
            empty_funnel(
                self.policy,
                self.diligence_model is not None,
                self.collection_batch_size,
            ),
        )
        try:
            manifest = await self.discovery.discover(self.policy)
        except UniverseCoverageError as exc:
            funnel = FunnelProgress(
                exc.manifest,
                analysis_mode(self.diligence_model is not None),
                self.collector.provider_slots,
                0,
                stage="failed",
            )
            yield stage_event(
                EventKind.STAGE_COMPLETED,
                Stage.UNIVERSE,
                "Automatic universe did not meet the controller minimum",
                5,
                funnel.snapshot(),
            )
            raise
        except UniverseDiscoveryError:
            failed = empty_funnel(
                self.policy,
                self.diligence_model is not None,
                self.collection_batch_size,
            )
            failed["stage"] = "failed"
            yield stage_event(
                EventKind.STAGE_COMPLETED,
                Stage.UNIVERSE,
                "Automatic universe discovery failed closed",
                5,
                failed,
            )
            raise

        total = len(manifest.members)
        batches_total = math.ceil(total / self.collection_batch_size)
        progress = FunnelProgress(
            manifest,
            analysis_mode(self.diligence_model is not None),
            self.collector.provider_slots,
            batches_total,
            stage="collection",
        )
        manifest_snapshot = manifest.snapshot(include_members=False)
        yield stage_event(
            EventKind.STAGE_COMPLETED,
            Stage.UNIVERSE,
            f"Selected {total} source-bound issuers for automatic research",
            9,
            progress.snapshot(),
            {"universe_manifest": manifest_snapshot},
        )

        lane_by_ticker = {member.ticker: lane_id(index) for index, member in enumerate(manifest.members)}
        index_by_ticker = {member.ticker: index for index, member in enumerate(manifest.members)}
        for lane_number in range(LOGICAL_AGENT_LANES):
            agent_id = lane_id(lane_number)
            yield EventDraft(
                EventKind.AGENT_REGISTERED,
                f"Registered automatic research lane {lane_number + 1}",
                agent_id=agent_id,
                payload={"role": "Reusable public-equity research lane", "lane": f"lane-{lane_number + 1}"},
            )
            yield EventDraft(
                EventKind.AGENT_STARTED,
                f"Automatic lane {lane_number + 1} admitted",
                agent_id=agent_id,
                payload={"progress": 5, "universe_funnel": progress.snapshot()},
            )

        yield EventDraft(
            EventKind.WORKFLOW_PLANNED,
            f"Planned {total} issuer screens across 8 reusable lanes",
            payload={
                "workflow_id": WORKFLOW_ID,
                "workflow_version": WORKFLOW_VERSION,
                "tasks": scaled_planned_tasks(
                    manifest,
                    lane_by_ticker,
                    include_diligence=self.diligence_model is not None,
                    diligence_limit=self.diligence_limit,
                ),
                "configured_active_slots": spec.active_slots,
                "requested_active_slots": spec.active_slots,
                "effective_active_slots": min(spec.active_slots, LOGICAL_AGENT_LANES),
                "actual_active_slots": None,
                "lane_assignment": "selected_universe_order_modulo_8",
                "collection_batch_size": self.collection_batch_size,
                "candidate_limit": self.candidate_limit,
                "diligence_configured": self.diligence_model is not None,
                "diligence_limit": self.diligence_limit if self.diligence_model is not None else 0,
                "analysis_mode": progress.analysis_mode,
                "universe_manifest": manifest_snapshot,
                "universe_funnel": progress.snapshot(),
            },
        )

        yield stage_event(
            EventKind.STAGE_STARTED,
            Stage.EVIDENCE,
            "Collecting comparable SEC facts and source-bound screener market fields",
            12,
            progress.snapshot(),
        )
        collections: list[LiveCollection] = []
        ready_by_ticker: dict[str, LiveIssuerEvidence] = {}
        collection_errors: dict[str, str] = {}
        async for batch in self.collector.iter_collect_manifest(
            manifest.members,
            identity_retrieved_at=manifest.sec_source.retrieved_at,
            identity_source_url=manifest.sec_source.source_url,
            batch_size=self.collection_batch_size,
        ):
            collections.append(batch)
            for issuer in batch.issuers:
                ready_by_ticker[issuer.issuer.ticker] = issuer
                agent_id = lane_by_ticker[issuer.issuer.ticker]
                for record in (issuer.identity_evidence, *issuer.packet.evidence):
                    yield _evidence_event(agent_id, issuer, record)
            ready_tickers = {issuer.issuer.ticker for issuer in batch.issuers}
            for ticker in batch.requested_tickers:
                if ticker in ready_tickers:
                    continue
                error = failure_reason(ticker, batch.failures)
                collection_errors[ticker] = error
                progress.failed_tickers.add(ticker)
                task_id = screen_task_id(index_by_ticker[ticker], ticker)
                agent_id = lane_by_ticker[ticker]
                yield EventDraft(
                    EventKind.TASK_STARTED,
                    f"Admitted {ticker} evidence screen",
                    agent_id=agent_id,
                    payload={"task_id": task_id},
                )
                yield EventDraft(
                    EventKind.TASK_FAILED,
                    f"Could not establish a trusted SEC packet for {ticker}",
                    agent_id=agent_id,
                    payload={"task_id": task_id, "error": error, "phase": "evidence_collection"},
                )
            progress.batches_completed += 1
            progress.eligible = len(ready_by_ticker)
            progress.observed_peak_provider_requests = max(
                progress.observed_peak_provider_requests,
                batch.observed_peak_provider_requests,
            )
            for agent_id in sorted({lane_by_ticker[ticker] for ticker in batch.requested_tickers}):
                yield EventDraft(
                    EventKind.AGENT_PROGRESS,
                    f"Collected evidence batch {progress.batches_completed}/{batches_total}",
                    agent_id=agent_id,
                    payload={
                        "progress": scaled_progress(progress.batches_completed, batches_total, 10, 45),
                        "universe_funnel": progress.snapshot(),
                    },
                )

        live_snapshot = aggregate_live_snapshot(manifest, collections)
        yield stage_event(
            EventKind.STAGE_COMPLETED,
            Stage.EVIDENCE,
            f"Trusted evidence ready for {len(ready_by_ticker)}/{total} selected issuers",
            47,
            progress.snapshot(),
            {"live_collection": live_snapshot},
        )
        execution = _Execution(
            manifest,
            progress,
            lane_by_ticker,
            index_by_ticker,
            collections,
            ready_by_ticker,
            collection_errors,
            live_snapshot,
        )
        async for draft in self._screen_and_synthesize(spec, execution, manifest_snapshot):
            yield draft

    async def _screen_and_synthesize(
        self,
        spec: RunSpec,
        execution: _Execution,
        manifest_snapshot: Mapping[str, JsonValue],
    ) -> AsyncIterator[EventDraft]:
        manifest = execution.manifest
        progress = execution.progress
        progress.stage = "screening"
        yield stage_event(
            EventKind.STAGE_STARTED,
            Stage.ANALYSIS,
            "Screening every eligible issuer with controller-owned calculations",
            50,
            progress.snapshot(),
        )
        analyses: list[IssuerAnalysis] = []
        analysis_errors: dict[str, str] = {}
        ready = [
            execution.ready_by_ticker[member.ticker]
            for member in manifest.members
            if member.ticker in execution.ready_by_ticker
        ]
        processed = 0
        for batch in _analysis_waves(ready, spec.active_slots, execution.lane_by_ticker):
            for issuer in batch:
                ticker = issuer.issuer.ticker
                task_id = screen_task_id(execution.index_by_ticker[ticker], ticker)
                agent_id = execution.lane_by_ticker[ticker]
                yield EventDraft(
                    EventKind.TASK_STARTED,
                    f"Screening {ticker}",
                    agent_id=agent_id,
                    payload={"task_id": task_id},
                )
                call_id = f"{spec.run_id}:{task_id}:finance.metrics"
                source_ids = list(issuer.packet.source_ids)
                arguments: dict[str, JsonValue] = {
                    "entity_id": issuer.issuer.entity_id,
                    "observation_ids": dict(sorted(issuer.observation_ids.items())),
                    "source_ids": source_ids,
                }
                yield EventDraft(
                    EventKind.TOOL_STARTED,
                    "Running controller-owned finance.metrics",
                    agent_id=agent_id,
                    payload={
                        "task_id": task_id,
                        "call_id": call_id,
                        "tool": "finance.metrics",
                        "tool_name": "finance.metrics",
                        "arguments": arguments,
                        "arguments_hash": canonical_hash(arguments),
                        "source_ids": source_ids,
                    },
                )
            results, peak = await _analyze_batch(
                batch,
                spec.active_slots,
                execution.lane_by_ticker,
                self.analysis_function,
            )
            progress.observed_peak_analysis_tasks = max(progress.observed_peak_analysis_tasks, peak)
            for issuer, result in results:
                ticker = issuer.issuer.ticker
                task_id = screen_task_id(execution.index_by_ticker[ticker], ticker)
                agent_id = execution.lane_by_ticker[ticker]
                if isinstance(result, Exception):
                    error = f"{type(result).__name__}: issuer calculation failed"
                    analysis_errors[ticker] = error
                    progress.failed_tickers.add(ticker)
                    call_id = f"{spec.run_id}:{task_id}:finance.metrics"
                    error_payload: dict[str, JsonValue] = {
                        "code": "issuer_calculation_failed",
                        "message": error,
                    }
                    failure_envelope: dict[str, JsonValue] = {
                        "call_id": call_id,
                        "payload": {"error": error_payload},
                        "source_ids": [],
                        "retryable": False,
                    }
                    yield EventDraft(
                        EventKind.TOOL_FAILED,
                        f"Controller calculation failed for {ticker}",
                        agent_id=agent_id,
                        payload={
                            "task_id": task_id,
                            "call_id": call_id,
                            "tool": "finance.metrics",
                            "tool_name": "finance.metrics",
                            "error": error_payload,
                            "result_envelope": failure_envelope,
                            "result_hash": canonical_hash(failure_envelope),
                        },
                    )
                    yield EventDraft(
                        EventKind.TASK_FAILED,
                        f"Screen failed for {ticker}",
                        agent_id=agent_id,
                        payload={"task_id": task_id, "error": error, "phase": "screening"},
                    )
                    continue
                analyses.append(result)
                call_id = f"{spec.run_id}:{task_id}:finance.metrics"
                result_payload = dict(result.tool_payload)
                source_ids = list(result.source_ids)
                result_envelope: dict[str, JsonValue] = {
                    "call_id": call_id,
                    "payload": result_payload,
                    "source_ids": source_ids,
                    "retryable": False,
                }
                yield EventDraft(
                    EventKind.TOOL_COMPLETED,
                    f"Calculated bound metrics for {ticker}",
                    agent_id=agent_id,
                    payload={
                        "task_id": task_id,
                        "call_id": call_id,
                        "tool": "finance.metrics",
                        "tool_name": "finance.metrics",
                        "result": result_payload,
                        "source_ids": source_ids,
                        "retryable": False,
                        "error": None,
                        "result_envelope": result_envelope,
                        "result_hash": canonical_hash(result_envelope),
                    },
                )
                gaps = _analysis_gaps(result)
                yield EventDraft(
                    EventKind.TASK_COMPLETED,
                    f"Completed automatic screen for {ticker}",
                    agent_id=agent_id,
                    payload=_task_output_payload(
                        task_id,
                        gaps,
                        {"metrics": _json_metrics(result.metrics)},
                    ),
                )
            progress.screened = len(analyses)
            processed += len(batch)
            for agent_id in sorted({execution.lane_by_ticker[issuer.issuer.ticker] for issuer in batch}):
                yield EventDraft(
                    EventKind.AGENT_PROGRESS,
                    f"Screened {processed}/{len(ready)} eligible issuers",
                    agent_id=agent_id,
                    payload={
                        "progress": scaled_progress(processed, max(1, len(ready)), 45, 75),
                        "universe_funnel": progress.snapshot(),
                    },
                )

        required = max(
            self.minimum_screened_count,
            math.ceil(len(manifest.members) * self.minimum_screened_ratio),
        )
        if len(analyses) < required:
            progress.stage = "failed"
            all_errors = execution.collection_errors | analysis_errors
            data_quality_by_entity = {
                analysis.evidence.issuer.entity_id: ("partial" if _analysis_gaps(analysis) else "complete")
                for analysis in analyses
            }
            progress.universe_rows = audit_universe_rows(
                manifest,
                execution.ready_by_ticker,
                progress.failed_tickers,
                all_errors,
                {},
                data_quality_by_entity,
                set(),
                set(),
            )
            failed_funnel = progress.snapshot()
            for lane_number in range(LOGICAL_AGENT_LANES):
                yield EventDraft(
                    EventKind.AGENT_FAILED,
                    f"Automatic research lane {lane_number + 1} stopped at the coverage gate",
                    agent_id=lane_id(lane_number),
                    payload={
                        "error": "minimum_coverage_not_met",
                        "universe_funnel": failed_funnel,
                    },
                )
            yield stage_event(
                EventKind.STAGE_COMPLETED,
                Stage.ANALYSIS,
                f"Automatic screen failed minimum coverage: {len(analyses)}/{required}",
                76,
                progress.snapshot(include_rows=True),
                {
                    "universe_manifest": dict(manifest_snapshot),
                    "live_collection": execution.live_snapshot,
                },
            )
            raise AutomaticCoverageError(
                f"automatic screen produced {len(analyses)} successful issuers; required {required}"
            )

        yield stage_event(
            EventKind.STAGE_COMPLETED,
            Stage.ANALYSIS,
            f"Screened {len(analyses)} issuers with {len(progress.failed_tickers)} isolated failures",
            76,
            progress.snapshot(),
        )
        async for draft in self._synthesize(
            spec,
            execution,
            tuple(analyses),
            execution.collection_errors | analysis_errors,
            manifest_snapshot,
        ):
            yield draft

    async def _synthesize(
        self,
        spec: RunSpec,
        execution: _Execution,
        analyses: Sequence[IssuerAnalysis],
        issuer_errors: Mapping[str, str],
        manifest_snapshot: Mapping[str, JsonValue],
    ) -> AsyncIterator[EventDraft]:
        manifest = execution.manifest
        progress = execution.progress
        progress.stage = "ranking"
        yield EventDraft(
            EventKind.RUN_SYNTHESIZING,
            "Ranking the complete eligible cohort and selecting deeper work",
            payload={"progress": 79, "universe_funnel": progress.snapshot()},
        )
        yield stage_event(
            EventKind.STAGE_STARTED,
            Stage.SYNTHESIS,
            "Applying deterministic cross-universe ranking",
            80,
            progress.snapshot(),
        )
        ranked = _rank_analyses(analyses)
        raw_ranked = ranked["ranked"]
        if not isinstance(raw_ranked, list):
            raise RuntimeError("trusted ranking result is malformed")
        ranked_rows = [row for row in raw_ranked if isinstance(row, dict)]
        shortlist = ranked_rows[: self.candidate_limit]
        analysis_by_entity = {analysis.evidence.issuer.entity_id: analysis for analysis in analyses}
        diligence_by_entity: dict[str, Mapping[str, JsonValue]] = {}
        diligence_errors: dict[str, str] = {}

        if self.diligence_model is not None:
            progress.stage = "deep_review"
            jobs: list[DiligenceJob] = []
            for screen_rank, row in enumerate(shortlist[: self.diligence_limit], 1):
                analysis = analysis_by_entity[str(row["id"])]
                ticker = analysis.evidence.issuer.ticker
                task_id = diligence_lane_task_id(screen_rank - 1)
                agent_id = lane_id(screen_rank - 1)
                request = _diligence_request(spec, analysis, screen_rank, task_id, agent_id)
                job = DiligenceJob(ticker, task_id, agent_id, request)
                jobs.append(job)
                yield EventDraft(
                    EventKind.TASK_STARTED,
                    f"Starting bounded model diligence for {ticker}",
                    agent_id=agent_id,
                    payload={"task_id": task_id},
                )
                exact_request = request.to_dict()
                yield EventDraft(
                    EventKind.MODEL_TURN_STARTED,
                    f"Requested one diligence action for {ticker}",
                    agent_id=agent_id,
                    payload={
                        "task_id": task_id,
                        "turn": 1,
                        "request_id": request.request_id,
                        "session_id": request.session_id,
                        "request": exact_request,
                        "request_hash": canonical_hash(exact_request),
                    },
                )

            outcomes, _ = await BoundedDiligenceRunner(
                self.diligence_model,
                slots=self.diligence_slots,
                timeout_seconds=self.diligence_timeout_seconds,
            ).run(jobs)
            for outcome in outcomes:
                job = outcome.job
                trace = dict(outcome.trace)
                yield EventDraft(
                    EventKind.MODEL_TURN_COMPLETED,
                    f"Received bounded diligence action for {job.ticker}",
                    agent_id=job.agent_id,
                    payload={
                        "task_id": job.task_id,
                        "turn": 1,
                        "request_id": str(trace["request_id"]),
                        "session_id": job.request.session_id,
                        "output": trace["output_text"],
                        "output_bytes": trace["output_bytes"],
                        "output_hash": trace["output_hash"],
                        "output_truncated": False,
                        "trace": trace,
                        "trace_hash": canonical_hash(trace),
                    },
                )
                if outcome.output is None:
                    code = outcome.error_code or "invalid_diligence_output"
                    error = outcome.error or "model diligence failed closed"
                    diligence_errors[job.ticker] = error
                    yield EventDraft(
                        EventKind.ACTION_REJECTED,
                        f"Rejected diligence action for {job.ticker}",
                        agent_id=job.agent_id,
                        payload={
                            "task_id": job.task_id,
                            "code": code,
                            "error": error,
                            "repair_allowed": False,
                        },
                    )
                    yield EventDraft(
                        EventKind.TASK_FAILED,
                        f"Model diligence unavailable for {job.ticker}",
                        agent_id=job.agent_id,
                        payload={"task_id": job.task_id, "error": error},
                    )
                    continue
                entity_id = f"ticker:{job.ticker}"
                diligence_by_entity[entity_id] = outcome.output
                yield EventDraft(
                    EventKind.TASK_COMPLETED,
                    f"Validated model diligence for {job.ticker}",
                    agent_id=job.agent_id,
                    payload={
                        "task_id": job.task_id,
                        "partial": False,
                        "output": dict(outcome.output),
                        "source_ids": list(outcome.output["source_ids"]),
                    },
                )
            progress.deep_reviewed = len(diligence_by_entity)
            for agent_id in sorted({job.agent_id for job in jobs}):
                yield EventDraft(
                    EventKind.AGENT_PROGRESS,
                    f"Validated {progress.deep_reviewed}/{len(jobs)} deep reviews",
                    agent_id=agent_id,
                    payload={"progress": 88, "universe_funnel": progress.snapshot()},
                )

        validator_task = validator_task_id()
        yield EventDraft(
            EventKind.TASK_STARTED,
            "Validating controller-owned cohort ranking",
            agent_id=lane_id(0),
            payload={"task_id": validator_task},
        )
        yield EventDraft(
            EventKind.TASK_COMPLETED,
            "Trusted automatic-universe ranking complete",
            agent_id=lane_id(0),
            payload=_task_output_payload(
                validator_task,
                ["one or more selected issuers failed"] if progress.failed_tickers else [],
                ranked,
            ),
        )

        progress.surfaced = len(shortlist)
        for screen_rank, row in enumerate(shortlist, 1):
            entity_id = str(row["id"])
            analysis = analysis_by_entity[entity_id]
            payload = _candidate_payload(analysis, row, screen_rank, len(analyses))
            payload["universe_rank"] = next(
                member.rank for member in manifest.members if member.ticker == analysis.evidence.issuer.ticker
            )
            payload["screen_rank"] = screen_rank
            payload["analysis_mode"] = progress.analysis_mode
            payload["diligence"] = dict(diligence_by_entity[entity_id]) if entity_id in diligence_by_entity else None
            payload["universe_funnel"] = progress.snapshot()
            diligence_error = diligence_errors.get(analysis.evidence.issuer.ticker)
            if diligence_error is not None:
                raw_gaps = payload.get("evidence_gaps")
                evidence_gaps = list(raw_gaps) if isinstance(raw_gaps, list) else []
                evidence_gaps.append("Optional model diligence was rejected; controller ranking is unchanged.")
                payload["evidence_gaps"] = evidence_gaps
                payload["diligence_status"] = "rejected"
            yield EventDraft(
                EventKind.CANDIDATE_UPDATED,
                f"Surfaced {analysis.evidence.issuer.ticker} at screen rank {screen_rank}",
                payload=payload,
            )

        score_by_entity = {str(row["id"]): row.get("composite_score") for row in ranked_rows}
        data_quality_by_entity = {
            entity_id: ("partial" if _analysis_gaps(analysis) else "complete")
            for entity_id, analysis in analysis_by_entity.items()
        }
        progress.stage = "complete"
        progress.universe_rows = audit_universe_rows(
            manifest,
            execution.ready_by_ticker,
            progress.failed_tickers,
            issuer_errors,
            score_by_entity,
            data_quality_by_entity,
            {str(row["id"]) for row in shortlist},
            set(diligence_by_entity),
        )
        final_funnel = progress.snapshot()
        terminal_funnel = progress.snapshot(include_rows=True)
        for lane_number in range(LOGICAL_AGENT_LANES):
            yield EventDraft(
                EventKind.AGENT_COMPLETED,
                f"Automatic research lane {lane_number + 1} complete",
                agent_id=lane_id(lane_number),
                payload={"universe_funnel": final_funnel},
            )
        yield stage_event(
            EventKind.STAGE_COMPLETED,
            Stage.SYNTHESIS,
            "Automatic research priorities are ready for human review",
            96,
            final_funnel,
            {
                "universe_manifest": dict(manifest_snapshot),
                "live_collection": execution.live_snapshot,
            },
        )
        yield EventDraft(
            EventKind.WORKFLOW_COMPLETED,
            (
                f"Automatic research surfaced {progress.surfaced} priorities "
                f"from {len(manifest.members)} selected issuers"
            ),
            payload={
                "workflow_id": WORKFLOW_ID,
                "workflow_version": WORKFLOW_VERSION,
                "analysis_mode": progress.analysis_mode,
                "observed_peak_active_tasks": progress.observed_peak_analysis_tasks,
                "diligence_configured": self.diligence_model is not None,
                "diligence_failures": len(diligence_errors),
                "universe_manifest": dict(manifest_snapshot),
                "universe_funnel": terminal_funnel,
                "live_collection": execution.live_snapshot,
            },
        )


def _analysis_waves(
    issuers: Sequence[LiveIssuerEvidence],
    active_slots: int,
    lane_by_ticker: Mapping[str, str],
) -> tuple[tuple[LiveIssuerEvidence, ...], ...]:
    limit = min(active_slots, LOGICAL_AGENT_LANES)
    if limit < 1:
        raise ValueError("active_slots must be positive")
    pending = list(issuers)
    waves: list[tuple[LiveIssuerEvidence, ...]] = []
    while pending:
        wave: list[LiveIssuerEvidence] = []
        deferred: list[LiveIssuerEvidence] = []
        used_lanes: set[str] = set()
        for issuer in pending:
            lane = lane_by_ticker[issuer.issuer.ticker]
            if len(wave) < limit and lane not in used_lanes:
                wave.append(issuer)
                used_lanes.add(lane)
            else:
                deferred.append(issuer)
        if not wave:
            raise RuntimeError("analysis wave scheduler could not make progress")
        waves.append(tuple(wave))
        pending = deferred
    return tuple(waves)


async def _analyze_batch(
    issuers: Sequence[LiveIssuerEvidence],
    active_slots: int,
    lane_by_ticker: Mapping[str, str],
    analyzer: Callable[[LiveIssuerEvidence], IssuerAnalysis],
) -> tuple[tuple[tuple[LiveIssuerEvidence, IssuerAnalysis | Exception], ...], int]:
    semaphore = asyncio.Semaphore(min(active_slots, LOGICAL_AGENT_LANES, len(issuers)))
    lane_locks = {lane: asyncio.Lock() for lane in set(lane_by_ticker.values())}
    counter_lock = asyncio.Lock()
    active = 0
    peak = 0

    async def analyze(
        evidence: LiveIssuerEvidence,
    ) -> tuple[LiveIssuerEvidence, IssuerAnalysis | Exception]:
        nonlocal active, peak
        lane_lock = lane_locks[lane_by_ticker[evidence.issuer.ticker]]
        async with lane_lock, semaphore:
            async with counter_lock:
                active += 1
                peak = max(peak, active)
            try:
                try:
                    result = await asyncio.to_thread(analyzer, evidence)
                except Exception as exc:
                    result = exc
                return evidence, result
            finally:
                async with counter_lock:
                    active -= 1

    return tuple(await asyncio.gather(*(analyze(issuer) for issuer in issuers))), peak


def _diligence_request(
    spec: RunSpec,
    analysis: IssuerAnalysis,
    screen_rank: int,
    task_id: str,
    agent_id: str,
) -> ActionModelRequest:
    evidence = analysis.evidence
    source_ids = tuple(dict.fromkeys((evidence.identity_evidence.evidence_id, *analysis.source_ids)))
    transcript: tuple[Mapping[str, JsonValue], ...] = (
        {
            "role": "system",
            "content": {
                "mandate": (
                    "Produce one bounded diligence annotation for a controller-ranked research priority. "
                    "Do not change its score or rank and cite only allowed source IDs."
                ),
                "ticker": evidence.issuer.ticker,
                "company": evidence.issuer.company,
                "screen_rank": screen_rank,
                "controller_metrics": _json_metrics(analysis.metrics),
                "output_contract": DILIGENCE_OUTPUT_SCHEMA,
            },
        },
    )
    return ActionModelRequest(
        run_id=spec.run_id,
        workflow_id=WORKFLOW_ID,
        task_id=task_id,
        agent_id=agent_id,
        turn=1,
        request_id=f"{spec.run_id}:{task_id}:t1",
        session_id=f"{spec.run_id}:{agent_id}",
        prompt_key="automatic_equity_diligence_v1",
        transcript=transcript,
        tool_contracts=(),
        output_schema=DILIGENCE_OUTPUT_SCHEMA,
        allowed_source_ids=source_ids,
        evidence_packet={
            "identity": evidence.identity_evidence.to_dict(),
            "financials": evidence.packet.to_dict(),
        },
        max_new_tokens=800,
        max_action_bytes=8_192,
        sampling={"temperature": 0.0, "seed": 0},
    )


def build_automatic_live_runtime(
    *,
    policy: UniversePolicy | None = None,
    cache_root: Path = Path("artifacts/live-cache"),
    cache_max_age: timedelta = timedelta(hours=6),
    sec_map_max_age: timedelta = timedelta(days=7),
    provider_slots: int = 8,
    provider_timeout_seconds: float = 30.0,
    collection_batch_size: int = 25,
    candidate_limit: int = 25,
    diligence_model: ActionModel | None = None,
    diligence_limit: int = 8,
    diligence_slots: int = 4,
    diligence_timeout_seconds: float = 60.0,
    minimum_screened_ratio: float = 0.70,
    minimum_screened_count: int = 100,
    analysis_function: Callable[[LiveIssuerEvidence], IssuerAnalysis] = _analyze_one,
    sec: Any | None = None,
    market: Any | None = None,
    now: Callable[[], datetime] | None = None,
    env_file: Path = Path(".env"),
) -> AutomaticLiveRuntime:
    """Build a no-ticker automatic runtime with shared, source-bound providers."""

    selected_policy = policy or DEFAULT_AUTOMATIC_POLICY
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
    cache = ContentAddressedJsonCache(cache_root)
    discovery = AutomaticUniverseDiscovery(
        sec,
        market,
        cache,
        cache_max_age=cache_max_age,
        sec_map_max_age=sec_map_max_age,
        provider_timeout_seconds=provider_timeout_seconds,
        now=now,
    )
    collector = LiveDataCollector(
        sec,
        market,
        cache,
        cache_max_age=cache_max_age,
        ticker_map_max_age=sec_map_max_age,
        provider_slots=provider_slots,
        provider_timeout_seconds=provider_timeout_seconds,
        now=now,
    )
    return AutomaticLiveRuntime(
        discovery,
        collector,
        policy=selected_policy,
        collection_batch_size=collection_batch_size,
        candidate_limit=candidate_limit,
        diligence_model=diligence_model,
        diligence_limit=diligence_limit,
        diligence_slots=diligence_slots,
        diligence_timeout_seconds=diligence_timeout_seconds,
        minimum_screened_ratio=minimum_screened_ratio,
        minimum_screened_count=minimum_screened_count,
        analysis_function=analysis_function,
        owned_sec=owned_sec,
    )


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
