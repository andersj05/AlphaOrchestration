"""Release-blocking integrity checks for automatic 300-issuer research."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from pathlib import Path
from typing import cast

from alpha_orchestration._automatic_harness_fixture import (
    PRIMARY_CANDIDATE_LIMIT,
    PRIMARY_UNIVERSE_SIZE,
    AutomaticHarnessRun,
    build_fixture_manifest,
    build_fixture_policy,
    execute_fixture,
)
from alpha_orchestration.automatic_projection import LOGICAL_AGENT_LANES
from alpha_orchestration.domain import EventKind, JsonValue, RunEvent, RunStatus, TaskStatus
from alpha_orchestration.live_runtime import _rank_analyses

EXPECTED_PRIMARY_ARTIFACT_HASH = "8d5dc83af78bca7c407c01cd233dabe21e8f288d61dbd3bd9ce95152286148bf"


async def run_harness(
    journal_path: Path,
    *,
    cache_root: Path,
) -> dict[str, JsonValue]:
    """Execute and self-check the hermetic 300-issuer fixture."""

    run = await execute_fixture(journal_path, cache_root=cache_root)
    summary = validate_primary_run(run)
    summary["journal"] = str(journal_path.resolve())
    return summary


def validate_primary_run(run: AutomaticHarnessRun) -> dict[str, JsonValue]:
    """Assert scale, bounds, replay, identity, lineage, and terminal ordering."""

    if len(run.manifest.members) != PRIMARY_UNIVERSE_SIZE:
        raise RuntimeError("primary harness did not select exactly 300 issuers")
    if run.state.status is not RunStatus.COMPLETE or run.restored != run.state:
        raise RuntimeError("automatic harness execution/replay state is not equivalent")
    if run.discovery.calls != 1 or run.sec.mapping_calls:
        raise RuntimeError("automatic collection did not preserve manifest-owned identity")
    if len(run.sec.fact_calls) != PRIMARY_UNIVERSE_SIZE:
        raise RuntimeError("automatic collection did not request every selected SEC issuer")
    if len(set(run.sec.fact_calls)) != PRIMARY_UNIVERSE_SIZE:
        raise RuntimeError("automatic collection requested a selected issuer more than once")
    if run.market.snapshot_calls or run.market.screen_calls:
        raise RuntimeError("automatic collection made a per-ticker market or discovery call")
    if any(
        event.kind in {EventKind.MODEL_TURN_STARTED, EventKind.MODEL_TURN_COMPLETED}
        for event in run.events
    ):
        raise RuntimeError("default automatic harness invoked a model without injection")

    registered = [event for event in run.events if event.kind is EventKind.AGENT_REGISTERED]
    if len(registered) != LOGICAL_AGENT_LANES:
        raise RuntimeError("automatic harness did not register exactly eight lanes")
    if len({event.agent_id for event in registered}) != LOGICAL_AGENT_LANES:
        raise RuntimeError("automatic harness reused an invalid lane identity")
    screen_tasks = [task for task in run.state.tasks.values() if task.task_id.startswith("screen-")]
    if len(screen_tasks) != PRIMARY_UNIVERSE_SIZE:
        raise RuntimeError("automatic harness did not retain 300 screen lifecycles")
    if any(task.status is not TaskStatus.COMPLETE for task in run.state.tasks.values()):
        raise RuntimeError("primary automatic harness left a non-complete workflow task")

    workflow = _only_event(run.events, EventKind.WORKFLOW_COMPLETED)
    funnel = _object(workflow.payload.get("universe_funnel"), "terminal universe_funnel")
    rows = _array(funnel.get("universe_rows"), "terminal universe_rows")
    for key, expected in {
        "selected": PRIMARY_UNIVERSE_SIZE,
        "discovered": PRIMARY_UNIVERSE_SIZE,
        "eligible": PRIMARY_UNIVERSE_SIZE,
        "screened": PRIMARY_UNIVERSE_SIZE,
        "failed": 0,
    }.items():
        if funnel.get(key) != expected:
            raise RuntimeError(f"terminal funnel {key} is {funnel.get(key)!r}; expected {expected}")
    if len(rows) != PRIMARY_UNIVERSE_SIZE:
        raise RuntimeError("terminal universe_rows does not retain all 300 issuers")
    row_events = [
        event
        for event in run.events
        if isinstance(event.payload.get("universe_funnel"), dict)
        and "universe_rows" in event.payload["universe_funnel"]
    ]
    if row_events != [workflow]:
        raise RuntimeError("full universe_rows must be persisted once, on workflow completion")
    if [row.get("ticker") for row in map(_row, rows)] != list(run.manifest.tickers):
        raise RuntimeError("terminal universe_rows changed controller-owned universe order")

    runtime_peak = _integer(workflow.payload.get("observed_peak_active_tasks"), "analysis peak")
    funnel_peak = _integer(funnel.get("observed_peak_analysis_tasks"), "funnel analysis peak")
    provider_peak = _integer(funnel.get("observed_peak_provider_requests"), "provider peak")
    if runtime_peak != funnel_peak or runtime_peak != run.analyzer.peak_active_calls:
        raise RuntimeError("runtime and fixture analysis peaks disagree")
    if provider_peak != run.sec.peak_active_calls:
        raise RuntimeError("runtime and fixture provider peaks disagree")
    if runtime_peak != LOGICAL_AGENT_LANES or provider_peak != LOGICAL_AGENT_LANES:
        raise RuntimeError("controlled barriers did not prove genuine eight-way overlap")
    if max(runtime_peak, provider_peak) > LOGICAL_AGENT_LANES:
        raise RuntimeError("automatic harness exceeded the eight-slot limit")

    journal_peak = verify_screen_intervals(run.events)
    if journal_peak != LOGICAL_AGENT_LANES:
        raise RuntimeError("journal task intervals did not prove eight bounded screen admissions")
    candidate_events = [event for event in run.events if event.kind is EventKind.CANDIDATE_UPDATED]
    if len(candidate_events) != PRIMARY_CANDIDATE_LIMIT:
        raise RuntimeError("automatic harness projected an unexpected candidate count")
    expected_ids = _expected_ids(run)
    actual_ids = tuple(str(event.payload.get("candidate_id")) for event in candidate_events)
    if actual_ids != expected_ids or tuple(run.state.candidates) != expected_ids:
        raise RuntimeError("candidate identity or ordering escaped controller-owned ranking")
    if any(
        event.payload.get("screen_rank") != rank
        for rank, event in enumerate(candidate_events, start=1)
    ):
        raise RuntimeError("candidate screen ranks are not contiguous")

    evidence = {
        str(event.payload["evidence_id"]): event.payload
        for event in run.events
        if event.kind is EventKind.EVIDENCE_ADDED
    }
    member_by_ticker = {member.ticker: member for member in run.manifest.members}
    for ticker, analysis in run.analyzer.results.items():
        member = member_by_ticker[ticker]
        revenue = analysis.evidence.observations_by_name["revenue"]
        for name in ("market_cap", "share_price"):
            observation = analysis.evidence.observations_by_name.get(name)
            if observation is None or observation.unit.symbol != revenue.unit.symbol:
                raise RuntimeError(f"{ticker} mixed or dropped a same-currency market value")
            if observation.unit.symbol != member.currency:
                raise RuntimeError(f"{ticker} market currency escaped manifest binding")
            for source_id in observation.evidence_ids:
                source = evidence.get(source_id)
                if source is None or source.get("content_hash") != member.market_content_hash:
                    raise RuntimeError(f"{ticker} market value lost content-hash binding")

    for event in candidate_events:
        ticker = str(event.payload["ticker"])
        member = member_by_ticker[ticker]
        source_ids = _strings(event.payload.get("evidence_ids"), "candidate evidence_ids")
        if any(source_id not in evidence for source_id in source_ids):
            raise RuntimeError(f"{ticker} candidate cites unavailable evidence")
        for value in _array(event.payload.get("financials"), "candidate financials"):
            financial = _object(value, "candidate financial")
            cited = _strings(financial.get("source_ids"), "financial source_ids")
            if not set(cited).issubset(source_ids):
                raise RuntimeError(f"{ticker} financial citations escape candidate evidence")
            if financial.get("metric") == "revenue" and financial.get(
                "unit"
            ) != f"{member.currency} millions":
                raise RuntimeError(f"{ticker} revenue currency is not source-bound")
            if financial.get("metric") == "share_price" and financial.get("unit") != member.currency:
                raise RuntimeError(f"{ticker} share-price currency is not source-bound")

    _verify_terminal_order(run.events)
    artifact = results_artifact(run)
    artifact_hash = _canonical_hash(artifact)
    if EXPECTED_PRIMARY_ARTIFACT_HASH and artifact_hash != EXPECTED_PRIMARY_ARTIFACT_HASH:
        raise RuntimeError(
            f"automatic results artifact changed: {artifact_hash} "
            f"!= {EXPECTED_PRIMARY_ARTIFACT_HASH}"
        )
    return {
        "ok": True,
        "fixture": "synthetic SEC-shaped automatic-universe acceptance data",
        "run_id": run.restored.spec.run_id,
        "status": run.restored.status.value,
        "events": len(run.events),
        "selected": PRIMARY_UNIVERSE_SIZE,
        "eligible": PRIMARY_UNIVERSE_SIZE,
        "screened": PRIMARY_UNIVERSE_SIZE,
        "universe_rows": len(rows),
        "candidates": len(candidate_events),
        "registered_lanes": len(registered),
        "configured_active_slots": LOGICAL_AGENT_LANES,
        "observed_peak_analysis_tasks": runtime_peak,
        "observed_peak_provider_requests": provider_peak,
        "observed_peak_journal_screen_tasks": journal_peak,
        "market_snapshot_calls": 0,
        "controller_ranked_ids": list(expected_ids),
        "source_currency_binding_verified": True,
        "terminal_order_verified": True,
        "replay_equivalent": True,
        "artifact_hash": artifact_hash,
    }


def terminal_funnel(events: Sequence[RunEvent]) -> dict[str, JsonValue]:
    workflow = _only_event(events, EventKind.WORKFLOW_COMPLETED)
    return _object(workflow.payload.get("universe_funnel"), "terminal universe_funnel")


def verify_screen_intervals(events: Sequence[RunEvent]) -> int:
    """Reconstruct bounded screen and tool lifecycles from journal order."""

    active: dict[str, str] = {}
    active_by_agent: dict[str, str] = {}
    open_tools: set[str] = set()
    seen: set[str] = set()
    peak = 0
    task_terminal = {
        EventKind.TASK_COMPLETED,
        EventKind.TASK_FAILED,
        EventKind.TASK_SKIPPED,
    }
    tool_terminal = {
        EventKind.TOOL_COMPLETED,
        EventKind.TOOL_FAILED,
        EventKind.TOOL_REJECTED,
    }
    for event in events:
        task_value = event.payload.get("task_id")
        if not isinstance(task_value, str) or not task_value.startswith("screen-"):
            continue
        task_id = task_value
        agent_id = event.agent_id
        if not isinstance(agent_id, str) or not agent_id:
            raise RuntimeError(f"screen task {task_id} event is missing its lane identity")
        if event.kind is EventKind.TASK_STARTED:
            if task_id in seen or task_id in active:
                raise RuntimeError(f"screen task {task_id} started more than once")
            if agent_id in active_by_agent:
                raise RuntimeError(
                    f"lane {agent_id} overlapped {active_by_agent[agent_id]} and {task_id}"
                )
            seen.add(task_id)
            active[task_id] = agent_id
            active_by_agent[agent_id] = task_id
            peak = max(peak, len(active))
            if peak > LOGICAL_AGENT_LANES:
                raise RuntimeError("journal admitted more than eight screen tasks")
            continue
        if event.kind is EventKind.TOOL_STARTED:
            if active.get(task_id) != agent_id or task_id in open_tools:
                raise RuntimeError(f"tool start escaped screen task {task_id} lifecycle")
            open_tools.add(task_id)
            continue
        if event.kind in tool_terminal:
            if active.get(task_id) != agent_id or task_id not in open_tools:
                raise RuntimeError(f"tool terminal escaped screen task {task_id} lifecycle")
            open_tools.remove(task_id)
            continue
        if event.kind in task_terminal:
            if active.get(task_id) != agent_id:
                raise RuntimeError(f"screen task {task_id} terminated outside its lifecycle")
            if task_id in open_tools:
                raise RuntimeError(f"screen task {task_id} terminated with an open tool")
            del active[task_id]
            del active_by_agent[agent_id]
    if active or active_by_agent or open_tools:
        raise RuntimeError("journal ended with an open screen or tool lifecycle")
    return peak


def results_artifact(run: AutomaticHarnessRun) -> dict[str, JsonValue]:
    workflow = _only_event(run.events, EventKind.WORKFLOW_COMPLETED)
    funnel = _object(workflow.payload.get("universe_funnel"), "terminal universe_funnel")
    fields = (
        "candidate_id",
        "ticker",
        "company",
        "bucket",
        "priority_score",
        "screen_rank",
        "universe_rank",
        "financials",
        "evidence_ids",
        "as_of",
        "source_mode",
    )
    candidates = [
        {key: event.payload.get(key) for key in fields}
        for event in run.events
        if event.kind is EventKind.CANDIDATE_UPDATED
    ]
    return {
        "schema_version": 1,
        "manifest_content_hash": run.manifest.content_hash,
        "policy_hash": run.manifest.policy.policy_hash,
        "funnel": _strict_json(funnel),
        "candidates": _strict_json(candidates),
        "workflow_id": workflow.payload.get("workflow_id"),
        "workflow_version": workflow.payload.get("workflow_version"),
        "analysis_mode": workflow.payload.get("analysis_mode"),
    }


def _expected_ids(run: AutomaticHarnessRun) -> tuple[str, ...]:
    analyses = tuple(
        run.analyzer.results[member.ticker]
        for member in run.manifest.members
        if member.ticker in run.analyzer.results
    )
    ranked = _rank_analyses(analyses)
    rows = _array(ranked.get("ranked"), "trusted ranked rows")
    return tuple(str(_object(row, "trusted ranked row")["id"]) for row in rows[:PRIMARY_CANDIDATE_LIMIT])


def _verify_terminal_order(events: Sequence[RunEvent]) -> None:
    candidates = [event.sequence for event in events if event.kind is EventKind.CANDIDATE_UPDATED]
    agents = [event.sequence for event in events if event.kind is EventKind.AGENT_COMPLETED]
    workflow = _only_event(events, EventKind.WORKFLOW_COMPLETED)
    synth = [
        event
        for event in events
        if event.kind is EventKind.STAGE_COMPLETED
        and event.payload.get("stage") == "synthesis"
    ]
    if not candidates or len(agents) != LOGICAL_AGENT_LANES or len(synth) != 1:
        raise RuntimeError("terminal projection events are incomplete")
    if not (
        max(candidates)
        < min(agents)
        <= max(agents)
        < synth[0].sequence
        < workflow.sequence
        < events[-1].sequence
    ):
        raise RuntimeError("candidate, lane, workflow, stage, and run order regressed")
    if workflow.sequence + 1 != events[-1].sequence:
        raise RuntimeError("workflow_completed must be the last runtime draft")
    if events[-1].kind is not EventKind.RUN_COMPLETED:
        raise RuntimeError("successful automatic journal does not end with run_completed")


def _only_event(events: Sequence[RunEvent], kind: EventKind) -> RunEvent:
    matches = [event for event in events if event.kind is kind]
    if len(matches) != 1:
        raise RuntimeError(f"expected one {kind.value} event; observed {len(matches)}")
    return matches[0]


def _row(value: JsonValue) -> dict[str, JsonValue]:
    return _object(value, "universe row")


def _object(value: object, label: str) -> dict[str, JsonValue]:
    if not isinstance(value, dict):
        raise RuntimeError(f"{label} must be an object")
    return cast(dict[str, JsonValue], value)


def _array(value: object, label: str) -> list[JsonValue]:
    if not isinstance(value, list):
        raise RuntimeError(f"{label} must be an array")
    return cast(list[JsonValue], value)


def _strings(value: object, label: str) -> tuple[str, ...]:
    values = _array(value, label)
    if any(not isinstance(item, str) or not item for item in values):
        raise RuntimeError(f"{label} must contain non-empty strings")
    return tuple(cast(str, item) for item in values)


def _integer(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f"{label} must be an integer")
    return value


def _strict_json(value: object) -> JsonValue:
    return cast(
        JsonValue,
        json.loads(json.dumps(value, allow_nan=False, sort_keys=True, separators=(",", ":"))),
    )


def _canonical_hash(value: object) -> str:
    encoded = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "EXPECTED_PRIMARY_ARTIFACT_HASH",
    "build_fixture_manifest",
    "build_fixture_policy",
    "execute_fixture",
    "results_artifact",
    "run_harness",
    "terminal_funnel",
    "validate_primary_run",
    "verify_screen_intervals",
]
