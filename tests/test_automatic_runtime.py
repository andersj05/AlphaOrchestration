from __future__ import annotations

import asyncio
import json
from dataclasses import replace

from alpha_orchestration._automatic_harness_fixture import (
    FIXTURE_NOW,
    BarrierAnalyzer,
    BarrierSecProvider,
    FixtureDiscovery,
    ForbiddenMarketProvider,
    build_fixture_manifest,
    build_fixture_policy,
    execute_fixture,
)
from alpha_orchestration.automatic_harness import terminal_funnel, verify_screen_intervals
from alpha_orchestration.automatic_projection import (
    AUTOMATIC_ANALYSIS_MODE,
    LOGICAL_AGENT_LANES,
    FunnelProgress,
)
from alpha_orchestration.automatic_runtime import AutomaticLiveRuntime
from alpha_orchestration.controller import RunController
from alpha_orchestration.data.cache import ContentAddressedJsonCache
from alpha_orchestration.data.live import LiveDataCollector
from alpha_orchestration.domain import AgentStatus, EventKind, RunSpec, RunStatus
from alpha_orchestration.journal import JsonlJournal, load_events, replay
from alpha_orchestration.ports import ActionModelRequest, ActionModelResult


def _funnel_events_with_rows(events) -> list:
    return [
        event
        for event in events
        if isinstance(event.payload.get("universe_funnel"), dict)
        and "universe_rows" in event.payload["universe_funnel"]
    ]


def test_lane_gap_scheduling_is_bounded_and_non_surfaced_rows_remain_screened(tmp_path) -> None:
    failures = ("U0002", "U0017")
    run = asyncio.run(
        execute_fixture(
            tmp_path / "lane-gaps.jsonl",
            cache_root=tmp_path / "lane-gaps-cache",
            target_size=100,
            fact_failures=failures,
        )
    )

    assert run.state.status is RunStatus.COMPLETE
    assert run.restored == run.state
    assert verify_screen_intervals(run.events) == LOGICAL_AGENT_LANES
    assert all(agent.status is AgentStatus.COMPLETE for agent in run.state.agents.values())
    assert not any(event.kind in {EventKind.MODEL_TURN_STARTED, EventKind.MODEL_TURN_COMPLETED} for event in run.events)

    workflow = next(event for event in run.events if event.kind is EventKind.WORKFLOW_COMPLETED)
    synthesis = next(
        event
        for event in run.events
        if event.kind is EventKind.STAGE_COMPLETED and event.payload.get("stage") == "synthesis"
    )
    assert synthesis.sequence < workflow.sequence
    assert workflow.sequence + 1 == run.events[-1].sequence
    assert len(_funnel_events_with_rows(run.events)) == 1

    rows = terminal_funnel(run.events)["universe_rows"]
    assert isinstance(rows, list)
    failed_rows = [row for row in rows if row["status"] == "failed"]
    assert {row["ticker"] for row in failed_rows} == set(failures)
    non_surfaced = next(row for row in rows if not row["surfaced"] and row["status"] != "failed")
    assert non_surfaced["status"] == "screened"


def test_minimum_coverage_failure_terminalizes_all_lanes_and_persists_rows_once(tmp_path) -> None:
    failures = tuple(f"U{rank:04d}" for rank in range(90, 101))
    run = asyncio.run(
        execute_fixture(
            tmp_path / "coverage.jsonl",
            cache_root=tmp_path / "coverage-cache",
            target_size=100,
            fact_failures=failures,
        )
    )

    assert run.state.status is RunStatus.FAILED
    assert run.restored == run.state
    assert verify_screen_intervals(run.events) == LOGICAL_AGENT_LANES
    assert len(run.state.agents) == LOGICAL_AGENT_LANES
    assert all(agent.status is AgentStatus.FAILED for agent in run.state.agents.values())
    lane_terminals = [event for event in run.events if event.kind is EventKind.AGENT_FAILED]
    assert len(lane_terminals) == LOGICAL_AGENT_LANES
    assert len({event.agent_id for event in lane_terminals}) == LOGICAL_AGENT_LANES

    row_events = _funnel_events_with_rows(run.events)
    assert len(row_events) == 1
    assert row_events[0].kind is EventKind.STAGE_COMPLETED
    assert row_events[0].payload["stage"] == "analysis"
    assert run.events[-1].kind is EventKind.RUN_FAILED


class _MixedDiligenceModel:
    def __init__(self) -> None:
        self.requests: list[ActionModelRequest] = []

    async def complete(self, request: ActionModelRequest) -> ActionModelResult:
        position = len(self.requests)
        self.requests.append(request)
        await asyncio.sleep(0)
        source_id = "source-outside-controller-allowlist" if position == 0 else request.allowed_source_ids[0]
        output = json.dumps(
            {
                "kind": "final",
                "payload": {
                    "summary": "Bounded fixture diligence annotation.",
                    "risks": ["Confirm durability in the next filing."],
                    "questions": ["Which disclosed KPI would falsify the screen?"],
                    "source_ids": [source_id],
                },
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return ActionModelResult(
            request_id=request.request_id,
            output_text=output,
            prompt_ids=(1, 2),
            output_ids=(3, 4),
            finish_reason="stop",
            telemetry={"fixture": True},
            model_fingerprint="fixture-model-v1",
            tokenizer_fingerprint="fixture-tokenizer-v1",
        )


def test_optional_diligence_rejection_is_isolated_and_cannot_change_rank(tmp_path) -> None:
    policy = build_fixture_policy(100)
    manifest = build_fixture_manifest(policy)
    discovery = FixtureDiscovery(manifest)
    sec = BarrierSecProvider(manifest)
    market = ForbiddenMarketProvider()
    collector = LiveDataCollector(
        sec,
        market,
        ContentAddressedJsonCache(tmp_path / "diligence-cache"),
        provider_slots=LOGICAL_AGENT_LANES,
        provider_timeout_seconds=10,
        now=lambda: FIXTURE_NOW,
    )
    analyzer = BarrierAnalyzer()
    model = _MixedDiligenceModel()
    runtime = AutomaticLiveRuntime(
        discovery,
        collector,
        policy=policy,
        collection_batch_size=25,
        candidate_limit=25,
        diligence_model=model,
        diligence_limit=LOGICAL_AGENT_LANES,
        diligence_slots=4,
        minimum_screened_ratio=0.70,
        minimum_screened_count=90,
        analysis_function=analyzer,
    )
    spec = RunSpec(
        sector="Optional diligence fixture",
        universe_size=100,
        agent_budget=LOGICAL_AGENT_LANES,
        active_slots=LOGICAL_AGENT_LANES,
        mode="automatic_live",
        run_id="run-automatic-diligence-isolation",
    )
    journal_path = tmp_path / "diligence.jsonl"
    state = asyncio.run(RunController(spec, runtime, JsonlJournal(journal_path)).run())
    events = load_events(journal_path)

    assert state.status is RunStatus.COMPLETE
    assert replay(journal_path) == state
    assert len(model.requests) == LOGICAL_AGENT_LANES
    assert len([event for event in events if event.kind is EventKind.MODEL_TURN_STARTED]) == 8
    assert len([event for event in events if event.kind is EventKind.MODEL_TURN_COMPLETED]) == 8
    assert len([event for event in events if event.kind is EventKind.ACTION_REJECTED]) == 1

    candidate_events = [event for event in events if event.kind is EventKind.CANDIDATE_UPDATED]
    assert [event.payload["screen_rank"] for event in candidate_events] == list(range(1, 26))
    rejected = [event for event in candidate_events if event.payload.get("diligence_status") == "rejected"]
    assert len(rejected) == 1
    assert "controller ranking is unchanged" in rejected[0].payload["evidence_gaps"][-1]
    workflow = next(event for event in events if event.kind is EventKind.WORKFLOW_COMPLETED)
    assert workflow.payload["diligence_failures"] == 1


def test_funnel_excluded_counts_inspected_rows_not_selected() -> None:
    manifest = build_fixture_manifest(build_fixture_policy(100))
    source = replace(manifest.market_sources[0], row_count=120)
    expanded = replace(
        manifest,
        market_sources=(source,),
        screened_unique_count=120,
        fetched_row_count=120,
    )

    funnel = FunnelProgress(
        expanded,
        AUTOMATIC_ANALYSIS_MODE,
        configured_provider_slots=8,
        batches_total=4,
    ).snapshot()

    assert funnel["provider_matches"] == 225
    assert funnel["inspected"] == 120
    assert funnel["selected"] == 100
    assert funnel["excluded"] == 20
    assert funnel["uninspected"] == 105
