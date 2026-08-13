from __future__ import annotations

import asyncio

import pytest

from alpha_orchestration.domain import (
    CandidateDataQuality,
    CandidateSourceMode,
    EventKind,
)
from alpha_orchestration.journal import load_events, replay
from alpha_orchestration.offline_harness import run_harness


def test_multi_issuer_harness_proves_parallel_ranked_results(tmp_path) -> None:
    journal = tmp_path / "events.jsonl"

    summary = asyncio.run(run_harness(journal))

    assert summary["ok"] is True
    assert summary["branches_completed"] == 3
    assert summary["branch_statuses"] == {
        "analyze-alp": "complete",
        "analyze-bet": "complete",
        "analyze-gam": "complete",
    }
    assert summary["slot_limit"] == 3
    assert summary["observed_peak_active_tasks"] == 3
    assert summary["model_peak_active_calls"] == 3
    assert summary["slot_bound_verified"] is True
    assert summary["dependency_order_verified"] is True
    assert summary["candidate_count"] == 3
    assert summary["ranked_ids"] == ["ticker:ALP", "ticker:BET", "ticker:GAM"]
    assert summary["validator_rank_override_blocked"] is True
    assert summary["source_coverage"] == {
        "available": 9,
        "cited": 9,
        "complete": True,
    }
    assert summary["data_quality_posture"] == {"complete": 3, "total": 3}
    assert summary["results_ready"] is True
    assert summary["replay_equivalent"] is True

    restored = replay(journal)
    alpha = restored.candidates["ticker:ALP"]
    assert {item.metric: item.value for item in alpha.financials} == pytest.approx(
        {"revenue_growth": 0.4, "net_margin": 0.15}
    )
    assert alpha.data_quality is CandidateDataQuality.COMPLETE
    assert alpha.source_mode is CandidateSourceMode.SYNTHETIC
    assert tuple(restored.candidates) == ("ticker:ALP", "ticker:BET", "ticker:GAM")
    assert "ticker:FORGED" not in restored.candidates

    events = load_events(journal)
    branch_done = {
        event.payload["task_id"]: event.sequence
        for event in events
        if event.kind is EventKind.TASK_COMPLETED
        and event.payload.get("task_id") in {"analyze-alp", "analyze-bet", "analyze-gam"}
    }
    validator_started = next(
        event.sequence
        for event in events
        if event.kind is EventKind.TASK_STARTED
        and event.payload.get("task_id") == "validate-ranked-results"
    )
    workflow_completed = next(
        event.sequence for event in events if event.kind is EventKind.WORKFLOW_COMPLETED
    )
    candidate_sequences = [
        event.sequence for event in events if event.kind is EventKind.CANDIDATE_UPDATED
    ]

    assert len(branch_done) == 3
    assert max(branch_done.values()) < validator_started
    assert len(candidate_sequences) == 3
    assert max(candidate_sequences) < workflow_completed
