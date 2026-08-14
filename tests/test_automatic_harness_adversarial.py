from __future__ import annotations

import asyncio

from alpha_orchestration.automatic_harness import execute_fixture, terminal_funnel, verify_screen_intervals
from alpha_orchestration.domain import EventKind, RunStatus, TaskStatus


def _row_funnel_events(run) -> list:
    return [
        event
        for event in run.events
        if isinstance(event.payload.get("universe_funnel"), dict)
        and "universe_rows" in event.payload["universe_funnel"]
    ]


def test_one_sec_failure_is_isolated_and_full_audit_rows_survive(tmp_path) -> None:
    failed_ticker = "U0050"
    run = asyncio.run(
        execute_fixture(
            tmp_path / "isolated.jsonl",
            cache_root=tmp_path / "isolated-cache",
            target_size=100,
            fact_failures=(failed_ticker,),
        )
    )

    assert run.state.status is RunStatus.COMPLETE
    assert run.restored == run.state
    funnel = terminal_funnel(run.events)
    assert (funnel["selected"], funnel["eligible"], funnel["screened"], funnel["failed"]) == (
        100,
        99,
        99,
        1,
    )
    rows = funnel["universe_rows"]
    assert isinstance(rows, list) and len(rows) == 100
    failed = next(row for row in rows if row["ticker"] == failed_ticker)
    assert failed["status"] == "failed"
    assert failed["screen_score"] is None
    assert all(candidate.ticker != failed_ticker for candidate in run.state.candidates.values())
    screen_states = [
        task.status for task in run.state.tasks.values() if task.task_id.startswith("screen-")
    ]
    assert verify_screen_intervals(run.events) == 8
    assert screen_states.count(TaskStatus.FAILED) == 1
    assert screen_states.count(TaskStatus.COMPLETE) == 99
    assert len(_row_funnel_events(run)) == 1
    assert not run.market.snapshot_calls


def test_below_minimum_coverage_fails_after_one_complete_audit_snapshot(tmp_path) -> None:
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
    assert run.events[-1].kind is EventKind.RUN_FAILED
    assert not any(event.kind is EventKind.WORKFLOW_COMPLETED for event in run.events)
    assert not any(event.kind is EventKind.CANDIDATE_UPDATED for event in run.events)
    row_events = _row_funnel_events(run)
    assert len(row_events) == 1
    assert row_events[0].kind is EventKind.STAGE_COMPLETED
    assert row_events[0].payload["stage"] == "analysis"
    funnel = row_events[0].payload["universe_funnel"]
    assert (funnel["selected"], funnel["eligible"], funnel["screened"], funnel["failed"]) == (
        100,
        89,
        89,
        11,
    )
    assert len(funnel["universe_rows"]) == 100
    assert verify_screen_intervals(run.events) == 8
    assert not run.market.snapshot_calls
