import asyncio

from textual.widgets import DataTable, Static, TabbedContent

from alpha_orchestration.adapters.demo import DemoRuntime
from alpha_orchestration.domain import EventKind, RunSpec, RunStatus
from alpha_orchestration.journal import MemoryJournal
from alpha_orchestration.ports import EventDraft
from alpha_orchestration.tui.app import AlphaApp, MissionScreen, RunScreen


def test_mission_screen_mounts() -> None:
    async def exercise() -> None:
        app = AlphaApp()
        async with app.run_test(size=(120, 38)) as pilot:
            await pilot.pause()
            assert isinstance(app.screen, MissionScreen)

    asyncio.run(exercise())


def test_headless_demo_reaches_human_review() -> None:
    async def exercise() -> None:
        spec = RunSpec(run_id="run-tui")
        journal = MemoryJournal()
        app = AlphaApp(
            initial_spec=spec,
            runtime_factory=lambda _: DemoRuntime(0),
            journal_factory=lambda _: journal,
        )
        async with app.run_test(size=(150, 44)) as pilot:
            await pilot.pause()
            screen = app.screen
            assert isinstance(screen, RunScreen)
            await asyncio.wait_for(screen.completed.wait(), timeout=2)
            await pilot.pause()
            assert screen.controller.state.status is RunStatus.COMPLETE
            assert screen.controller.state.candidates
            tabs = screen.query_one("#run-tabs", TabbedContent)
            assert tabs.active == "results-tab"
            assert screen.query_one("#results-table", DataTable).row_count == 3
            status = str(screen.query_one("#results-status", Static).render())
            assert "BOUNDED RUN COMPLETE" in status
            assert "18 of 18 reviewed" in status
            assert "whole market" in status
            assert "NO LIVE READINESS" in str(screen.query_one("#result-sources", Static).render())
            detail = str(screen.query_one("#results-detail", Static).render())
            assert "ALP-X" in detail
            assert "KEY FINANCIAL SNAPSHOT" in detail
            assert "Revenue growth" in detail
            results_table = screen.query_one("#results-table", DataTable)
            results_table.focus()
            results_table.move_cursor(row=1)
            await pilot.pause()
            assert "OPEN EVIDENCE GAPS" in detail
            assert "NOT AN INVESTMENT RECOMMENDATION" in detail
            agent_table = screen.query_one("#agent-table", DataTable)
            lead_row = agent_table.get_row("lead")
            assert "Candidate funnel ready" in str(lead_row[3])
            assert "ION-X" in str(screen.query_one("#results-detail", Static).render())
            assert "SERIAL FIXTURE" in str(screen.query_one("#metric-engine", Static).render())
            assert journal.closed

    asyncio.run(exercise())


class EmptyRuntime:
    async def stream(self, spec: RunSpec):  # type: ignore[no-untyped-def]
        del spec
        if False:
            yield EventDraft(EventKind.RUN_STARTED, "unreachable")


class FailedRuntime:
    async def stream(self, spec: RunSpec):  # type: ignore[no-untyped-def]
        del spec
        if False:
            yield EventDraft(EventKind.RUN_STARTED, "unreachable")
        raise RuntimeError("synthetic source adapter unavailable")


class SlowRuntime:
    async def stream(self, spec: RunSpec):  # type: ignore[no-untyped-def]
        del spec
        yield EventDraft(EventKind.STAGE_STARTED, "Review started", payload={"stage": "universe"})
        await asyncio.Event().wait()


class ConcurrencyTelemetryRuntime:
    async def stream(self, spec: RunSpec):  # type: ignore[no-untyped-def]
        del spec
        yield EventDraft(
            EventKind.AGENT_REGISTERED,
            "Telemetry worker registered",
            agent_id="worker",
            payload={"role": "research", "lane": "analysis"},
        )
        yield EventDraft(
            EventKind.WORKFLOW_PLANNED,
            "Workflow planned",
            payload={
                "workflow_id": "telemetry",
                "workflow_version": "1",
                "tasks": [
                    {
                        "task_id": "one",
                        "agent_id": "worker",
                        "depends_on": [],
                        "required": True,
                    }
                ],
                "effective_active_slots": 4,
                "actual_active_slots": None,
            },
        )
        yield EventDraft(
            EventKind.TASK_STARTED,
            "Telemetry task started",
            agent_id="worker",
            payload={"task_id": "one"},
        )
        yield EventDraft(
            EventKind.TASK_COMPLETED,
            "Telemetry task completed",
            agent_id="worker",
            payload={"task_id": "one", "output": {"status": "complete"}},
        )
        yield EventDraft(
            EventKind.WORKFLOW_COMPLETED,
            "Workflow completed",
            payload={
                "observed_peak_active_tasks": 3,
            },
        )


class PartialThenFailRuntime:
    async def stream(self, spec: RunSpec):  # type: ignore[no-untyped-def]
        del spec
        yield EventDraft(
            EventKind.CANDIDATE_UPDATED,
            "Candidate synthesized before failure",
            payload={
                "candidate_id": "partial",
                "ticker": "PART-X",
                "company": "Partial Systems · synthetic",
                "bucket": "exposure_unproven",
                "priority_score": 41,
                "variant_wedge": "Provisional",
                "why_now": "Incomplete evidence window",
                "first_rejection": "Run did not finish",
                "investable_if": "A complete rerun confirms the evidence",
                "kill_if": "The rerun rejects the signal",
                "next_workflow": "rerun",
            },
        )
        raise RuntimeError("downstream synthesis failed")


def test_results_explain_empty_and_failed_terminal_states() -> None:
    async def exercise(runtime, run_id: str) -> tuple[RunStatus, str, str, int]:
        app = AlphaApp(
            initial_spec=RunSpec(run_id=run_id),
            runtime_factory=lambda _: runtime,
            journal_factory=lambda _: MemoryJournal(),
        )
        async with app.run_test(size=(150, 44)) as pilot:
            await pilot.pause()
            screen = app.screen
            assert isinstance(screen, RunScreen)
            await asyncio.wait_for(screen.completed.wait(), timeout=2)
            await pilot.pause()
            return (
                screen.controller.state.status,
                str(screen.query_one("#results-status", Static).render()),
                str(screen.query_one("#results-detail", Static).render()),
                screen.query_one("#results-table", DataTable).row_count,
            )

    empty_status, empty_banner, empty_detail, empty_count = asyncio.run(exercise(EmptyRuntime(), "empty-results"))
    assert empty_status is RunStatus.COMPLETE
    assert "NO CANDIDATES SURFACED" in empty_banner
    assert "not evidence" in empty_detail
    assert empty_count == 0

    failed_status, failed_banner, failed_detail, failed_count = asyncio.run(exercise(FailedRuntime(), "failed-results"))
    assert failed_status is RunStatus.FAILED
    assert "RUN FAILED" in failed_banner
    assert "Coverage is incomplete" in failed_banner
    assert "No candidate brief" in failed_detail
    assert failed_count == 0

    partial_status, partial_banner, _, partial_count = asyncio.run(
        exercise(PartialThenFailRuntime(), "partial-results")
    )
    assert partial_status is RunStatus.FAILED
    assert "PARTIAL RESULTS · RUN FAILED" in partial_banner
    assert partial_count == 1


def test_results_explain_in_progress_and_cancelled_states() -> None:
    async def exercise() -> tuple[str, str, str, str]:
        app = AlphaApp(
            initial_spec=RunSpec(run_id="cancelled-results"),
            runtime_factory=lambda _: SlowRuntime(),
            journal_factory=lambda _: MemoryJournal(),
        )
        async with app.run_test(size=(150, 44)) as pilot:
            await pilot.pause()
            screen = app.screen
            assert isinstance(screen, RunScreen)
            screen.action_show_results()
            await pilot.pause()
            running = str(screen.query_one("#results-status", Static).render())
            await screen.action_cancel_run()
            await asyncio.wait_for(screen.completed.wait(), timeout=2)
            await pilot.pause()
            return (
                running,
                str(screen.query_one("#results-status", Static).render()),
                str(screen.query_one("#results-detail", Static).render()),
                screen.query_one("#run-tabs", TabbedContent).active,
            )

    running, cancelled, detail, active_tab = asyncio.run(exercise())
    assert "RESEARCH IN PROGRESS" in running
    assert "RUN CANCELLED" in cancelled
    assert "provisional" in cancelled
    assert "No candidate brief" in detail
    assert active_tab == "results-tab"


def test_live_mode_is_not_labeled_as_synthetic() -> None:
    async def exercise() -> tuple[str, str]:
        app = AlphaApp(
            initial_spec=RunSpec(run_id="live-results", mode="live"),
            runtime_factory=lambda _: EmptyRuntime(),
            journal_factory=lambda _: MemoryJournal(),
        )
        async with app.run_test(size=(150, 44)) as pilot:
            await pilot.pause()
            screen = app.screen
            assert isinstance(screen, RunScreen)
            await asyncio.wait_for(screen.completed.wait(), timeout=2)
            await pilot.pause()
            return (
                str(screen.query_one("#result-sources", Static).render()),
                str(screen.query_one("#safety-bar", Static).render()),
            )

    source_posture, safety_bar = asyncio.run(exercise())
    assert "LIVE · VERIFY READINESS" in source_posture
    assert "VERIFY SOURCE READINESS" in safety_bar
    assert "SYNTHETIC" not in safety_bar


def test_execution_metric_prefers_observed_peak_concurrency() -> None:
    async def exercise() -> str:
        app = AlphaApp(
            initial_spec=RunSpec(run_id="concurrency-results", mode="live"),
            runtime_factory=lambda _: ConcurrencyTelemetryRuntime(),
            journal_factory=lambda _: MemoryJournal(),
        )
        async with app.run_test(size=(150, 44)) as pilot:
            await pilot.pause()
            screen = app.screen
            assert isinstance(screen, RunScreen)
            await asyncio.wait_for(screen.completed.wait(), timeout=2)
            await pilot.pause()
            return str(screen.query_one("#metric-engine", Static).render())

    execution_metric = asyncio.run(exercise())
    assert "OBSERVED PEAK / LIMIT" in execution_metric
    assert "3 / 4" in execution_metric
