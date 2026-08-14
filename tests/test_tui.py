import asyncio

from textual.containers import VerticalScroll
from textual.widgets import Button, DataTable, Input, Select, Static, TabbedContent

from alpha_orchestration.adapters.demo import DemoRuntime
from alpha_orchestration.domain import EventKind, RunSpec, RunStatus
from alpha_orchestration.journal import MemoryJournal
from alpha_orchestration.ports import EventDraft
from alpha_orchestration.tui.app import (
    AUTOMATIC_LIVE_MODE,
    AlphaApp,
    AutomaticPreflightScreen,
    LiveReadiness,
    MissionScreen,
    RunScreen,
)


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
            initial_spec=RunSpec(run_id="live-results", mode="live", universe_size=3),
            live_runtime_factory=lambda _spec, _tickers: EmptyRuntime(),
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
            initial_spec=RunSpec(run_id="concurrency-results", mode="live", universe_size=3),
            live_runtime_factory=lambda _spec, _tickers: ConcurrencyTelemetryRuntime(),
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


class PartialLiveCollectionRuntime:
    async def stream(self, spec: RunSpec):  # type: ignore[no-untyped-def]
        del spec
        yield EventDraft(
            EventKind.STAGE_STARTED,
            "Collecting live SEC and market data for 3 issuers",
            payload={
                "stage": "evidence",
                "progress": 20,
                "live_collection": {
                    "requested_tickers": ["AAPL", "MSFT", "NVDA"],
                },
            },
        )
        yield EventDraft(
            EventKind.STAGE_COMPLETED,
            "Live source collection completed with provider gaps",
            payload={
                "stage": "evidence",
                "progress": 70,
                "live_collection": {
                    "requested_tickers": ["AAPL", "MSFT", "NVDA"],
                    "requested_count": 3,
                    "ready_count": 2,
                    "failed_count": 1,
                    "partial": True,
                    "provider_successes": {"sec": 2, "yfinance": 1},
                    "provider_failures": {"sec": 1, "yfinance": 1},
                    "mapping": {
                        "retrieved_at": "2026-08-13T13:59:00+00:00",
                    },
                    "issuers": [
                        {
                            "ticker": "AAPL",
                            "status": "ready",
                            "providers": {
                                "sec": {
                                    "status": "ok",
                                    "source": "network",
                                    "retrieved_at": "2026-08-13T14:00:00+00:00",
                                },
                                "yfinance": {
                                    "status": "failed",
                                    "source": "network",
                                },
                            },
                        },
                        {
                            "ticker": "MSFT",
                            "status": "ready",
                            "providers": {
                                "sec": {
                                    "status": "ok",
                                    "source": "cache",
                                    "retrieved_at": "2026-08-13T14:01:00+00:00",
                                },
                                "yfinance": {
                                    "status": "ok",
                                    "source": "network",
                                    "retrieved_at": "2026-08-13T14:02:00+00:00",
                                },
                            },
                        },
                        {"ticker": "NVDA", "status": "failed"},
                    ],
                    "failures": [
                        {
                            "ticker": "AAPL",
                            "provider": "yfinance",
                            "phase": "snapshot",
                            "error": "market snapshot unavailable",
                            "retryable": False,
                            "occurred_at": "2026-08-13T14:00:30+00:00",
                        },
                        {
                            "ticker": "NVDA",
                            "provider": "sec",
                            "phase": "issuer",
                            "error": "issuer facts unavailable",
                            "retryable": False,
                            "occurred_at": "2026-08-13T14:01:30+00:00",
                        },
                    ],
                },
            },
        )


def test_live_mission_controls_are_scrollable_and_reachable() -> None:
    async def exercise(size: tuple[int, int]) -> None:
        app = AlphaApp(
            live_readiness=LiveReadiness(
                sec_identity_configured=True,
                yfinance_installed=True,
                runtime_available=True,
                analysis_label="RULE-BASED",
            ),
            live_runtime_factory=lambda _spec, _tickers: EmptyRuntime(),
            journal_factory=lambda _: MemoryJournal(),
        )
        async with app.run_test(size=size) as pilot:
            await pilot.pause()
            screen = app.screen
            assert isinstance(screen, MissionScreen)
            screen.query_one("#mode-select", Select).value = "live"
            await pilot.pause()

            card = screen.query_one("#mission-card", VerticalScroll)
            readiness = screen.query_one("#readiness-panel", Static)
            launch = screen.query_one("#launch-run", Button)
            assert card.region.bottom <= app.size.height
            assert card.max_scroll_y > 0

            card.scroll_end(animate=False)
            await pilot.pause()
            assert card.region.contains_region(readiness.region)
            assert card.region.contains_region(launch.region)
            assert not launch.disabled
            readiness_text = str(readiness.render())
            assert "SEC: CONFIGURED" in readiness_text
            assert "MARKET: INSTALLED" in readiness_text
            assert "RUNTIME: AVAILABLE" in readiness_text

            await pilot.click("#launch-run")
            await pilot.pause()
            assert isinstance(app.screen, RunScreen)

    for terminal_size in ((120, 38), (90, 28)):
        asyncio.run(exercise(terminal_size))


def test_partial_live_collection_is_explicit_in_results() -> None:
    async def exercise() -> tuple[str, str, str, str, str, int]:
        app = AlphaApp(
            initial_spec=RunSpec(run_id="partial-live", mode="live", universe_size=3),
            initial_tickers=("AAPL", "MSFT", "NVDA"),
            live_runtime_factory=lambda _spec, _tickers: PartialLiveCollectionRuntime(),
            journal_factory=lambda _: MemoryJournal(),
        )
        async with app.run_test(size=(150, 44)) as pilot:
            await pilot.pause()
            screen = app.screen
            assert isinstance(screen, RunScreen)
            await asyncio.wait_for(screen.completed.wait(), timeout=2)
            await pilot.pause()
            status = screen.query_one("#results-status", Static)
            return (
                str(status.render()),
                str(screen.query_one("#result-coverage", Static).render()),
                str(screen.query_one("#result-sources", Static).render()),
                str(screen.query_one("#results-detail", Static).render()),
                str(screen.query_one("#safety-bar", Static).render()),
                status.content_region.height,
            )

    status, coverage, sources, detail, safety, status_height = asyncio.run(exercise())
    assert "PARTIAL LIVE RESULTS" in status
    assert "2 of 3 had usable evidence" in status
    assert "1 issuer failed" in status
    assert "2026-08-13T14:02:00+00:00" in status
    assert "AAPL / yfinance: market snapshot unavailable" in status
    assert "USABLE / REQUESTED" in coverage
    assert "2 / 3" in coverage
    assert "LIVE PARTIAL / SEC 2/3 / MARKET 1/3" in sources
    assert "partial live collection" in detail
    assert "SYNTHETIC" not in safety
    assert status_height >= 3


class LiveProvenanceRuntime:
    async def stream(self, spec: RunSpec):  # type: ignore[no-untyped-def]
        del spec
        yield EventDraft(
            EventKind.AGENT_REGISTERED,
            "Registered ALP research lane",
            agent_id="live-alp",
            payload={"role": "Live issuer analyst", "lane": "ALP"},
        )
        yield EventDraft(
            EventKind.EVIDENCE_ADDED,
            "Added SEC evidence for ALP",
            agent_id="live-alp",
            payload={
                "evidence_id": "ev-live-alp",
                "title": "ALP annual filing",
                "source": "SEC",
                "source_kind": "company_facts",
                "summary": "Live SEC record",
                "observed_at": "2024-12-31T00:00:00+00:00",
                "retrieved_at": "2026-08-13T12:00:00+00:00",
                "source_url": "https://www.sec.gov/Archives/edgar/data/1/alp-2024.htm",
                "synthetic": False,
            },
        )
        yield EventDraft(
            EventKind.CANDIDATE_UPDATED,
            "Ranked ALP as a research priority",
            payload={
                "candidate_id": "ticker:ALP",
                "ticker": "ALP",
                "company": "Alpha Logic",
                "bucket": "advance",
                "priority_score": 62,
                "variant_wedge": "Rule-based screen signal",
                "why_now": "Latest filing evidence is available",
                "first_rejection": "The screen does not establish a mispricing",
                "investable_if": "Deeper diligence confirms the signal",
                "kill_if": "Follow-up evidence invalidates the signal",
                "next_workflow": "company_tearsheet",
                "evidence_ids": ["ev-live-alp"],
                "source_mode": "live",
                "as_of": "2024-12-31",
            },
        )


def test_live_candidate_detail_keeps_retrieval_and_source_url() -> None:
    async def exercise() -> str:
        app = AlphaApp(
            initial_spec=RunSpec(run_id="live-provenance", mode="live", universe_size=1),
            initial_tickers=("ALP",),
            live_runtime_factory=lambda _spec, _tickers: LiveProvenanceRuntime(),
            journal_factory=lambda _: MemoryJournal(),
        )
        async with app.run_test(size=(150, 44)) as pilot:
            await pilot.pause()
            screen = app.screen
            assert isinstance(screen, RunScreen)
            await asyncio.wait_for(screen.completed.wait(), timeout=2)
            await pilot.pause()
            return str(screen.query_one("#results-detail", Static).render())

    detail = asyncio.run(exercise())
    assert "retrieved 2026-08-13T12:00:00+00:00" in detail
    assert "Source URL: https://www.sec.gov/Archives/edgar/data/1/alp-2024.htm" in detail


class AutomaticFunnelRuntime:
    async def stream(self, spec: RunSpec):  # type: ignore[no-untyped-def]
        del spec
        starting = {
            "profile": "US_LARGE_LIQUID_V1",
            "stage": "screening",
            "total": 300,
            "provider_matches": 1_962,
            "inspected": 1_000,
            "selected": 300,
            "discovered": 1_962,
            "eligible": 300,
            "screened": 125,
            "deep_reviewed": 0,
            "surfaced": 0,
            "excluded": 700,
            "failed": 0,
            "source_posture": "SEC OFFICIAL MAP + SEPARATELY TIMESTAMPED YFINANCE EVIDENCE",
            "as_of": "2026-08-13",
            "retrieved_at": "2026-08-13T15:00:00+00:00",
            "batches_completed": 5,
            "batches_total": 12,
            "configured_agent_lanes": 8,
            "configured_provider_slots": 8,
            "observed_peak_provider_requests": 8,
            "observed_peak_analysis_tasks": 8,
            "analysis_mode": "RULE-BASED",
            "universe_rows": [
                {"ticker": "ALP", "company": "Alpha Logic", "status": "surfaced", "rank": 1},
                {"ticker": "BET", "company": "Beta Systems", "status": "screened", "rank": 122},
                {"ticker": "GAM", "company": "Gamma Works", "status": "failed", "rank": 250},
            ],
        }
        yield EventDraft(
            EventKind.STAGE_STARTED,
            "Screened 125 of 300 selected issuers",
            payload={"stage": "analysis", "progress": 48, "universe_funnel": starting},
        )
        completed = {
            **starting,
            "stage": "complete",
            "screened": 297,
            "deep_reviewed": 36,
            "surfaced": 1,
            "failed": 3,
            "batches_completed": 12,
            "retrieved_at": "2026-08-13T15:12:00+00:00",
        }
        yield EventDraft(
            EventKind.CANDIDATE_UPDATED,
            "Surfaced ALP for deeper research",
            payload={
                "candidate_id": "ticker:ALP",
                "ticker": "ALP",
                "company": "Alpha Logic",
                "bucket": "advance",
                "priority_score": 73,
                "variant_wedge": "Rule-based screen signal requiring deeper diligence",
                "why_now": "Recent source-backed growth and cash conversion passed the screen",
                "first_rejection": "The screen does not establish what is priced in",
                "investable_if": "A sourced valuation and expectations review confirms a variant wedge",
                "kill_if": "Current valuation already discounts the operating case",
                "next_workflow": "company_tearsheet",
                "source_mode": "live",
                "confidence": "medium",
                "data_quality": "partial",
                "as_of": "2026-08-13",
                "universe_funnel": completed,
            },
        )


def test_automatic_startup_fails_closed_to_preflight_without_fixture_fallback() -> None:
    async def exercise(size: tuple[int, int]) -> None:
        calls = 0

        def forbidden_runtime(_spec: RunSpec):
            nonlocal calls
            calls += 1
            raise AssertionError("blocked automatic startup must not construct a runtime")

        app = AlphaApp(
            initial_spec=RunSpec(
                sector="U.S. listed equities",
                universe_size=300,
                agent_budget=8,
                active_slots=8,
                mode=AUTOMATIC_LIVE_MODE,
                run_id="automatic-preflight",
            ),
            automatic_runtime_factory=forbidden_runtime,
            live_readiness=LiveReadiness(
                sec_identity_configured=False,
                yfinance_installed=True,
                runtime_available=True,
                blocker="ALPHA_SEC_USER_AGENT is not configured",
            ),
        )
        async with app.run_test(size=size) as pilot:
            await pilot.pause()
            assert isinstance(app.screen, AutomaticPreflightScreen)
            rendered = str(app.screen.query_one("#automatic-preflight-status", Static).render())
            assert "SEC IDENTITY" in rendered
            assert "MISSING" in rendered
            assert "ALPHA_SEC_USER_AGENT" in rendered
            assert "PREFLIGHT BLOCKED" in str(
                app.screen.query_one("#automatic-preflight-copy", Static).render()
            )
            assert calls == 0
            card = app.screen.query_one("#automatic-preflight-card", VerticalScroll)
            card.scroll_end(animate=False)
            await pilot.pause()
            expert_button = app.screen.query_one("#expert-setup", Button)
            assert card.region.contains_region(expert_button.region)
            await pilot.click("#expert-setup")
            await pilot.pause()
            assert isinstance(app.screen, MissionScreen)

    for terminal_size in ((120, 38), (90, 28)):
        asyncio.run(exercise(terminal_size))


def test_automatic_large_universe_funnel_is_compact_readable_and_capturable() -> None:
    async def exercise() -> str:
        app = AlphaApp(
            initial_spec=RunSpec(
                sector="U.S. listed equities",
                universe_size=300,
                agent_budget=8,
                active_slots=8,
                mode=AUTOMATIC_LIVE_MODE,
                run_id="automatic-funnel",
            ),
            automatic_runtime_factory=lambda _spec: AutomaticFunnelRuntime(),
            live_readiness=LiveReadiness(
                sec_identity_configured=True,
                yfinance_installed=True,
                runtime_available=True,
                analysis_label="RULE-BASED",
            ),
            journal_factory=lambda _: MemoryJournal(),
        )
        async with app.run_test(size=(150, 44)) as pilot:
            await pilot.pause()
            screen = app.screen
            assert isinstance(screen, RunScreen)
            await asyncio.wait_for(screen.completed.wait(), timeout=2)
            await pilot.pause()
            funnel = str(screen.query_one("#results-funnel", Static).render())
            status = str(screen.query_one("#results-status", Static).render())
            coverage = str(screen.query_one("#result-coverage", Static).render())
            sources = str(screen.query_one("#result-sources", Static).render())
            assert "US_LARGE_LIQUID_V1" in funnel
            assert "PROVIDER MATCHES 1,962" in funnel
            assert "INSPECTED 1,000" in funnel
            assert "SELECTED 300" in funnel
            assert "297" in funnel
            assert "36" in funnel
            assert "EXCLUDED AFTER INSPECTION 700" in funnel
            assert "UNINSPECTED 962" in funnel
            assert "FAILED 3" in funnel
            assert "AUTOMATIC SCREEN COMPLETE WITH GAPS" in status
            assert "SCREENED / SELECTED" in coverage
            assert "297 / 300" in coverage
            assert "SEC OFFICIAL MAP + SEPARATELY TIMESTAMPED YFINANCE EVIDENCE" in sources
            assert "RULE-BASED" in sources
            assert screen.query_one("#results-table", DataTable).row_count == 1
            universe_table = screen.query_one("#universe-table", DataTable)
            assert universe_table.row_count == 3
            screen.query_one("#universe-search", Input).value = "beta"
            await pilot.pause()
            assert universe_table.row_count == 1
            universe_summary = str(screen.query_one("#universe-summary", Static).render())
            assert "showing 1 of 3 persisted rows" in universe_summary
            svg = app.export_screenshot(title="Automatic universe results", simplify=True)
            assert svg.startswith("<svg")
            assert "AUTOMATIC" in svg
            return svg

    capture = asyncio.run(exercise())
    assert len(capture) > 10_000
