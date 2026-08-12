import asyncio
from datetime import UTC, datetime

from textual.widgets import Checkbox, DataTable, Input, Select, Static, TabbedContent

from alpha_orchestration.domain import EventKind, RunEvent, RunSpec
from alpha_orchestration.journal import MemoryJournal
from alpha_orchestration.ports import EventDraft
from alpha_orchestration.tui.app import AlphaApp, RunScreen
from alpha_orchestration.tui.debug import (
    UNASSIGNED_AGENT,
    EventQuery,
    count_events,
    event_row,
    filter_events,
    follow_row_index,
    format_agent_transcript,
    format_counters,
    format_event_detail,
)


def _event(
    sequence: int,
    kind: EventKind,
    message: str,
    *,
    agent_id: str | None = None,
    payload: dict[str, object] | None = None,
) -> RunEvent:
    return RunEvent(
        schema_version=1,
        run_id="debug-run",
        sequence=sequence,
        kind=kind,
        timestamp=datetime(2026, 8, 12, 15, 30, sequence % 60, tzinfo=UTC),
        message=message,
        agent_id=agent_id,
        payload={} if payload is None else payload,  # type: ignore[arg-type]
    )


def test_debug_presenter_filters_summarizes_and_preserves_exact_payload() -> None:
    events = [
        _event(1, EventKind.RUN_STARTED, "start [literal]"),
        _event(
            2,
            EventKind.TOOL_STARTED,
            "call <unsafe>",
            agent_id="analyst",
            payload={
                "task_id": "growth",
                "tool": "compound_growth",
                "call_id": "debug-run:growth:t1:c0",
                "arguments": {"start": 100, "end": 144, "periods": 2},
            },
        ),
        _event(3, EventKind.TOOL_FAILED, "bad units", agent_id="analyst"),
    ]

    assert filter_events(events, EventQuery(family="tool")) == tuple(events[1:])
    assert filter_events(events, EventQuery(agent_id=UNASSIGNED_AGENT)) == (events[0],)
    assert filter_events(events, EventQuery(search="t1:c0")) == (events[1],)
    assert event_row(events[1])[2:4] == ("tool_started", "analyst")
    assert "call_id=debug-run:growth:t1:c0" in event_row(events[1])[4]

    detail = format_event_detail(events[1]).plain
    assert '"arguments": {' in detail
    assert '"periods": 2' in detail
    assert "call <unsafe>" in detail
    transcript = format_agent_transcript(events, events[1]).plain
    assert "tool_started" in transcript
    assert "tool_failed" in transcript

    counters = count_events(events, events[1:])
    assert counters.total == 3
    assert counters.visible == 2
    assert counters.tool_calls == 1
    assert counters.failures == 1
    assert "VISIBLE 2" in format_counters(counters)


def test_follow_index_keeps_selection_or_tracks_tail() -> None:
    assert follow_row_index(enabled=True, current=1, row_count=9) == 8
    assert follow_row_index(enabled=False, current=1, row_count=9) == 1
    assert follow_row_index(enabled=False, current=99, row_count=9) == 8
    assert follow_row_index(enabled=False, current=None, row_count=9) == 0
    assert follow_row_index(enabled=True, current=None, row_count=0) is None


class BurstRuntime:
    async def stream(self, spec: RunSpec):  # type: ignore[no-untyped-def]
        del spec
        yield EventDraft(
            EventKind.AGENT_REGISTERED,
            "Registered [debug] analyst",
            agent_id="analyst",
            payload={"role": "Debug Analyst", "lane": "Harness"},
        )
        yield EventDraft(EventKind.AGENT_STARTED, "Started", agent_id="analyst")
        for index in range(90):
            yield EventDraft(
                EventKind.AGENT_PROGRESS,
                f"step {index}",
                agent_id="analyst",
                payload={"progress": min(index, 99), "needle": f"row-{index}"},
            )
        yield EventDraft(
            EventKind.TOOL_STARTED,
            "Calling compound growth [literal]",
            agent_id="analyst",
            payload={
                "tool": "compound_growth",
                "call_id": "debug-run:growth:t1:c0",
                "arguments": {"start": 100, "end": 144, "periods": 2},
                "argument_hash": "sha256:arguments",
            },
        )
        yield EventDraft(
            EventKind.TOOL_COMPLETED,
            "Growth calculated",
            agent_id="analyst",
            payload={
                "tool": "compound_growth",
                "call_id": "debug-run:growth:t1:c0",
                "result": {"rate": 0.2},
                "result_hash": "sha256:result",
                "source_ids": ["sec:fact:revenue:2025"],
            },
        )
        yield EventDraft(EventKind.AGENT_COMPLETED, "Finished", agent_id="analyst")


def test_debug_tab_retains_full_stream_and_exercises_filters_and_detail() -> None:
    async def exercise() -> None:
        spec = RunSpec(run_id="debug-run")
        app = AlphaApp(
            initial_spec=spec,
            runtime_factory=lambda _: BurstRuntime(),
            journal_factory=lambda _: MemoryJournal(),
        )
        async with app.run_test(size=(150, 44)) as pilot:
            await pilot.pause()
            screen = app.screen
            assert isinstance(screen, RunScreen)
            await asyncio.wait_for(screen.completed.wait(), timeout=3)
            await pilot.pause()

            tabs = screen.query_one("#run-tabs", TabbedContent)
            assert tabs.active == "overview-tab"
            screen.action_show_debug()
            await pilot.pause()
            assert tabs.active == "debug-tab"

            event_table = screen.query_one("#debug-event-table", DataTable)
            assert len(screen.debug_events) > 80
            assert len(screen.controller.state.recent_events) == 80
            assert event_table.row_count == len(screen.debug_events)

            screen.query_one("#debug-kind-filter", Select).value = "tool"
            screen.query_one("#debug-search", Input).value = "arguments"
            await pilot.pause()
            assert event_table.row_count == 1
            detail = screen.query_one("#debug-detail", Static).render()
            assert '"call_id": "debug-run:growth:t1:c0"' in str(detail)
            assert '"periods": 2' in str(detail)
            assert "[literal]" in str(detail)

            screen.query_one("#debug-search", Input).value = ""
            screen.query_one("#debug-agent-filter", Select).value = "analyst"
            await pilot.pause()
            assert event_table.row_count == 2
            assert screen.query_one("#debug-follow", Checkbox).value
            expected_tail = max(
                event.sequence for event in screen.debug_events if event.kind is EventKind.TOOL_COMPLETED
            )
            assert screen._debug_selected_sequence == expected_tail

            screen.action_show_overview()
            assert tabs.active == "overview-tab"

    asyncio.run(exercise())


def test_debug_tab_small_viewport_smoke() -> None:
    async def exercise() -> None:
        app = AlphaApp(
            initial_spec=RunSpec(run_id="debug-small"),
            runtime_factory=lambda _: BurstRuntime(),
            journal_factory=lambda _: MemoryJournal(),
        )
        async with app.run_test(size=(90, 28)) as pilot:
            await pilot.pause()
            screen = app.screen
            assert isinstance(screen, RunScreen)
            screen.action_show_debug()
            await asyncio.wait_for(screen.completed.wait(), timeout=3)
            await pilot.pause()
            assert screen.query_one("#debug-event-table", DataTable).row_count > 80
            assert screen.query_one("#debug-detail", Static).region.height > 0

    asyncio.run(exercise())
