"""Mission setup and live run screens for AlphaOrchestration."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from pathlib import Path
from typing import cast

from rich.markup import escape
from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical, VerticalScroll
from textual.screen import ModalScreen, Screen
from textual.widgets import (
    Button,
    Checkbox,
    DataTable,
    Footer,
    Input,
    Label,
    ProgressBar,
    RichLog,
    Select,
    Static,
    TabbedContent,
    TabPane,
)

from alpha_orchestration.adapters.demo import DemoRuntime
from alpha_orchestration.controller import RunController
from alpha_orchestration.domain import (
    STAGE_ORDER,
    AgentStatus,
    Candidate,
    CandidateBucket,
    CandidateFinancial,
    EventKind,
    ResearchDepth,
    RunEvent,
    RunSpec,
    RunState,
    RunStatus,
    Stage,
)
from alpha_orchestration.journal import JsonlJournal
from alpha_orchestration.ports import EventJournal, OrchestratorRuntime
from alpha_orchestration.tui.debug import (
    ALL_EVENTS,
    EVENT_FAMILIES,
    UNASSIGNED_AGENT,
    EventQuery,
    available_agents,
    count_events,
    event_row,
    filter_events,
    follow_row_index,
    format_agent_transcript,
    format_counters,
    format_event_detail,
)

JournalFactory = Callable[[RunSpec], EventJournal]
RuntimeFactory = Callable[[RunSpec], OrchestratorRuntime]


STAGE_LABELS = {
    Stage.UNIVERSE: "Universe map",
    Stage.EVIDENCE: "Evidence collection",
    Stage.ANALYSIS: "Challenge + analysis",
    Stage.SYNTHESIS: "Candidate synthesis",
    Stage.REVIEW: "Human review",
}

AGENT_STYLES = {
    AgentStatus.QUEUED: ("○", "#8293A7", "QUEUED"),
    AgentStatus.RUNNING: ("●", "#54D6FF", "RUNNING"),
    AgentStatus.WAITING_TOOL: ("◐", "#E3B341", "TOOL"),
    AgentStatus.COMPLETE: ("✓", "#7EE787", "DONE"),
    AgentStatus.CANCELLED: ("×", "#8293A7", "CANCEL"),
    AgentStatus.FAILED: ("!", "#FF6B6B", "FAILED"),
}

BUCKET_STYLES = {
    CandidateBucket.ADVANCE: ("ADVANCE", "#7EE787"),
    CandidateBucket.VALUATION_GATED: ("GATED", "#E3B341"),
    CandidateBucket.EXPOSURE_UNPROVEN: ("UNPROVEN", "#C099FF"),
    CandidateBucket.DEPRIORITIZED: ("DEPRIOR", "#8293A7"),
}

EVENT_COLORS = {
    EventKind.RUN_FAILED: "#FF6B6B",
    EventKind.AGENT_FAILED: "#FF6B6B",
    EventKind.RUN_COMPLETED: "#7EE787",
    EventKind.AGENT_COMPLETED: "#7EE787",
    EventKind.EVIDENCE_ADDED: "#54D6FF",
    EventKind.CANDIDATE_UPDATED: "#C099FF",
    EventKind.TOOL_STARTED: "#E3B341",
    EventKind.RUN_PAUSED: "#E3B341",
}


def _financial_value(financial: CandidateFinancial) -> str:
    unit = financial.unit.casefold()
    if unit == "ratio":
        return f"{financial.value * 100:.1f}%"
    if unit == "x":
        return f"{financial.value:.1f}x"
    if unit == "usd millions":
        return f"${financial.value:,.0f}m"
    return f"{financial.value:g} {financial.unit}"


def _readable_token(value: str) -> str:
    return value.replace("_", " ").upper()


class HelpScreen(ModalScreen[None]):
    BINDINGS = [Binding("escape", "dismiss", "Close", show=False)]

    def compose(self) -> ComposeResult:
        with Container(id="help-dialog"):
            yield Static("ALPHA / CONTROLS", id="help-title")
            yield Static(
                "[b]Space[/b]  Pause or resume the event stream\n"
                "[b]O / V / D[/b]  Overview / results / debug journal\n"
                "[b]Enter[/b]  Inspect the selected candidate\n"
                "[b]C[/b]      Cancel and drain the run\n"
                "[b]R[/b]      Restart with the same mandate\n"
                "[b]N[/b]      Create a new mandate\n"
                "[b]Q[/b]      Quit\n\n"
                "Results rank the next diligence step, not expected return. The demo "
                "uses offline synthetic fixtures, has no live-source readiness, and "
                "does not issue investment recommendations.",
                markup=True,
            )
            yield Button("CLOSE", id="close-help", variant="primary")

    def action_dismiss(self) -> None:
        self.dismiss(None)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "close-help":
            self.dismiss(None)


class MissionScreen(Screen[None]):
    BINDINGS = [
        Binding("q", "quit_app", "Quit"),
        Binding("enter", "start_run", "Launch", show=False),
    ]

    def compose(self) -> ComposeResult:
        with Container(id="mission-shell"), Vertical(id="mission-card"):
            yield Static("ALPHA / ORCHESTRATION", id="mission-brand")
            yield Static(
                "LOCAL-FIRST RESEARCH OPERATIONS  ·  SYNTHETIC PROTOTYPE",
                id="mission-kicker",
            )
            yield Static(
                "[b]Run a bounded research workflow.[/b]\n"
                "Map a sector, collect evidence, challenge the screen, and hand "
                "a small candidate funnel to a human analyst.",
                id="mission-copy",
                markup=True,
            )
            yield Label("SECTOR / THEME", classes="field-label")
            yield Input(value="Semiconductors", id="sector-input")
            with Horizontal(classes="field-row"):
                with Vertical(classes="field-block"):
                    yield Label("RESEARCH DEPTH", classes="field-label")
                    yield Select(
                        [("Quick scan", "quick"), ("Standard", "standard"), ("Deep", "deep")],
                        value="standard",
                        id="depth-select",
                    )
                with Vertical(classes="field-block"):
                    yield Label("UNIVERSE SIZE", classes="field-label")
                    yield Input(value="18", id="universe-input")
            with Horizontal(id="mission-stats"):
                yield Static("[dim]RESEARCH ROLES[/dim]\n[b]8[/b]", classes="mission-stat")
                yield Static("[dim]SLOT LIMIT[/dim]\n[b]4 MAX[/b]", classes="mission-stat")
                yield Static("[dim]DATA MODE[/dim]\n[b]OFFLINE[/b]", classes="mission-stat")
            yield Static(
                "[b #E3B341]SYNTHETIC REPLAY[/b #E3B341]  No live sources or model "
                "execution. The slot limit is configuration, not measured concurrency.",
                id="demo-notice",
                markup=True,
            )
            with Horizontal(id="mission-actions"):
                yield Button("LAUNCH RESEARCH RUN", id="launch-run", variant="primary")
                yield Button("QUIT", id="quit", variant="default")

    def on_mount(self) -> None:
        self.query_one("#sector-input", Input).focus()

    def action_quit_app(self) -> None:
        self.app.exit()

    def action_start_run(self) -> None:
        self._launch()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        del event
        self._launch()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "launch-run":
            self._launch()
        elif event.button.id == "quit":
            self.app.exit()

    def _launch(self) -> None:
        sector = self.query_one("#sector-input", Input).value.strip()
        raw_universe = self.query_one("#universe-input", Input).value.strip()
        depth_select = self.query_one("#depth-select", Select)
        try:
            universe_size = int(raw_universe)
            depth = ResearchDepth(str(depth_select.value))
            spec = RunSpec(sector=sector, depth=depth, universe_size=universe_size)
        except (TypeError, ValueError) as exc:
            self.notify(str(exc), severity="error", timeout=4)
            return
        cast("AlphaApp", self.app).launch_run(spec)


class RunScreen(Screen[None]):
    BINDINGS = [
        Binding("space", "toggle_pause", "Pause / resume"),
        Binding("c", "cancel_run", "Cancel"),
        Binding("r", "restart_run", "Restart"),
        Binding("n", "new_run", "New run"),
        Binding("d", "show_debug", "Debug"),
        Binding("o", "show_overview", "Overview", show=False),
        Binding("v", "show_results", "Results"),
        Binding("question_mark", "show_help", "Help"),
        Binding("q", "quit_app", "Quit"),
    ]

    def __init__(
        self,
        spec: RunSpec,
        runtime: OrchestratorRuntime,
        journal: EventJournal,
    ) -> None:
        super().__init__()
        self.spec = spec
        self.controller = RunController(spec, runtime, journal, subscribers=[self._on_event])
        self.completed = asyncio.Event()
        self._selected_candidate_id: str | None = None
        self._events: list[RunEvent] = []
        self._debug_selected_sequence: int | None = None
        self._debug_agents: tuple[str, ...] = ()
        self._debug_ready = False
        self._debug_rendering = False
        self._last_rendered_status: RunStatus | None = None

    def compose(self) -> ComposeResult:
        yield Static("", id="top-banner", markup=True)
        yield ProgressBar(total=100, show_eta=False, show_percentage=False, id="run-progress")
        with Horizontal(id="metric-row"):
            yield Static("[dim]PROGRESS[/dim]\n[b]0%[/b]", id="metric-progress", classes="metric")
            yield Static("[dim]AGENTS[/dim]\n[b]0 / 8[/b]", id="metric-agents", classes="metric")
            yield Static("[dim]EVIDENCE[/dim]\n[b]0[/b]", id="metric-evidence", classes="metric")
            yield Static("[dim]CANDIDATES[/dim]\n[b]0[/b]", id="metric-candidates", classes="metric")
            yield Static("[dim]EXECUTION[/dim]\n[b]NOT STARTED[/b]", id="metric-engine", classes="metric")
        with TabbedContent(id="run-tabs"):
            with TabPane("OVERVIEW", id="overview-tab"), Horizontal(id="main-grid"):
                with Vertical(id="left-pane", classes="panel"):
                    yield Static("PIPELINE", classes="section-title")
                    yield Static("", id="pipeline", markup=True)
                    yield Static("AGENT ROSTER", classes="section-title")
                    yield DataTable(id="agent-table")
                with Vertical(id="center-pane", classes="panel"):
                    yield Static("LIVE ACTIVITY", classes="section-title")
                    yield RichLog(id="activity-log", markup=True, wrap=True, highlight=False)
                    yield Static("EVIDENCE LEDGER", classes="section-title")
                    yield DataTable(id="evidence-table")
                with Vertical(id="right-pane", classes="panel"):
                    yield Static("RESEARCH-PRIORITY FUNNEL", classes="section-title")
                    yield DataTable(id="candidate-table")
                    yield Static("SELECTED CANDIDATE", classes="section-title")
                    with VerticalScroll(id="candidate-scroll"):
                        yield Static(
                            "[dim]Candidates will appear after evidence has been challenged.[/dim]",
                            id="candidate-detail",
                            markup=True,
                        )
            with TabPane("RESULTS", id="results-tab"), Vertical(id="results-shell"):
                yield Static(
                    "[b #7AA2F7]PREPARING BOUNDED RUN[/b #7AA2F7]  No candidate ranking is available yet.",
                    id="results-status",
                    markup=True,
                )
                with Horizontal(id="results-summary"):
                    yield Static("", id="result-coverage", classes="result-stat")
                    yield Static("", id="result-surfaced", classes="result-stat")
                    yield Static("", id="result-evidence", classes="result-stat")
                    yield Static("", id="result-quality", classes="result-stat")
                    yield Static("", id="result-sources", classes="result-stat")
                with Horizontal(id="results-grid"):
                    with Vertical(id="results-list-pane", classes="panel"):
                        yield Static("RANKED RESEARCH CANDIDATES", classes="section-title")
                        yield Static(
                            "Priority ranks the next diligence step—not expected return.",
                            id="results-ranking-note",
                        )
                        yield DataTable(id="results-table")
                    with Vertical(id="results-detail-pane", classes="panel"):
                        yield Static("CANDIDATE RESEARCH BRIEF", classes="section-title")
                        with VerticalScroll(id="results-detail-scroll"):
                            yield Static(
                                "[dim]Results will appear as candidates are synthesized.[/dim]",
                                id="results-detail",
                                markup=True,
                            )
            with TabPane("DEBUG / JOURNAL", id="debug-tab"), Vertical(id="debug-shell"):
                with Horizontal(id="debug-controls"):
                    yield Select(
                        [(family.upper(), family) for family in EVENT_FAMILIES],
                        value=ALL_EVENTS,
                        allow_blank=False,
                        id="debug-kind-filter",
                    )
                    yield Select(
                        [("ALL AGENTS", ALL_EVENTS), ("UNASSIGNED", UNASSIGNED_AGENT)],
                        value=ALL_EVENTS,
                        allow_blank=False,
                        id="debug-agent-filter",
                    )
                    yield Input(placeholder="Search message, payload, ID?", id="debug-search")
                    yield Checkbox("FOLLOW TAIL", value=True, id="debug-follow")
                yield Static("", id="debug-counters", markup=False)
                with Horizontal(id="debug-grid"):
                    with Vertical(id="debug-events-pane", classes="panel"):
                        yield Static("EVENT STREAM", classes="section-title")
                        yield DataTable(id="debug-event-table")
                    with Vertical(id="debug-inspector-pane", classes="panel"):
                        yield Static("EXACT JOURNAL RECORD", classes="section-title")
                        with VerticalScroll(id="debug-detail-scroll"):
                            yield Static("", id="debug-detail", markup=False)
                        yield Static("AGENT TRANSCRIPT", classes="section-title")
                        with VerticalScroll(id="debug-transcript-scroll"):
                            yield Static("", id="debug-transcript", markup=False)
        yield Static(
            "TRIAGE ONLY  ·  SYNTHETIC FIXTURES  ·  HUMAN REVIEW REQUIRED BEFORE ANY ACTION",
            id="safety-bar",
        )
        yield Footer()

    def on_mount(self) -> None:
        agent_table = self.query_one("#agent-table", DataTable)
        agent_table.add_columns("", "AGENT", "STATE", "WORK")
        agent_table.cursor_type = "row"
        agent_table.zebra_stripes = True

        evidence_table = self.query_one("#evidence-table", DataTable)
        evidence_table.add_columns("SIGNAL", "SOURCE", "SUMMARY")
        evidence_table.cursor_type = "row"
        evidence_table.zebra_stripes = True

        candidate_table = self.query_one("#candidate-table", DataTable)
        candidate_table.add_columns("TICKER", "PRIORITY", "STATUS")
        candidate_table.cursor_type = "row"
        candidate_table.zebra_stripes = True

        debug_table = self.query_one("#debug-event-table", DataTable)
        debug_table.add_columns("SEQ", "TIME (UTC)", "KIND", "AGENT", "SUMMARY")
        debug_table.cursor_type = "row"
        debug_table.zebra_stripes = True
        self._debug_ready = True
        results_table = self.query_one("#results-table", DataTable)
        results_table.add_columns("#", "TICKER", "PRIORITY", "TRIAGE", "CONF.")
        results_table.cursor_type = "row"
        results_table.zebra_stripes = True

        self._render_state(self.controller.state)
        self._render_debug()
        self.run_worker(self._drive(), name=f"run:{self.spec.run_id}", exclusive=True)

    async def _drive(self) -> None:
        try:
            await self.controller.run()
        finally:
            self.completed.set()

    async def on_unmount(self) -> None:
        await self.controller.cancel()

    def _on_event(self, event: RunEvent) -> None:
        self._events.append(event)
        color = EVENT_COLORS.get(event.kind, "#8293A7")
        agent = f"  [#54D6FF]{escape(event.agent_id)}[/#54D6FF]" if event.agent_id else ""
        line = Text.from_markup(
            f"[dim]{event.sequence:03d}[/dim]  [{color}]{event.kind.value.upper():<20}[/{color}]"
            f"{agent}  {escape(event.message)}"
        )
        self.query_one("#activity-log", RichLog).write(line)
        self._render_state(self.controller.state)
        self._render_debug()

    @property
    def debug_events(self) -> tuple[RunEvent, ...]:
        """Return the full screen-owned stream, including events older than state history."""

        return tuple(self._events)

    def _debug_query(self) -> EventQuery:
        family = self.query_one("#debug-kind-filter", Select).value
        agent_id = self.query_one("#debug-agent-filter", Select).value
        return EventQuery(
            family=ALL_EVENTS if family is Select.BLANK else str(family),
            agent_id=ALL_EVENTS if agent_id is Select.BLANK else str(agent_id),
            search=self.query_one("#debug-search", Input).value,
        )

    def _sync_debug_agent_options(self) -> None:
        agents = available_agents(self._events)
        if agents == self._debug_agents:
            return
        select = self.query_one("#debug-agent-filter", Select)
        current = select.value
        select.set_options(
            [("ALL AGENTS", ALL_EVENTS), ("UNASSIGNED", UNASSIGNED_AGENT)]
            + [(agent_id, agent_id) for agent_id in agents]
        )
        if current == UNASSIGNED_AGENT or current in agents:
            select.value = current
        else:
            select.value = ALL_EVENTS
        self._debug_agents = agents

    def _render_debug(self) -> None:
        if not self._debug_ready or self._debug_rendering:
            return
        self._debug_rendering = True
        try:
            self._sync_debug_agent_options()
            visible = filter_events(self._events, self._debug_query())
            current_index = next(
                (index for index, event in enumerate(visible) if event.sequence == self._debug_selected_sequence),
                None,
            )
            target_index = follow_row_index(
                enabled=self.query_one("#debug-follow", Checkbox).value,
                current=current_index,
                row_count=len(visible),
            )
            table = self.query_one("#debug-event-table", DataTable)
            table.clear(columns=False)
            for event in visible:
                table.add_row(*event_row(event), key=str(event.sequence))

            counters = count_events(self._events, visible)
            self.query_one("#debug-counters", Static).update(Text(format_counters(counters), style="bold #8293A7"))
            if target_index is None:
                self._render_debug_selection(None)
                return
            table.move_cursor(row=target_index, column=0, animate=False)
            self._render_debug_selection(visible[target_index])
        finally:
            self._debug_rendering = False

    def _render_debug_selection(self, event: RunEvent | None) -> None:
        self._debug_selected_sequence = None if event is None else event.sequence
        self.query_one("#debug-detail", Static).update(format_event_detail(event))
        self.query_one("#debug-transcript", Static).update(format_agent_transcript(self._events, event))

    def on_select_changed(self, event: Select.Changed) -> None:
        if event.select.id in {"debug-kind-filter", "debug-agent-filter"}:
            self._render_debug()

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id == "debug-search":
            self._render_debug()

    def on_checkbox_changed(self, event: Checkbox.Changed) -> None:
        if event.checkbox.id == "debug-follow":
            self._render_debug()

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        if event.data_table.id == "results-table":
            candidate_id = str(event.row_key.value)
            candidate = self.controller.state.candidates.get(candidate_id)
            if candidate is not None:
                self._selected_candidate_id = candidate_id
                self._render_result_detail(self.controller.state, candidate)
                self._render_candidate_detail(candidate)
            return
        if event.data_table.id != "debug-event-table":
            return
        try:
            sequence = int(str(event.row_key.value))
        except (TypeError, ValueError):
            return
        selected = next((item for item in self._events if item.sequence == sequence), None)
        if selected is not None:
            self._render_debug_selection(selected)

    def _render_state(self, state: RunState) -> None:
        previous_status = self._last_rendered_status
        status_color = {
            RunStatus.IDLE: "#8293A7",
            RunStatus.PLANNING: "#7AA2F7",
            RunStatus.RUNNING: "#54D6FF",
            RunStatus.PAUSED: "#E3B341",
            RunStatus.SYNTHESIZING: "#C099FF",
            RunStatus.COMPLETE: "#7EE787",
            RunStatus.CANCELLED: "#8293A7",
            RunStatus.FAILED: "#FF6B6B",
        }[state.status]
        mode_label = _readable_token(state.spec.mode)
        self.query_one("#top-banner", Static).update(
            f"[b]ALPHA / ORCHESTRATION[/b]   [#E3B341]{escape(mode_label)}[/#E3B341]   "
            f"[b]{escape(state.spec.sector.upper())}[/b]   "
            f"[{status_color}]● {state.status.value.upper()}[/{status_color}]   "
            f"[dim]{escape(state.spec.run_id)}[/dim]"
        )
        self.query_one("#run-progress", ProgressBar).update(progress=state.progress, total=100)
        self.query_one("#metric-progress", Static).update(f"[dim]PROGRESS[/dim]\n[b]{state.progress}%[/b]")
        self.query_one("#metric-agents", Static).update(
            f"[dim]AGENTS[/dim]\n[b]{state.complete_agents} / {state.spec.agent_budget}[/b]"
        )
        self.query_one("#metric-evidence", Static).update(f"[dim]EVIDENCE[/dim]\n[b]{len(state.evidence)}[/b]")
        self.query_one("#metric-candidates", Static).update(f"[dim]CANDIDATES[/dim]\n[b]{len(state.candidates)}[/b]")
        self.query_one("#metric-engine", Static).update(self._execution_metric(state))
        self._render_pipeline(state)
        self._render_agents(state)
        self._render_evidence(state)
        self._render_candidates(state)
        self._render_results(state)
        self._render_safety_bar(state)
        tabs = self.query_one("#run-tabs", TabbedContent)
        terminal_statuses = {RunStatus.COMPLETE, RunStatus.CANCELLED, RunStatus.FAILED}
        if (
            state.status in terminal_statuses
            and previous_status not in terminal_statuses
            and tabs.active == "overview-tab"
        ):
            tabs.active = "results-tab"
        self._last_rendered_status = state.status

    def _execution_metric(self, state: RunState) -> str:
        completed = next(
            (event for event in reversed(self._events) if event.kind is EventKind.WORKFLOW_COMPLETED),
            None,
        )
        planned = next(
            (event for event in reversed(self._events) if event.kind is EventKind.WORKFLOW_PLANNED),
            None,
        )
        limit = state.spec.active_slots
        if planned is not None:
            raw_limit = planned.payload.get("effective_active_slots")
            if isinstance(raw_limit, int) and not isinstance(raw_limit, bool):
                limit = raw_limit
        if completed is not None:
            peak = completed.payload.get("observed_peak_active_tasks")
            if isinstance(peak, int) and not isinstance(peak, bool):
                return f"[dim]OBSERVED PEAK / LIMIT[/dim]\n[b]{peak} / {limit}[/b]"
        if planned is not None:
            actual = planned.payload.get("actual_active_slots")
            if isinstance(actual, int) and not isinstance(actual, bool):
                return f"[dim]ACTUAL / SLOT LIMIT[/dim]\n[b]{actual} / {limit}[/b]"
            peak_label = "PENDING" if not state.terminal else "NOT REPORTED"
            return f"[dim]OBSERVED PEAK / LIMIT[/dim]\n[b]{peak_label} / {limit}[/b]"
        if state.spec.mode == "synthetic_demo":
            return "[dim]EXECUTION[/dim]\n[b]SERIAL FIXTURE[/b]"
        return f"[dim]SLOT LIMIT[/dim]\n[b]≤ {state.spec.active_slots} · ACTUAL N/R[/b]"

    def _synthetic_posture(self, state: RunState) -> bool:
        return (bool(state.evidence) and all(evidence.synthetic for evidence in state.evidence.values())) or (
            not state.evidence and state.spec.mode == "synthetic_demo"
        )

    def _render_safety_bar(self, state: RunState) -> None:
        if self._synthetic_posture(state):
            posture = "SYNTHETIC FIXTURES · NO LIVE-SOURCE READINESS"
        elif any(evidence.synthetic for evidence in state.evidence.values()):
            posture = "MIXED SOURCES · VERIFY AS-OF + PROVENANCE"
        else:
            posture = "VERIFY SOURCE READINESS + AS-OF"
        self.query_one("#safety-bar", Static).update(
            f"TRIAGE ONLY  ·  {posture}  ·  HUMAN REVIEW REQUIRED BEFORE ANY ACTION"
        )

    def _reviewed_issuers(self) -> int | None:
        for event in reversed(self._events):
            raw_value = event.payload.get("reviewed_issuers")
            if raw_value is None and event.payload.get("tool") == "demo.universe_map":
                raw_value = event.payload.get("rows")
            if isinstance(raw_value, int) and not isinstance(raw_value, bool) and raw_value >= 0:
                return raw_value
        return None

    def _render_results(self, state: RunState) -> None:
        ranked = sorted(
            state.candidates.values(),
            key=lambda candidate: candidate.priority_score,
            reverse=True,
        )
        table = self.query_one("#results-table", DataTable)
        table.clear(columns=False)
        for rank, candidate in enumerate(ranked, start=1):
            label, color = BUCKET_STYLES[candidate.bucket]
            table.add_row(
                str(rank),
                Text(candidate.ticker, style="bold #D7E3EE"),
                f"{candidate.priority_score} / 100",
                Text(label, style=color),
                _readable_token(candidate.confidence.value),
                key=candidate.candidate_id,
            )

        if ranked and self._selected_candidate_id not in state.candidates:
            self._selected_candidate_id = ranked[0].candidate_id
        selected = state.candidates.get(self._selected_candidate_id or "")
        if selected is not None:
            selected_index = next(
                index for index, candidate in enumerate(ranked) if candidate.candidate_id == selected.candidate_id
            )
            table.move_cursor(row=selected_index, column=0, animate=False)
            self._render_result_detail(state, selected)
        else:
            self.query_one("#results-detail", Static).update(self._empty_results_markup(state))

        reviewed = self._reviewed_issuers()
        if reviewed is not None:
            coverage_value = f"{reviewed} / {state.spec.universe_size}"
        elif state.status in {RunStatus.IDLE, RunStatus.PLANNING}:
            coverage_value = f"PENDING / {state.spec.universe_size}"
        elif state.status in {RunStatus.RUNNING, RunStatus.PAUSED, RunStatus.SYNTHESIZING}:
            coverage_value = f"IN PROGRESS / {state.spec.universe_size}"
        elif state.status in {RunStatus.CANCELLED, RunStatus.FAILED}:
            coverage_value = f"PARTIAL / {state.spec.universe_size}"
        else:
            coverage_value = f"N/R / {state.spec.universe_size}"
        self.query_one("#result-coverage", Static).update(f"[dim]REVIEWED / REQUESTED[/dim]\n[b]{coverage_value}[/b]")
        self.query_one("#result-surfaced", Static).update(f"[dim]CANDIDATES SURFACED[/dim]\n[b]{len(ranked)}[/b]")
        candidates_with_gaps = sum(bool(candidate.evidence_gaps) for candidate in ranked)
        self.query_one("#result-evidence", Static).update(
            f"[dim]EVIDENCE / OPEN GAPS[/dim]\n[b]{len(state.evidence)} records · {candidates_with_gaps} names[/b]"
        )
        quality_flags = sum(
            candidate.data_quality.value in {"limited", "partial", "not_assessed"} for candidate in ranked
        )
        self.query_one("#result-quality", Static).update(
            f"[dim]DATA-QUALITY FLAGS[/dim]\n[b]{quality_flags} / {len(ranked)}[/b]"
        )
        if self._synthetic_posture(state):
            source_value = "OFFLINE FIXTURE · NO LIVE READINESS"
        elif any(evidence.synthetic for evidence in state.evidence.values()):
            source_value = "MIXED SOURCES · VERIFY AS-OF"
        else:
            source_value = f"{_readable_token(state.spec.mode)} · VERIFY READINESS"
        self.query_one("#result-sources", Static).update(f"[dim]SOURCE POSTURE[/dim]\n[b]{escape(source_value)}[/b]")
        self.query_one("#results-status", Static).update(self._results_status_markup(state, reviewed, len(ranked)))

    def _results_status_markup(
        self,
        state: RunState,
        reviewed: int | None,
        candidate_count: int,
    ) -> str:
        if reviewed is None:
            coverage = f"review count not reported; {state.spec.universe_size} requested"
        else:
            coverage = f"{reviewed} of {state.spec.universe_size} reviewed"
        if state.status is RunStatus.COMPLETE:
            if candidate_count:
                headline = (
                    f"[b #7EE787]BOUNDED RUN COMPLETE[/b #7EE787]  {coverage}; "
                    f"{candidate_count} research candidates surfaced."
                )
            else:
                headline = (
                    f"[b #E3B341]NO CANDIDATES SURFACED[/b #E3B341]  {coverage}; none met this run's triage criteria."
                )
            return (
                f"{headline}\n[dim]This is a research-priority screen—not an investment "
                "recommendation or a conclusion about the whole market.[/dim]"
            )
        if state.status is RunStatus.FAILED:
            prefix = "PARTIAL RESULTS · " if candidate_count else ""
            failure = escape((state.failure or "Unspecified runtime failure")[:140])
            return (
                f"[b #FF6B6B]{prefix}RUN FAILED[/b #FF6B6B]  {failure}\n"
                "[dim]Coverage is incomplete. Preserve artifacts for diagnosis; do not use "
                "the current ranking as a completed screen.[/dim]"
            )
        if state.status is RunStatus.CANCELLED:
            prefix = "PARTIAL RESULTS · " if candidate_count else ""
            return (
                f"[b #E3B341]{prefix}RUN CANCELLED[/b #E3B341]  Work stopped before "
                "the bounded mandate completed.\n[dim]Any surfaced candidates are provisional "
                "and require a new or resumed run.[/dim]"
            )
        if state.status is RunStatus.PAUSED:
            return (
                "[b #E3B341]RESEARCH PAUSED[/b #E3B341]  Results are provisional and may "
                "change after the event stream resumes."
            )
        if state.status is RunStatus.SYNTHESIZING:
            return (
                "[b #C099FF]SYNTHESIZING CANDIDATES[/b #C099FF]  Reconciling evidence, "
                "objections, quality flags, and next diligence steps."
            )
        if state.status is RunStatus.RUNNING:
            return (
                "[b #54D6FF]RESEARCH IN PROGRESS[/b #54D6FF]  The bounded universe is still "
                "being reviewed; rankings are not final."
            )
        return "[b #7AA2F7]PREPARING BOUNDED RUN[/b #7AA2F7]  No candidate ranking is available yet."

    def _empty_results_markup(self, state: RunState) -> str:
        if state.status is RunStatus.COMPLETE:
            return (
                "[b #E3B341]No research candidate met this run's triage criteria.[/b #E3B341]\n\n"
                "Review the mandate, exclusions, and evidence coverage before changing the "
                "screen. This is not evidence that the sector or wider market has no opportunities."
            )
        if state.status is RunStatus.FAILED:
            return (
                "[b #FF6B6B]No candidate brief is available.[/b #FF6B6B]\n\n"
                "The run failed before a candidate could be synthesized. Inspect Debug / Journal."
            )
        if state.status is RunStatus.CANCELLED:
            return (
                "[b #E3B341]No candidate brief is available.[/b #E3B341]\n\n"
                "The run was cancelled before synthesis completed."
            )
        return "[dim]Results will appear as candidates are synthesized.[/dim]"

    def _render_result_detail(self, state: RunState, candidate: Candidate) -> None:
        label, color = BUCKET_STYLES[candidate.bucket]
        financial_lines = "\n\n".join(
            f"• [b]{escape(financial.label)}[/b]  {_financial_value(financial)}  "
            f"[dim]{escape(financial.period)}[/dim]\n"
            f"  [dim]Sources: {escape(', '.join(financial.source_ids) or 'not provided')}[/dim]"
            for financial in candidate.financials
        )
        if not financial_lines:
            financial_lines = "[dim]No typed financial snapshot was supplied by this run.[/dim]"

        evidence_lines: list[str] = []
        for evidence_id in candidate.evidence_ids:
            evidence = state.evidence.get(evidence_id)
            if evidence is None:
                evidence_lines.append(f"• [#E3B341]{escape(evidence_id)} — record unavailable[/#E3B341]")
            else:
                evidence_lines.append(
                    f"• {escape(evidence.title)} — [dim]{escape(evidence.source)} · {escape(evidence_id)}[/dim]"
                )
        if not evidence_lines:
            evidence_lines.append("[dim]No evidence records were linked.[/dim]")
        gap_lines = (
            "\n".join(f"• {escape(gap)}" for gap in candidate.evidence_gaps)
            or "[dim]No explicit gaps were supplied; treat absence as not assessed.[/dim]"
        )
        evidence_text = "\n".join(evidence_lines)
        next_workflow = candidate.next_workflow.replace("_", " ").title()
        if candidate.source_mode.value == "synthetic":
            source_caveat = "Synthetic fixtures make no real-company claim and have no live-source readiness."
        elif candidate.source_mode.value == "live":
            source_caveat = "Verify every cited live source, as-of date, and calculation before action."
        elif candidate.source_mode.value == "mixed":
            source_caveat = "Mixed-source result: reconcile synthetic and live evidence before action."
        else:
            source_caveat = "Source posture was not supplied; verify provenance and as-of before action."
        self.query_one("#results-detail", Static).update(
            f"[b #D7E3EE]{escape(candidate.ticker)} · {escape(candidate.company)}[/b #D7E3EE]\n"
            f"[dim]PRIORITY {candidate.priority_score} / 100 · RESEARCH URGENCY ONLY[/dim]\n"
            f"[{color}]TRIAGE | {escape(label.upper())}[/{color}]\n"
            f"[b #E3B341]NOT AN INVESTMENT RECOMMENDATION[/b #E3B341]\n\n"
            f"[b]CONFIDENCE[/b]  {_readable_token(candidate.confidence.value)}    "
            f"[b]DATA QUALITY[/b]  {_readable_token(candidate.data_quality.value)}\n"
            f"[b]AS OF[/b]  {escape(candidate.as_of)}    "
            f"[b]SOURCE MODE[/b]  {_readable_token(candidate.source_mode.value)}\n\n"
            f"[b #54D6FF]WHY IT SURFACED · VARIANT WEDGE[/b #54D6FF]\n"
            f"{escape(candidate.variant_wedge)}\n\n"
            f"[b #54D6FF]WHY NOW[/b #54D6FF]\n{escape(candidate.why_now)}\n\n"
            f"[b]KEY FINANCIAL SNAPSHOT[/b]\n{financial_lines}\n\n"
            f"[b #E3B341]FIRST REJECTION / DOWNSIDE MECHANISM[/b #E3B341]\n"
            f"{escape(candidate.first_rejection)}\n\n"
            f"[b]WHAT WOULD MAKE IT INVESTABLE[/b]\n{escape(candidate.investable_if)}\n\n"
            f"[b #FF6B6B]KILL TEST[/b #FF6B6B]\n{escape(candidate.kill_if)}\n\n"
            f"[b]EVIDENCE USED[/b]\n{evidence_text}\n\n"
            f"[b #E3B341]OPEN EVIDENCE GAPS[/b #E3B341]\n{gap_lines}\n\n"
            f"[b]NEXT DILIGENCE[/b]  {escape(next_workflow)}\n\n"
            f"[dim]{escape(label)} is a triage label. "
            f"{escape(source_caveat)}[/dim]"
        )

    def _render_pipeline(self, state: RunState) -> None:
        lines: list[str] = []
        for stage in STAGE_ORDER:
            if stage in state.completed_stages:
                symbol, color = "✓", "#7EE787"
            elif stage is state.current_stage:
                symbol, color = "●", "#54D6FF"
            else:
                symbol, color = "○", "#526274"
            lines.append(f"[{color}]{symbol}  {STAGE_LABELS[stage]}[/{color}]")
        self.query_one("#pipeline", Static).update("\n".join(lines))

    def _render_agents(self, state: RunState) -> None:
        table = self.query_one("#agent-table", DataTable)
        table.clear(columns=False)
        for agent in state.agents.values():
            symbol, color, label = AGENT_STYLES[agent.status]
            table.add_row(
                Text(symbol, style=color),
                agent.role,
                Text(label, style=color),
                agent.current_task,
                key=agent.agent_id,
            )

    def _render_evidence(self, state: RunState) -> None:
        table = self.query_one("#evidence-table", DataTable)
        table.clear(columns=False)
        for evidence in tuple(state.evidence.values())[-7:]:
            summary = evidence.summary
            if len(summary) > 60:
                summary = f"{summary[:57]}…"
            table.add_row(evidence.title, evidence.source, summary, key=evidence.evidence_id)

    def _render_candidates(self, state: RunState) -> None:
        ranked = sorted(
            state.candidates.values(),
            key=lambda candidate: candidate.priority_score,
            reverse=True,
        )
        table = self.query_one("#candidate-table", DataTable)
        table.clear(columns=False)
        for candidate in ranked:
            label, color = BUCKET_STYLES[candidate.bucket]
            table.add_row(
                Text(candidate.ticker, style="bold #D7E3EE"),
                str(candidate.priority_score),
                Text(label, style=color),
                key=candidate.candidate_id,
            )
        if ranked and self._selected_candidate_id not in state.candidates:
            self._selected_candidate_id = ranked[0].candidate_id
        selected = state.candidates.get(self._selected_candidate_id or "")
        if selected is not None:
            self._render_candidate_detail(selected)

    def _render_candidate_detail(self, candidate: Candidate) -> None:
        label, color = BUCKET_STYLES[candidate.bucket]
        self.query_one("#candidate-detail", Static).update(
            f"[b #D7E3EE]{escape(candidate.ticker)}[/b #D7E3EE]  "
            f"[{color}]{label}[/{color}]  [dim]PRIORITY {candidate.priority_score}[/dim]\n"
            f"{escape(candidate.company)}\n\n"
            f"[b #54D6FF]VARIANT WEDGE[/b #54D6FF]\n{escape(candidate.variant_wedge)}\n\n"
            f"[b #54D6FF]WHY NOW[/b #54D6FF]\n{escape(candidate.why_now)}\n\n"
            f"[b #E3B341]FIRST REJECTION[/b #E3B341]\n{escape(candidate.first_rejection)}\n\n"
            f"[b]INVESTABLE IF[/b]\n{escape(candidate.investable_if)}\n\n"
            f"[b #FF6B6B]KILL TEST[/b #FF6B6B]\n{escape(candidate.kill_if)}\n\n"
            f"[dim]NEXT WORKFLOW[/dim]  {escape(candidate.next_workflow)}\n"
            f"[dim]EVIDENCE IDS[/dim]   {len(candidate.evidence_ids)}"
        )

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if event.data_table.id == "debug-event-table":
            try:
                sequence = int(str(event.row_key.value))
            except (TypeError, ValueError):
                return
            selected = next((item for item in self._events if item.sequence == sequence), None)
            if selected is not None:
                self._debug_selected_sequence = sequence
                self.query_one("#debug-follow", Checkbox).value = False
                self._render_debug_selection(selected)
            return
        if event.data_table.id in {"candidate-table", "results-table"}:
            candidate_id = str(event.row_key.value)
            candidate = self.controller.state.candidates.get(candidate_id)
            if candidate is not None:
                self._selected_candidate_id = candidate_id
                self._render_candidate_detail(candidate)

                self._render_result_detail(self.controller.state, candidate)

    async def action_toggle_pause(self) -> None:
        if self.controller.state.status is RunStatus.PAUSED:
            await self.controller.resume()
        else:
            await self.controller.pause()

    async def action_cancel_run(self) -> None:
        await self.controller.cancel()

    async def action_restart_run(self) -> None:
        await self.controller.cancel()
        cast("AlphaApp", self.app).launch_run(self.spec.restarted())

    async def action_new_run(self) -> None:
        await self.controller.cancel()
        cast("AlphaApp", self.app).show_mission()

    def action_show_help(self) -> None:
        self.app.push_screen(HelpScreen())

    def action_show_debug(self) -> None:
        self.query_one("#run-tabs", TabbedContent).active = "debug-tab"

    def action_show_overview(self) -> None:
        self.query_one("#run-tabs", TabbedContent).active = "overview-tab"

    def action_show_results(self) -> None:
        self.query_one("#run-tabs", TabbedContent).active = "results-tab"

    def action_quit_app(self) -> None:
        self.app.exit()


class AlphaApp(App[None]):
    CSS_PATH = "theme.tcss"
    TITLE = "Alpha Orchestration"
    SUB_TITLE = "Local research operations"

    def __init__(
        self,
        *,
        initial_spec: RunSpec | None = None,
        demo_delay_seconds: float = 0.18,
        artifact_root: Path = Path("artifacts/runs"),
        runtime_factory: RuntimeFactory | None = None,
        journal_factory: JournalFactory | None = None,
    ) -> None:
        super().__init__()
        self.initial_spec = initial_spec
        self.demo_delay_seconds = demo_delay_seconds
        self.artifact_root = artifact_root
        self._runtime_factory = runtime_factory
        self._journal_factory = journal_factory

    def on_mount(self) -> None:
        if self.initial_spec is None:
            self.push_screen(MissionScreen())
        else:
            self.push_screen(self._make_run_screen(self.initial_spec))

    def _make_run_screen(self, spec: RunSpec) -> RunScreen:
        runtime = (
            self._runtime_factory(spec) if self._runtime_factory is not None else DemoRuntime(self.demo_delay_seconds)
        )
        journal = (
            self._journal_factory(spec)
            if self._journal_factory is not None
            else JsonlJournal(self.artifact_root / spec.run_id / "events.jsonl")
        )
        return RunScreen(spec, runtime, journal)

    def launch_run(self, spec: RunSpec) -> None:
        self.switch_screen(self._make_run_screen(spec))

    def show_mission(self) -> None:
        self.switch_screen(MissionScreen())
