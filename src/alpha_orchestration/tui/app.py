"""Mission setup and live run screens for AlphaOrchestration."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
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
LiveRuntimeFactory = Callable[[RunSpec, tuple[str, ...]], OrchestratorRuntime]
AutomaticRuntimeFactory = Callable[[RunSpec], OrchestratorRuntime]

AUTOMATIC_LIVE_MODE = "automatic_live"
LIVE_MODES = frozenset({"live", AUTOMATIC_LIVE_MODE})


def _is_live_mode(mode: str) -> bool:
    return mode in LIVE_MODES


@dataclass(frozen=True, slots=True)
class LiveReadiness:
    """Non-secret, non-network preflight facts shown before a live run."""

    sec_identity_configured: bool = False
    yfinance_installed: bool = False
    runtime_available: bool = False
    analysis_label: str = "DETERMINISTIC RULES"
    blocker: str | None = None

    @property
    def ready(self) -> bool:
        return (
            self.sec_identity_configured and self.yfinance_installed and self.runtime_available and self.blocker is None
        )

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, object],
        *,
        runtime_available: bool = True,
        analysis_label: str = "DETERMINISTIC RULES",
        blocker: str | None = None,
    ) -> LiveReadiness:
        return cls(
            sec_identity_configured=value.get("sec_identity_configured") is True,
            yfinance_installed=value.get("yfinance_installed") is True,
            runtime_available=runtime_available,
            analysis_label=analysis_label,
            blocker=blocker,
        )


_TICKER_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9.-]{0,19}$")


def normalize_tickers(value: str) -> tuple[str, ...]:
    """Normalize a comma-separated public-equity universe, preserving input order."""

    raw = tuple(item.strip().upper() for item in value.split(",") if item.strip())
    tickers = tuple(dict.fromkeys(raw))
    if not 1 <= len(tickers) <= 8:
        raise ValueError("live runs require between 1 and 8 unique tickers")
    invalid = tuple(ticker for ticker in tickers if _TICKER_PATTERN.fullmatch(ticker) is None)
    if invalid:
        raise ValueError(f"invalid ticker format: {', '.join(invalid)}")
    return tickers


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

    def __init__(self, mode: str) -> None:
        super().__init__()
        self.mode = mode

    def compose(self) -> ComposeResult:
        if _is_live_mode(self.mode):
            posture = (
                "This run requests live SEC and market data. CONFIGURED means a local prerequisite "
                "exists; provider access is verified only by timestamped run evidence. Partial "
                "source failures remain visible in Results."
            )
        else:
            posture = (
                "This fixture uses offline synthetic records, has no live-source readiness, and "
                "makes no claim about real companies."
            )
        with Container(id="help-dialog"):
            yield Static("ALPHA / CONTROLS", id="help-title")
            yield Static(
                "[b]Space[/b]  Pause or resume the event stream\n"
                "[b]O / V / D[/b]  Overview / results / debug journal\n"
                "[b]Enter[/b]  Inspect the selected candidate\n"
                "[b]C[/b]      Stop outstanding work\n"
                "[b]R[/b]      Restart with the same mandate\n"
                "[b]N[/b]      Create a new mandate\n"
                "[b]Q[/b]      Quit\n\n"
                "Results rank the next diligence step, not expected return, and do not "
                f"issue investment recommendations.\n\n{posture}",
                markup=True,
            )
            yield Button("CLOSE", id="close-help", variant="primary")

    def action_dismiss(self) -> None:
        self.dismiss(None)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "close-help":
            self.dismiss(None)

class AutomaticPreflightScreen(Screen[None]):
    """Fail-closed startup state for the zero-argument automatic workflow."""

    BINDINGS = [
        Binding("e", "expert_setup", "Expert setup"),
        Binding("q", "quit_app", "Quit"),
    ]

    def compose(self) -> ComposeResult:
        with Container(id="automatic-preflight-shell"), VerticalScroll(id="automatic-preflight-card"):
            yield Static("ALPHA / AUTOMATIC RESEARCH", id="automatic-preflight-brand")
            yield Static(
                "[b #FF6B6B]PREFLIGHT BLOCKED — NO RESEARCH STARTED[/b #FF6B6B]\n"
                "The default broad-market workflow requires live SEC identity, market-data support, "
                "and the automatic runtime. Missing prerequisites never fall back to fixtures.",
                id="automatic-preflight-copy",
                markup=True,
            )
            yield Static("", id="automatic-preflight-status", markup=True)
            yield Static(
                "Resolve the items above, then restart [b]python -m alpha_orchestration[/b].\n"
                "Press [b]E[/b] for manual ticker or offline fixture controls.",
                id="automatic-preflight-next",
                markup=True,
            )
            with Horizontal(id="automatic-preflight-actions"):
                yield Button("EXPERT SETUP", id="expert-setup", variant="primary")
                yield Button("QUIT", id="quit", variant="default")

    def on_mount(self) -> None:
        readiness = cast("AlphaApp", self.app).automatic_readiness

        def status(ready: bool) -> str:
            return "[#7EE787]READY[/#7EE787]" if ready else "[#FF6B6B]MISSING[/#FF6B6B]"

        blocker = f"\n\n[b]BLOCKER[/b]  {escape(readiness.blocker)}" if readiness.blocker else ""
        self.query_one("#automatic-preflight-status", Static).update(
            f"[b]SEC IDENTITY[/b]       {status(readiness.sec_identity_configured)}  "
            "ALPHA_SEC_USER_AGENT (key name only)\n"
            f"[b]MARKET PACKAGE[/b]     {status(readiness.yfinance_installed)}  "
            'install the optional data extra with pip install -e ".[data]"\n'
            f"[b]AUTOMATIC RUNTIME[/b]  {status(readiness.runtime_available)}"
            f"{blocker}"
        )

    def action_expert_setup(self) -> None:
        cast("AlphaApp", self.app).show_mission()

    def action_quit_app(self) -> None:
        self.app.exit()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "expert-setup":
            self.action_expert_setup()
        elif event.button.id == "quit":
            self.action_quit_app()

class MissionScreen(Screen[None]):
    BINDINGS = [
        Binding("q", "quit_app", "Quit"),
        Binding("enter", "start_run", "Launch", show=False),
    ]

    def __init__(
        self,
        initial_spec: RunSpec | None = None,
        initial_tickers: tuple[str, ...] = (),
    ) -> None:
        super().__init__()
        self.initial_spec = initial_spec or RunSpec()
        self.initial_tickers = initial_tickers
        self._last_mode = "live" if self.initial_spec.mode == "live" else "synthetic_demo"

    def compose(self) -> ComposeResult:
        initial_mode = "live" if self.initial_spec.mode == "live" else "synthetic_demo"
        initial_universe = (
            ", ".join(self.initial_tickers) if initial_mode == "live" else str(self.initial_spec.universe_size)
        )

        with Container(id="mission-shell"), VerticalScroll(id="mission-card"):
            yield Static("ALPHA / ORCHESTRATION", id="mission-brand")
            yield Static(
                "LOCAL-FIRST RESEARCH OPERATIONS  |  CHOOSE DATA POSTURE",
                id="mission-kicker",
            )
            yield Static(
                "[b]Run a bounded research workflow.[/b]\n"
                "Map a sector, collect evidence, challenge the screen, and hand "
                "a small candidate funnel to a human analyst.",
                id="mission-copy",
                markup=True,
            )
            yield Label("DATA MODE", classes="field-label")
            yield Select(
                [
                    ("Fixture - offline synthetic", "synthetic_demo"),
                    ("Live - SEC + market data", "live"),
                ],
                value=initial_mode,
                allow_blank=False,
                id="mode-select",
            )
            yield Label("SECTOR / THEME", classes="field-label")
            yield Input(value=self.initial_spec.sector, id="sector-input")
            with Horizontal(classes="field-row"):
                with Vertical(classes="field-block"):
                    yield Label("RESEARCH DEPTH", classes="field-label")
                    yield Select(
                        [("Quick scan", "quick"), ("Standard", "standard"), ("Deep", "deep")],
                        value=self.initial_spec.depth.value,
                        allow_blank=False,
                        id="depth-select",
                    )
                with Vertical(classes="field-block"):
                    yield Label("UNIVERSE SIZE", id="universe-label", classes="field-label")
                    yield Input(value=initial_universe, id="universe-input")
            with Horizontal(id="mission-stats"):
                yield Static("", id="mission-universe-stat", classes="mission-stat")
                yield Static("[dim]SLOT LIMIT[/dim]\n[b]4 MAX[/b]", classes="mission-stat")
                yield Static("", id="mission-mode-stat", classes="mission-stat")
            yield Static(
                "",
                id="readiness-panel",
                markup=True,
            )
            with Horizontal(id="mission-actions"):
                yield Button("LAUNCH FIXTURE RUN", id="launch-run", variant="primary")
                yield Button("QUIT", id="quit", variant="default")

    def on_mount(self) -> None:
        self._sync_mode(initial=True)
        self.query_one("#sector-input", Input).focus()

    def on_select_changed(self, event: Select.Changed) -> None:
        if event.select.id == "mode-select":
            self._sync_mode()

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id == "universe-input":
            self._sync_mode(initial=True)

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
        mode = str(self.query_one("#mode-select", Select).value)
        tickers: tuple[str, ...] = ()
        try:
            depth = ResearchDepth(str(depth_select.value))
            if mode == "live":
                readiness = cast("AlphaApp", self.app).live_readiness
                if not readiness.ready:
                    raise ValueError(readiness.blocker or "live prerequisites are not configured")
                tickers = normalize_tickers(raw_universe)
                agent_budget = min(8, max(2, len(tickers) + 1))
                spec = RunSpec(
                    sector=sector,
                    depth=depth,
                    universe_size=len(tickers),
                    agent_budget=agent_budget,
                    active_slots=min(4, agent_budget),
                    mode="live",
                )
            else:
                spec = RunSpec(
                    sector=sector,
                    depth=depth,
                    universe_size=int(raw_universe),
                    mode="synthetic_demo",
                )
        except (TypeError, ValueError) as exc:
            self.notify(str(exc), severity="error", timeout=4)
            return
        try:
            cast("AlphaApp", self.app).launch_run(spec, tickers=tickers)
        except RuntimeError as exc:
            self.notify(str(exc), severity="error", timeout=6)

    def _sync_mode(self, *, initial: bool = False) -> None:
        mode = str(self.query_one("#mode-select", Select).value)
        live = mode == "live"
        universe_input = self.query_one("#universe-input", Input)
        if not initial and mode != self._last_mode:
            if live:
                universe_input.value = ", ".join(self.initial_tickers) or "AAPL, MSFT, NVDA"
            else:
                default_size = self.initial_spec.universe_size if self.initial_spec.mode != "live" else 18
                universe_input.value = str(default_size)
            self._last_mode = mode
        launch = self.query_one("#launch-run", Button)
        if live:
            readiness = cast("AlphaApp", self.app).live_readiness
            self.query_one("#universe-label", Label).update("TICKERS | 1-8, COMMA-SEPARATED")
            try:
                universe_value = f"{len(normalize_tickers(universe_input.value))} TICKERS"
            except ValueError:
                universe_value = "CHECK INPUT"
            sec = "CONFIGURED" if readiness.sec_identity_configured else "MISSING IDENTITY"
            market = "INSTALLED" if readiness.yfinance_installed else "MISSING PACKAGE"
            runtime = "AVAILABLE" if readiness.runtime_available else "UNAVAILABLE"
            blocker = f"\n[#FF6B6B]{escape(readiness.blocker)}[/#FF6B6B]" if readiness.blocker else ""
            self.query_one("#readiness-panel", Static).update(
                f"[b #54D6FF]LIVE PREFLIGHT[/b #54D6FF]  SEC: {sec}  |  MARKET: {market}  |  "
                f"RUNTIME: {runtime}\n[dim]Analysis: {escape(readiness.analysis_label)}. "
                f"Provider access is verified only during the run.[/dim]{blocker}"
            )
            self.query_one("#mission-mode-stat", Static).update("[dim]DATA MODE[/dim]\n[b #54D6FF]LIVE[/b #54D6FF]")
            launch.label = "LAUNCH LIVE RESEARCH"
            launch.disabled = not readiness.ready
        else:
            self.query_one("#universe-label", Label).update("UNIVERSE SIZE")
            universe_value = universe_input.value.strip() or "CHECK INPUT"
            self.query_one("#readiness-panel", Static).update(
                "[b #E3B341]FIXTURE / SYNTHETIC[/b #E3B341]  Offline deterministic records; "
                "no network access and no claims about real companies."
            )
            self.query_one("#mission-mode-stat", Static).update("[dim]DATA MODE[/dim]\n[b #E3B341]FIXTURE[/b #E3B341]")
            launch.label = "LAUNCH FIXTURE RUN"
            launch.disabled = False
        self.query_one("#mission-universe-stat", Static).update(
            f"[dim]REQUESTED UNIVERSE[/dim]\n[b]{escape(universe_value)}[/b]"
        )


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

            with TabPane("UNIVERSE", id="universe-tab"), Vertical(id="universe-shell"):
                with Horizontal(id="universe-controls"):
                    yield Input(placeholder="Search ticker, company, status, or rank", id="universe-search")
                yield Static(
                    "Waiting for an automatic-universe snapshot.",
                    id="universe-summary",
                    markup=True,
                )
                yield DataTable(id="universe-table")
            with TabPane("RESULTS", id="results-tab"), Vertical(id="results-shell"):
                yield Static(
                    "[b #7AA2F7]PREPARING BOUNDED RUN[/b #7AA2F7]  No candidate ranking is available yet.",
                    id="results-status",
                    markup=True,
                )
                yield Static(
                    "[dim]UNIVERSE FUNNEL[/dim]  Waiting for controller-owned coverage telemetry.",
                    id="results-funnel",
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

        universe_table = self.query_one("#universe-table", DataTable)
        universe_table.add_columns("TICKER", "COMPANY", "STATUS", "RANK")
        universe_table.cursor_type = "row"
        universe_table.zebra_stripes = True

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
        elif event.input.id == "universe-search":
            self._render_universe(self.controller.state, self._universe_funnel())

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
        mode_label = (
            "AUTOMATIC LIVE"
            if state.spec.mode == AUTOMATIC_LIVE_MODE
            else "LIVE DATA"
            if state.spec.mode == "live"
            else "FIXTURE / SYNTHETIC"
            if state.spec.mode == "synthetic_demo"
            else _readable_token(state.spec.mode)
        )
        self.query_one("#top-banner", Static).update(
            f"[b]ALPHA / ORCHESTRATION[/b]   [#E3B341]{escape(mode_label)}[/#E3B341]   "
            f"[b]{escape(state.spec.sector.upper())}[/b]   "
            f"[{status_color}]● {state.status.value.upper()}[/{status_color}]   "
            f"[dim]{escape(state.spec.run_id)}[/dim]"
        )
        self.query_one("#run-progress", ProgressBar).update(progress=state.progress, total=100)
        self.query_one("#metric-progress", Static).update(f"[dim]PROGRESS[/dim]\n[b]{state.progress}%[/b]")
        funnel = self._universe_funnel()
        if state.spec.mode == AUTOMATIC_LIVE_MODE:
            selected = self._funnel_count(funnel, "selected") if funnel is not None else None
            if selected is None and funnel is not None:
                selected = self._funnel_count(funnel, "eligible")
            screened = self._funnel_count(funnel, "screened") if funnel is not None else None
            screened_value = (
                f"{screened:,} / {selected:,}" if screened is not None and selected is not None else "PENDING"
            )
            self.query_one("#metric-agents", Static).update(
                f"[dim]LANES COMPLETE[/dim]\n[b]{state.complete_agents} / {state.spec.agent_budget}[/b]"
            )
            self.query_one("#metric-evidence", Static).update(
                f"[dim]SCREENED / SELECTED[/dim]\n[b]{screened_value}[/b]"
            )
            self.query_one("#metric-candidates", Static).update(
                f"[dim]SURFACED[/dim]\n[b]{len(state.candidates)}[/b]"
            )
        else:
            self.query_one("#metric-agents", Static).update(
                f"[dim]AGENTS[/dim]\n[b]{state.complete_agents} / {state.spec.agent_budget}[/b]"
            )
            self.query_one("#metric-evidence", Static).update(f"[dim]EVIDENCE[/dim]\n[b]{len(state.evidence)}[/b]")
            self.query_one("#metric-candidates", Static).update(
                f"[dim]CANDIDATES[/dim]\n[b]{len(state.candidates)}[/b]"
            )
        self.query_one("#metric-engine", Static).update(self._execution_metric(state))
        self._render_pipeline(state)
        self._render_agents(state)
        self._render_evidence(state)
        self._render_candidates(state)
        self._render_universe(state, funnel)
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
        if state.spec.mode == AUTOMATIC_LIVE_MODE:
            funnel = self._universe_funnel()
            if funnel is not None:
                peak = self._funnel_count(funnel, "observed_peak_analysis_tasks")
                configured = self._funnel_count(funnel, "configured_agent_lanes") or limit
                if peak is not None:
                    return f"[dim]ANALYSIS PEAK / LANES[/dim]\n[b]{peak} / {configured}[/b]"
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

    def _live_collection(self) -> Mapping[str, object] | None:
        for event in reversed(self._events):
            value = event.payload.get("live_collection")
            if isinstance(value, dict):
                return value
        return None

    def _universe_funnel(self) -> Mapping[str, object] | None:
        for event in reversed(self._events):
            value = event.payload.get("universe_funnel")
            if isinstance(value, dict):
                return value
        return None

    def _universe_rows(self, funnel: Mapping[str, object] | None) -> tuple[Mapping[str, object], ...]:
        for event in reversed(self._events):
            rows = event.payload.get("universe_rows")
            if isinstance(rows, list):
                return tuple(row for row in rows if isinstance(row, dict))
            event_funnel = event.payload.get("universe_funnel")
            if isinstance(event_funnel, dict):
                rows = event_funnel.get("universe_rows")
                if isinstance(rows, list):
                    return tuple(row for row in rows if isinstance(row, dict))
        if funnel is not None:
            rows = funnel.get("universe_rows")
            if isinstance(rows, list):
                return tuple(row for row in rows if isinstance(row, dict))
        return ()

    @staticmethod
    def _funnel_count(funnel: Mapping[str, object], key: str) -> int | None:
        value = funnel.get(key)
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            return value
        return None

    def _render_universe_funnel(self, state: RunState, funnel: Mapping[str, object] | None) -> None:
        target = state.spec.universe_size
        if funnel is None:
            self.query_one("#results-funnel", Static).update(
                f"[dim]UNIVERSE FUNNEL[/dim]  Waiting for coverage telemetry · target {target:,} selected issuers"
            )
            return

        def count(key: str, fallback: str | None = None) -> str:
            value = self._funnel_count(funnel, key)
            if value is None and fallback is not None:
                value = self._funnel_count(funnel, fallback)
            return "N/R" if value is None else f"{value:,}"

        profile = str(funnel.get("profile") or "UNSPECIFIED PROFILE").strip().upper()
        stage = str(funnel.get("stage") or "in progress").replace("_", " ").upper()
        batches_done = self._funnel_count(funnel, "batches_completed")
        batches_total = self._funnel_count(funnel, "batches_total")
        batch_text = ""
        if batches_done is not None and batches_total is not None:
            batch_text = f" · BATCHES {batches_done:,}/{batches_total:,}"
        provider_matches = self._funnel_count(funnel, "provider_matches")
        if provider_matches is None:
            provider_matches = self._funnel_count(funnel, "discovered")
        inspected = self._funnel_count(funnel, "inspected")
        uninspected = self._funnel_count(funnel, "uninspected")
        if uninspected is None and provider_matches is not None and inspected is not None:
            uninspected = max(0, provider_matches - inspected)
        uninspected_text = "N/R" if uninspected is None else f"{uninspected:,}"
        self.query_one("#results-funnel", Static).update(
            f"[dim]{escape(profile)} · PROVIDER MATCHES {count('provider_matches', 'discovered')} · "
            f"INSPECTED {count('inspected')} · SELECTED {count('selected', 'eligible')}[/dim]\n"
            f"[b]{count('screened')}[/b] screened  →  [b]{count('deep_reviewed')}[/b] deep-reviewed  →  "
            f"[b #C099FF]{count('surfaced')}[/b #C099FF] surfaced\n"
            f"[dim]STAGE {escape(stage)}{batch_text} · EXCLUDED AFTER INSPECTION {count('excluded')} · "
            f"UNINSPECTED {uninspected_text} · FAILED {count('failed')}[/dim]"
        )

    def _render_universe(self, state: RunState, funnel: Mapping[str, object] | None) -> None:
        del state
        table = self.query_one("#universe-table", DataTable)
        table.clear(columns=False)
        rows = self._universe_rows(funnel)
        query = self.query_one("#universe-search", Input).value.strip().casefold()
        visible: list[Mapping[str, object]] = []
        for row in rows:
            ticker = str(row.get("ticker") or "").strip().upper()
            company = str(row.get("company") or row.get("title") or "").strip()
            status = str(row.get("status") or "not reported").strip().replace("_", " ").upper()
            rank = row.get("rank")
            if not isinstance(rank, int) or isinstance(rank, bool):
                rank = row.get("universe_rank")
            rank_text = str(rank) if isinstance(rank, int) and not isinstance(rank, bool) else "—"
            haystack = " ".join((ticker, company, status, rank_text)).casefold()
            if query and query not in haystack:
                continue
            visible.append(row)
            table.add_row(ticker or "—", company or "—", status, rank_text, key=f"{ticker}:{len(visible)}")

        profile = "US_LARGE_LIQUID_V1"
        if funnel is not None:
            raw_profile = funnel.get("profile")
            if isinstance(raw_profile, str) and raw_profile.strip():
                profile = raw_profile.strip().upper()
        selected = self._funnel_count(funnel, "selected") if funnel is not None else None
        if selected is None and funnel is not None:
            selected = self._funnel_count(funnel, "eligible")
        screened = self._funnel_count(funnel, "screened") if funnel is not None else None
        if rows:
            selected_text = "N/R" if selected is None else f"{selected:,}"
            screened_text = "N/R" if screened is None else f"{screened:,}"
            self.query_one("#universe-summary", Static).update(
                f"[b]{escape(profile)}[/b]  ·  showing {len(visible):,} of {len(rows):,} persisted rows  ·  "
                f"selected {selected_text}  ·  screened {screened_text}"
            )
        else:
            self.query_one("#universe-summary", Static).update(
                f"[b]{escape(profile)}[/b]  ·  no row-level universe roster in the latest event snapshot. "
                "Open [b]DEBUG / JOURNAL[/b] or the persisted run artifact for exact coverage and exclusions."
            )

    def _automatic_source_summary(self, funnel: Mapping[str, object] | None) -> str:
        if funnel is None:
            return "AUTOMATIC LIVE · SOURCE POSTURE PENDING"
        posture = funnel.get("source_posture")
        parts = [str(posture).strip()[:64] if isinstance(posture, str) and posture.strip() else "LIVE SOURCES"]
        analysis_mode = funnel.get("analysis_mode")
        if isinstance(analysis_mode, str) and analysis_mode.strip():
            parts.append(analysis_mode.strip()[:32])
        as_of = funnel.get("as_of")
        if isinstance(as_of, str) and as_of.strip():
            parts.append(f"AS OF {as_of.strip()[:24]}")
        return " / ".join(parts)

    @staticmethod
    def _collection_count(collection: Mapping[str, object], key: str) -> int | None:
        value = collection.get(key)
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            return value
        return None

    def _live_source_summary(self, collection: Mapping[str, object]) -> str:
        requested = self._collection_count(collection, "requested_count")
        ready = self._collection_count(collection, "ready_count")
        if requested is None or ready is None:
            return "LIVE COLLECTION IN PROGRESS"

        posture = "LIVE PARTIAL" if collection.get("partial") is True else "LIVE COLLECTED"
        provider_counts = collection.get("provider_successes")
        if not isinstance(provider_counts, dict):
            return posture

        parts = [posture]
        for key, label in (("sec", "SEC"), ("yfinance", "MARKET")):
            successes = self._collection_count(provider_counts, key)
            if successes is not None:
                parts.append(f"{label} {successes}/{requested}")
        return " / ".join(parts)

    @staticmethod
    def _latest_live_retrieval(collection: Mapping[str, object]) -> str | None:
        timestamps: list[str] = []
        mapping = collection.get("mapping")
        if isinstance(mapping, dict):
            value = mapping.get("retrieved_at")
            if isinstance(value, str) and value.strip():
                timestamps.append(value.strip())

        issuers = collection.get("issuers")
        if isinstance(issuers, list):
            for issuer in issuers:
                if not isinstance(issuer, dict):
                    continue
                providers = issuer.get("providers")
                if not isinstance(providers, dict):
                    continue
                for provider in providers.values():
                    if not isinstance(provider, dict):
                        continue
                    value = provider.get("retrieved_at")
                    if isinstance(value, str) and value.strip():
                        timestamps.append(value.strip())
        return max(timestamps, default=None)

    @staticmethod
    def _live_issue_note(collection: Mapping[str, object]) -> str | None:
        failures = collection.get("failures")
        if not isinstance(failures, list):
            return None
        for failure in failures:
            if not isinstance(failure, dict):
                continue
            ticker = failure.get("ticker")
            provider = failure.get("provider")
            error = failure.get("error")
            labels = [value.strip() for value in (ticker, provider) if isinstance(value, str) and value.strip()]
            if isinstance(error, str) and error.strip():
                prefix = " / ".join(labels) or "provider"
                return f"{prefix}: {error.strip()}"[:160]
            if labels:
                return f"{' / '.join(labels)}: source error reported"
        return None

    def _reviewed_issuers(self) -> int | None:
        collection = self._live_collection()
        if collection is not None:
            ready = self._collection_count(collection, "ready_count")
            if ready is not None:
                return ready
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

        funnel = self._universe_funnel()
        automatic = state.spec.mode == AUTOMATIC_LIVE_MODE
        self._render_universe_funnel(state, funnel if automatic else None)
        collection = self._live_collection()
        requested = state.spec.universe_size
        if collection is not None:
            reported_requested = self._collection_count(collection, "requested_count")
            if reported_requested is not None:
                requested = reported_requested
        if automatic and funnel is not None:
            selected_count = self._funnel_count(funnel, "selected")
            if selected_count is None:
                selected_count = self._funnel_count(funnel, "eligible")
            screened = self._funnel_count(funnel, "screened")
            if selected_count is not None:
                requested = selected_count
            reviewed = screened
        else:
            reviewed = self._reviewed_issuers()
        if reviewed is not None:
            coverage_value = f"{reviewed:,} / {requested:,}"
        elif state.status in {RunStatus.IDLE, RunStatus.PLANNING}:
            coverage_value = f"PENDING / {requested:,}"
        elif state.status in {RunStatus.RUNNING, RunStatus.PAUSED, RunStatus.SYNTHESIZING}:
            coverage_value = f"IN PROGRESS / {requested:,}"
        elif state.status in {RunStatus.CANCELLED, RunStatus.FAILED}:
            coverage_value = f"PARTIAL / {requested:,}"
        else:
            coverage_value = f"N/R / {requested:,}"
        if automatic:
            coverage_label = "SCREENED / SELECTED"
        else:
            coverage_label = "USABLE / REQUESTED" if collection is not None else "REVIEWED / REQUESTED"
        self.query_one("#result-coverage", Static).update(f"[dim]{coverage_label}[/dim]\n[b]{coverage_value}[/b]")
        surfaced = len(ranked)
        if automatic and funnel is not None:
            surfaced = self._funnel_count(funnel, "surfaced") or surfaced
        self.query_one("#result-surfaced", Static).update(f"[dim]CANDIDATES SURFACED[/dim]\n[b]{surfaced:,}[/b]")
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
        if automatic:
            source_value = self._automatic_source_summary(funnel)
        elif collection is not None:
            source_value = self._live_source_summary(collection)
        elif self._synthetic_posture(state):
            source_value = "OFFLINE FIXTURE · NO LIVE READINESS"
        elif any(evidence.synthetic for evidence in state.evidence.values()):
            source_value = "MIXED SOURCES · VERIFY AS-OF"
        else:
            source_value = f"{_readable_token(state.spec.mode)} · VERIFY READINESS"
        self.query_one("#result-sources", Static).update(f"[dim]SOURCE POSTURE[/dim]\n[b]{escape(source_value)}[/b]")
        if automatic:
            status_markup = self._automatic_results_status(state, funnel, len(ranked))
        else:
            status_markup = self._results_status_markup(state, reviewed, len(ranked), collection, requested)
        self.query_one("#results-status", Static).update(status_markup)

    def _automatic_results_status(
        self,
        state: RunState,
        funnel: Mapping[str, object] | None,
        candidate_count: int,
    ) -> str:
        def count(key: str) -> str:
            if funnel is None:
                return "not reported"
            value = self._funnel_count(funnel, key)
            return "not reported" if value is None else f"{value:,}"

        selected = count("selected")
        if selected == "not reported":
            selected = count("eligible")
        provider_matches = count("provider_matches")
        if provider_matches == "not reported":
            provider_matches = count("discovered")
        coverage = (
            f"{provider_matches} provider matches; {count('inspected')} inspected; {selected} selected; "
            f"{count('screened')} screened; {count('deep_reviewed')} deep-reviewed; {count('surfaced')} surfaced"
        )
        if state.status is RunStatus.COMPLETE:
            failures = self._funnel_count(funnel, "failed") if funnel is not None else None
            heading = "AUTOMATIC SCREEN COMPLETE WITH GAPS" if failures else "AUTOMATIC SCREEN COMPLETE"
            color = "#E3B341" if failures else "#7EE787"
            excluded = count("excluded")
            retrieval = funnel.get("retrieved_at") if funnel is not None else None
            retrieval_note = (
                f" Latest retrieval {escape(retrieval[:80])}."
                if isinstance(retrieval, str) and retrieval.strip()
                else ""
            )
            return (
                f"[b {color}]{heading}[/b {color}]  {coverage}; "
                f"{excluded} excluded after inspection; {count('failed')} failed.\n"
                f"[dim]{candidate_count} persisted research candidates."
                f"{retrieval_note} Priority is triage—not expected return or an investment recommendation.[/dim]"
            )
        if state.status is RunStatus.FAILED:
            failure = escape((state.failure or "Unspecified runtime failure")[:160])
            return (
                f"[b #FF6B6B]AUTOMATIC SCREEN FAILED[/b #FF6B6B]  {failure}\n"
                f"[dim]{coverage}. Coverage is incomplete; no fixture fallback was used.[/dim]"
            )
        if state.status is RunStatus.CANCELLED:
            return (
                f"[b #E3B341]AUTOMATIC SCREEN CANCELLED[/b #E3B341]  {coverage}.\n"
                "[dim]Persisted candidates are provisional until a complete rerun.[/dim]"
            )
        if state.status is RunStatus.PAUSED:
            return f"[b #E3B341]AUTOMATIC SCREEN PAUSED[/b #E3B341]  {coverage}."
        if state.status is RunStatus.SYNTHESIZING:
            return f"[b #C099FF]SYNTHESIZING RESEARCH PRIORITIES[/b #C099FF]  {coverage}."
        if state.status is RunStatus.RUNNING:
            return (
                f"[b #54D6FF]AUTOMATIC MARKET SCREEN IN PROGRESS[/b #54D6FF]  {coverage}.\n"
                "[dim]The funnel is provisional and updates from persisted controller telemetry.[/dim]"
            )
        return "[b #7AA2F7]PREPARING AUTOMATIC LIVE SCREEN[/b #7AA2F7]  No provider request has started yet."
    def _results_status_markup(
        self,
        state: RunState,
        reviewed: int | None,
        candidate_count: int,
        collection: Mapping[str, object] | None,
        requested: int,
    ) -> str:
        if reviewed is None:
            count_label = "usable evidence count" if collection is not None else "review count"
            coverage = f"{count_label} not reported; {requested} requested"
        elif collection is not None:
            coverage = f"{reviewed} of {requested} had usable evidence"
        else:
            coverage = f"{reviewed} of {requested} reviewed"
        if state.status is RunStatus.COMPLETE and collection is not None and collection.get("partial") is True:
            failed = self._collection_count(collection, "failed_count") or 0
            candidate_summary = (
                f"{candidate_count} research candidates surfaced"
                if candidate_count
                else "no candidate brief was produced"
            )
            failed_summary = ""
            if failed:
                issuer_label = "issuer" if failed == 1 else "issuers"
                failed_summary = f"; {failed} {issuer_label} failed"
            notes = ["Rankings use available evidence only"]
            latest = self._latest_live_retrieval(collection)
            if latest is not None:
                notes.append(f"latest source retrieval {latest[:80]}")
            issue = self._live_issue_note(collection)
            if issue is not None:
                notes.append(f"first reported issue: {issue}")
            note_text = escape(". ".join(notes))
            return (
                f"[b #E3B341]PARTIAL LIVE RESULTS[/b #E3B341]  {coverage}{failed_summary}; "
                f"{candidate_summary}.\n[dim]{note_text}. This is not an investment "
                "recommendation.[/dim]"
            )

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
        collection = self._live_collection()
        if state.status is RunStatus.COMPLETE and collection is not None and collection.get("partial") is True:
            return (
                "[b #E3B341]No candidate brief was produced from the partial live collection."
                "[/b #E3B341]\n\nSome requested source evidence was unavailable. Inspect the "
                "provider issue in the Results banner and Debug / Journal before rerunning."
            )

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
                provenance = [evidence.source, evidence_id]
                if evidence.retrieved_at != "not provided":
                    provenance.append(f"retrieved {evidence.retrieved_at}")
                evidence_lines.append(f"• {escape(evidence.title)} — [dim]{escape(' · '.join(provenance))}[/dim]")
                if evidence.source_url:
                    evidence_lines.append(f"  [dim]Source URL: {escape(evidence.source_url)}[/dim]")
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
        app = cast("AlphaApp", self.app)
        app.launch_run(self.spec.restarted(), tickers=app.tickers)

    async def action_new_run(self) -> None:
        await self.controller.cancel()
        cast("AlphaApp", self.app).show_mission(self.spec)

    def action_show_help(self) -> None:
        self.app.push_screen(HelpScreen(self.spec.mode))

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
        live_runtime_factory: LiveRuntimeFactory | None = None,
        automatic_runtime_factory: AutomaticRuntimeFactory | None = None,
        live_readiness: LiveReadiness | None = None,
        automatic_readiness: LiveReadiness | None = None,
        initial_tickers: tuple[str, ...] = (),
        startup_mode: str = "mission",
        journal_factory: JournalFactory | None = None,
    ) -> None:
        super().__init__()
        self.initial_spec = initial_spec
        self.demo_delay_seconds = demo_delay_seconds
        self.artifact_root = artifact_root
        self._runtime_factory = runtime_factory
        self._live_runtime_factory = live_runtime_factory
        self._automatic_runtime_factory = automatic_runtime_factory
        self.live_readiness = live_readiness or LiveReadiness(blocker="Live runtime is not configured")
        self.automatic_readiness = automatic_readiness or self.live_readiness
        self.tickers = initial_tickers
        self.startup_mode = startup_mode
        self._journal_factory = journal_factory

    def on_mount(self) -> None:
        if self.initial_spec is None:
            if self.startup_mode == AUTOMATIC_LIVE_MODE:
                self.push_screen(AutomaticPreflightScreen())
            else:
                self.push_screen(MissionScreen(initial_tickers=self.tickers))
            return
        if self.initial_spec.mode == AUTOMATIC_LIVE_MODE and not self.automatic_readiness.ready:
            self.push_screen(AutomaticPreflightScreen())
            return
        try:
            self.push_screen(self._make_run_screen(self.initial_spec, self.tickers))
        except (RuntimeError, ValueError) as exc:
            if self.initial_spec.mode != AUTOMATIC_LIVE_MODE:
                raise
            self.automatic_readiness = replace(
                self.automatic_readiness,
                runtime_available=False,
                blocker=str(exc),
            )
            self.push_screen(AutomaticPreflightScreen())

    def _make_run_screen(self, spec: RunSpec, tickers: tuple[str, ...] = ()) -> RunScreen:
        if spec.mode == AUTOMATIC_LIVE_MODE:
            if self._automatic_runtime_factory is None:
                raise RuntimeError("Automatic live runtime is unavailable; no fixture fallback was used")
            runtime = self._automatic_runtime_factory(spec)
        elif spec.mode == "live":
            if self._live_runtime_factory is None:
                raise RuntimeError("Live runtime is not configured; no fixture fallback was used")
            runtime = self._live_runtime_factory(spec, tickers)
        else:
            runtime = (
                self._runtime_factory(spec)
                if self._runtime_factory is not None
                else DemoRuntime(self.demo_delay_seconds)
            )
        journal = (
            self._journal_factory(spec)
            if self._journal_factory is not None
            else JsonlJournal(self.artifact_root / spec.run_id / "events.jsonl")
        )
        return RunScreen(spec, runtime, journal)

    def launch_run(self, spec: RunSpec, *, tickers: tuple[str, ...] = ()) -> None:
        screen = self._make_run_screen(spec, tickers)
        self.tickers = tickers
        self.switch_screen(screen)

    def show_mission(self, spec: RunSpec | None = None) -> None:
        if spec is not None and spec.mode == AUTOMATIC_LIVE_MODE:
            spec = None
        self.switch_screen(MissionScreen(spec, self.tickers))
