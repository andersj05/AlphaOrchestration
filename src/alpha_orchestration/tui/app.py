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
    DataTable,
    Footer,
    Input,
    Label,
    ProgressBar,
    RichLog,
    Select,
    Static,
)

from alpha_orchestration.adapters.demo import DemoRuntime
from alpha_orchestration.controller import RunController
from alpha_orchestration.domain import (
    STAGE_ORDER,
    AgentStatus,
    Candidate,
    CandidateBucket,
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


class HelpScreen(ModalScreen[None]):
    BINDINGS = [Binding("escape", "dismiss", "Close", show=False)]

    def compose(self) -> ComposeResult:
        with Container(id="help-dialog"):
            yield Static("ALPHA / CONTROLS", id="help-title")
            yield Static(
                "[b]Space[/b]  Pause or resume the event stream\n"
                "[b]Enter[/b]  Inspect the selected candidate\n"
                "[b]C[/b]      Cancel and drain the run\n"
                "[b]R[/b]      Restart with the same mandate\n"
                "[b]N[/b]      Create a new mandate\n"
                "[b]Q[/b]      Quit\n\n"
                "The demo is synthetic, offline, and produces research-priority "
                "candidates—not investment recommendations.",
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
                "[b]Deploy a bounded research swarm.[/b]\n"
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
                yield Static("[dim]LOGICAL AGENTS[/dim]\n[b]8[/b]", classes="mission-stat")
                yield Static("[dim]ACTIVE SLOTS[/dim]\n[b]4[/b]", classes="mission-stat")
                yield Static("[dim]DATA MODE[/dim]\n[b]OFFLINE[/b]", classes="mission-stat")
            yield Static(
                "[b #E3B341]SYNTHETIC DEMO[/b #E3B341]  No network, credentials, "
                "or real-company claims. Live SEC + market-data execution is the next adapter step.",
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

    def compose(self) -> ComposeResult:
        yield Static("", id="top-banner", markup=True)
        yield ProgressBar(total=100, show_eta=False, show_percentage=False, id="run-progress")
        with Horizontal(id="metric-row"):
            yield Static("[dim]PROGRESS[/dim]\n[b]0%[/b]", id="metric-progress", classes="metric")
            yield Static("[dim]AGENTS[/dim]\n[b]0 / 8[/b]", id="metric-agents", classes="metric")
            yield Static("[dim]EVIDENCE[/dim]\n[b]0[/b]", id="metric-evidence", classes="metric")
            yield Static("[dim]CANDIDATES[/dim]\n[b]0[/b]", id="metric-candidates", classes="metric")
            yield Static("[dim]ENGINE SHAPE[/dim]\n[b]8 → 4[/b]", id="metric-engine", classes="metric")
        with Horizontal(id="main-grid"):
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

        self._render_state(self.controller.state)
        self.run_worker(self._drive(), name=f"run:{self.spec.run_id}", exclusive=True)

    async def _drive(self) -> None:
        try:
            await self.controller.run()
        finally:
            self.completed.set()

    async def on_unmount(self) -> None:
        await self.controller.cancel()

    def _on_event(self, event: RunEvent) -> None:
        color = EVENT_COLORS.get(event.kind, "#8293A7")
        agent = f"  [#54D6FF]{escape(event.agent_id)}[/#54D6FF]" if event.agent_id else ""
        line = Text.from_markup(
            f"[dim]{event.sequence:03d}[/dim]  [{color}]{event.kind.value.upper():<20}[/{color}]"
            f"{agent}  {escape(event.message)}"
        )
        self.query_one("#activity-log", RichLog).write(line)
        self._render_state(self.controller.state)

    def _render_state(self, state: RunState) -> None:
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
        self.query_one("#top-banner", Static).update(
            f"[b]ALPHA / ORCHESTRATION[/b]   [#E3B341]SYNTHETIC DEMO[/#E3B341]   "
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
        self.query_one("#metric-engine", Static).update(
            f"[dim]LOGICAL → ACTIVE[/dim]\n[b]{state.spec.agent_budget} → {state.spec.active_slots}[/b]"
        )
        self._render_pipeline(state)
        self._render_agents(state)
        self._render_evidence(state)
        self._render_candidates(state)

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
        if event.data_table.id != "candidate-table":
            return
        candidate_id = str(event.row_key.value)
        candidate = self.controller.state.candidates.get(candidate_id)
        if candidate is not None:
            self._selected_candidate_id = candidate_id
            self._render_candidate_detail(candidate)

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
