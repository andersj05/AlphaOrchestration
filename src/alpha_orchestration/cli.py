"""Command-line entry point."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Sequence
from pathlib import Path

from alpha_orchestration.adapters.demo import DemoRuntime
from alpha_orchestration.controller import RunController
from alpha_orchestration.data.live import normalize_live_tickers
from alpha_orchestration.domain import ResearchDepth, RunEvent, RunSpec
from alpha_orchestration.journal import JsonlJournal, replay
from alpha_orchestration.live_runtime import build_live_runtime, live_environment_readiness
from alpha_orchestration.ports import OrchestratorRuntime
from alpha_orchestration.tui.app import AlphaApp, LiveReadiness


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="alpha-orchestrate",
        description="Local-first public-equity research orchestration prototype",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  alpha-orchestrate --demo\n"
            "  alpha-orchestrate --live --tickers AAPL,MSFT,NVDA\n"
            "  alpha-orchestrate --live --tickers AAPL --plain"
        ),
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--demo", action="store_true", help="launch the synthetic fixture")
    mode.add_argument("--live", action="store_true", help="launch a live SEC/yfinance run")
    parser.add_argument("--plain", action="store_true", help="run without the full-screen TUI")
    parser.add_argument("--tickers", help="comma-separated live ticker universe (1-8)")
    parser.add_argument("--sector", default="Semiconductors")
    parser.add_argument("--depth", choices=[depth.value for depth in ResearchDepth], default="standard")
    parser.add_argument("--universe-size", type=int, default=18)
    parser.add_argument("--active-slots", type=int, default=4)
    parser.add_argument("--provider-slots", type=int, default=4)
    parser.add_argument("--demo-delay", type=float, default=0.18, metavar="SECONDS")
    parser.add_argument("--artifacts", type=Path, default=Path("artifacts/runs"))
    parser.add_argument(
        "--live-cache",
        type=Path,
        default=Path("artifacts/live-cache"),
        help="content-addressed provider cache",
    )
    parser.add_argument("--replay", type=Path, metavar="EVENTS_JSONL")
    return parser


def _spec(args: argparse.Namespace, tickers: tuple[str, ...] = ()) -> RunSpec:
    if args.live:
        issuer_count = len(tickers)
        return RunSpec(
            sector=args.sector,
            depth=ResearchDepth(args.depth),
            universe_size=issuer_count,
            agent_budget=issuer_count,
            active_slots=min(args.active_slots, issuer_count),
            mode="live",
        )
    return RunSpec(
        sector=args.sector,
        depth=ResearchDepth(args.depth),
        universe_size=args.universe_size,
        active_slots=args.active_slots,
    )


async def _plain_run(args: argparse.Namespace, spec: RunSpec, runtime: OrchestratorRuntime) -> int:
    path = args.artifacts / spec.run_id / "events.jsonl"

    def print_event(event: RunEvent) -> None:
        agent = f" [{event.agent_id}]" if event.agent_id else ""
        print(f"{event.sequence:03d} {event.kind.value:<20}{agent} {event.message}")

    controller = RunController(
        spec,
        runtime,
        JsonlJournal(path),
        subscribers=[print_event],
    )
    state = await controller.run()
    print()
    print(f"{state.status.value.upper()}: {len(state.candidates)} research candidates")
    for candidate in sorted(state.candidates.values(), key=lambda item: item.priority_score, reverse=True):
        print(f"  {candidate.ticker:<8} {candidate.priority_score:>3}  {candidate.bucket.label}")
    print(f"Event journal: {path.resolve()}")
    if state.failure:
        print(f"Failure: {state.failure}")
    return 0 if state.status.value == "complete" else 1


def _tickers(args: argparse.Namespace, parser: argparse.ArgumentParser) -> tuple[str, ...]:
    if args.tickers is not None and not args.live:
        parser.error("--tickers requires --live")
    if args.live and args.tickers is None:
        parser.error("--live requires --tickers with 1-8 comma-separated symbols")
    if args.tickers is None:
        return ()
    raw = tuple(item for item in args.tickers.split(",") if item.strip())
    try:
        return normalize_live_tickers(raw)
    except ValueError as exc:
        parser.error(str(exc))
    raise AssertionError("argparse.error always exits")


def _live_runtime(args: argparse.Namespace, tickers: tuple[str, ...]) -> OrchestratorRuntime:
    return build_live_runtime(
        tickers,
        cache_root=args.live_cache,
        provider_slots=args.provider_slots,
    )


def _print_replay(path: Path) -> int:
    state = replay(path)
    print(f"Run: {state.spec.run_id}")
    print(f"Sector: {state.spec.sector}")
    print(f"Status: {state.status.value}")
    print(f"Events: {state.last_sequence + 1}")
    print(f"Evidence: {len(state.evidence)}")
    print(f"Candidates: {len(state.candidates)}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.replay is not None:
        return _print_replay(args.replay)
    if not 1 <= args.provider_slots <= 8:
        parser.error("--provider-slots must be between 1 and 8")
    tickers = _tickers(args, parser)
    try:
        spec = _spec(args, tickers)
    except ValueError as exc:
        parser.error(str(exc))
    if args.plain:
        if args.live:
            try:
                runtime = _live_runtime(args, tickers)
            except (RuntimeError, ValueError) as exc:
                parser.error(str(exc))
        else:
            runtime = DemoRuntime(args.demo_delay)
        return asyncio.run(_plain_run(args, spec, runtime))

    readiness = live_environment_readiness()
    if args.live and not all(readiness.values()):
        missing = []
        if not readiness["sec_identity_configured"]:
            missing.append("ALPHA_SEC_USER_AGENT")
        if not readiness["yfinance_installed"]:
            missing.append('the optional data extra (pip install -e ".[data]")')
        parser.error("live mode is not ready; configure " + " and ".join(missing))

    def live_factory(_spec_value: RunSpec, symbols: tuple[str, ...]) -> OrchestratorRuntime:
        return _live_runtime(args, symbols)

    initial_spec = spec if args.demo or args.live else None
    app = AlphaApp(
        initial_spec=initial_spec,
        demo_delay_seconds=args.demo_delay,
        artifact_root=args.artifacts,
        live_runtime_factory=live_factory,
        live_readiness=LiveReadiness.from_mapping(readiness),
        initial_tickers=tickers,
    )
    app.run()
    return 0
