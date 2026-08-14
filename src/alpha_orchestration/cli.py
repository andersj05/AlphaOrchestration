"""Command-line entry point."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Sequence
from pathlib import Path

from alpha_orchestration.adapters.demo import DemoRuntime
from alpha_orchestration.automatic_runtime import build_automatic_live_runtime
from alpha_orchestration.controller import RunController
from alpha_orchestration.data.live import normalize_live_tickers
from alpha_orchestration.data.universe import UniversePolicy
from alpha_orchestration.domain import ResearchDepth, RunEvent, RunSpec
from alpha_orchestration.journal import JsonlJournal, replay
from alpha_orchestration.live_runtime import build_live_runtime, live_environment_readiness
from alpha_orchestration.ports import OrchestratorRuntime
from alpha_orchestration.tui.app import AUTOMATIC_LIVE_MODE, AlphaApp, LiveReadiness

_AUTOMATIC_SECTOR = "U.S. large, liquid equities"
_RULE_BASED_LABEL = "RULE-BASED SCREEN (NO MODEL DILIGENCE)"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="alpha-orchestrate",
        description=(
            "Local-first public-equity research orchestration prototype. With no arguments, "
            "launch the fail-closed automatic live screen."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python -m alpha_orchestration\n"
            "  python -m alpha_orchestration --plain\n"
            "  alpha-orchestrate --live --tickers AAPL,MSFT,NVDA\n"
            "  alpha-orchestrate --demo"
        ),
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--automatic",
        action="store_true",
        help="launch the broad live screen explicitly (the no-argument default)",
    )
    mode.add_argument("--live", action="store_true", help="launch a bounded manual SEC/yfinance run")
    mode.add_argument("--demo", action="store_true", help="launch the offline synthetic fixture")
    parser.add_argument("--plain", action="store_true", help="run without the full-screen TUI")
    parser.add_argument("--tickers", help="comma-separated manual live ticker universe (1-8)")
    parser.add_argument("--sector", help="override the mandate label for manual or fixture runs")
    parser.add_argument("--depth", choices=[depth.value for depth in ResearchDepth], default="standard")
    parser.add_argument("--universe-size", type=int, default=18, help="synthetic fixture universe size")
    parser.add_argument(
        "--active-slots",
        type=int,
        metavar="N",
        help="engine concurrency (default: 8 automatic, 4 manual/fixture)",
    )
    parser.add_argument(
        "--provider-slots",
        type=int,
        metavar="N",
        help="live provider concurrency (default: 8 automatic, 4 manual)",
    )
    parser.add_argument("--demo-delay", type=float, default=0.18, metavar="SECONDS")
    parser.add_argument("--artifacts", type=Path, default=Path("artifacts/runs"))
    parser.add_argument(
        "--live-cache",
        type=Path,
        default=Path("artifacts/live-cache"),
        help="content-addressed provider cache",
    )
    automatic = parser.add_argument_group("automatic live expert options")
    automatic.add_argument("--target-size", type=int, default=300, metavar="N")
    automatic.add_argument("--minimum-universe-size", type=int, default=200, metavar="N")
    automatic.add_argument(
        "--max-inspected",
        type=int,
        default=1_000,
        metavar="N",
        help="maximum source-ranked rows inspected; uninspected matches are not exclusions",
    )
    automatic.add_argument("--collection-batch-size", type=int, default=25, metavar="N")
    automatic.add_argument(
        "--candidate-limit",
        type=int,
        default=25,
        metavar="N",
        help="maximum surfaced research-priority candidates",
    )
    automatic.add_argument(
        "--diligence-limit",
        type=int,
        metavar="N",
        help="cap reserved for an injected optional diligence model (default: min(8, candidate limit))",
    )
    automatic.add_argument(
        "--diligence-slots",
        type=int,
        default=4,
        metavar="N",
        help="concurrency reserved for an injected optional diligence model",
    )
    parser.add_argument("--replay", type=Path, metavar="EVENTS_JSONL")
    return parser


def _is_automatic(args: argparse.Namespace) -> bool:
    return not args.demo and not args.live


def _provider_slots(args: argparse.Namespace) -> int:
    if args.provider_slots is not None:
        return args.provider_slots
    return 8 if _is_automatic(args) else 4


def _automatic_policy(args: argparse.Namespace) -> UniversePolicy:
    return UniversePolicy(
        target_size=args.target_size,
        minimum_size=args.minimum_universe_size,
        max_screened=args.max_inspected,
    )

def _spec(args: argparse.Namespace, tickers: tuple[str, ...] = ()) -> RunSpec:
    if args.live:
        issuer_count = len(tickers)
        requested_slots = args.active_slots if args.active_slots is not None else 4
        return RunSpec(
            sector=args.sector or "User-selected live equities",
            depth=ResearchDepth(args.depth),
            universe_size=issuer_count,
            agent_budget=issuer_count,
            active_slots=min(requested_slots, issuer_count),
            mode="live",
        )
    if _is_automatic(args):
        policy = _automatic_policy(args)
        return RunSpec(
            sector=args.sector or _AUTOMATIC_SECTOR,
            depth=ResearchDepth(args.depth),
            universe_size=policy.target_size,
            agent_budget=8,
            active_slots=args.active_slots if args.active_slots is not None else 8,
            mode=AUTOMATIC_LIVE_MODE,
        )
    return RunSpec(
        sector=args.sector or "Semiconductors",
        depth=ResearchDepth(args.depth),
        universe_size=args.universe_size,
        active_slots=args.active_slots if args.active_slots is not None else 4,
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
        provider_slots=_provider_slots(args),
    )


def _automatic_runtime(args: argparse.Namespace) -> OrchestratorRuntime:
    diligence_limit = (
        args.diligence_limit
        if args.diligence_limit is not None
        else min(8, args.candidate_limit)
    )
    return build_automatic_live_runtime(
        policy=_automatic_policy(args),
        cache_root=args.live_cache,
        provider_slots=_provider_slots(args),
        collection_batch_size=args.collection_batch_size,
        candidate_limit=args.candidate_limit,
        diligence_limit=diligence_limit,
        diligence_slots=args.diligence_slots,
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


def _readiness_blocker(readiness: dict[str, bool]) -> str | None:
    if readiness.get("sec_identity_configured") is True and readiness.get("yfinance_installed") is True:
        return None
    return "Automatic live prerequisites are incomplete; no research has started"


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.replay is not None:
        return _print_replay(args.replay)
    provider_slots = _provider_slots(args)
    if not 1 <= provider_slots <= 8:
        parser.error("--provider-slots must be between 1 and 8")
    tickers = _tickers(args, parser)
    try:
        spec = _spec(args, tickers)
    except ValueError as exc:
        parser.error(str(exc))

    if args.plain:
        try:
            if args.live:
                runtime = _live_runtime(args, tickers)
            elif _is_automatic(args):
                runtime = _automatic_runtime(args)
            else:
                runtime = DemoRuntime(args.demo_delay)
        except (RuntimeError, ValueError) as exc:
            parser.error(str(exc))
        return asyncio.run(_plain_run(args, spec, runtime))

    readiness = live_environment_readiness()
    if args.live and not all(readiness.values()):
        missing = []
        if not readiness["sec_identity_configured"]:
            missing.append("ALPHA_SEC_USER_AGENT")
        if not readiness["yfinance_installed"]:
            missing.append('the optional data extra (pip install -e ".[data]")')
        parser.error("live mode is not ready; configure " + " and ".join(missing))

    live_readiness = LiveReadiness.from_mapping(readiness)
    automatic_readiness = LiveReadiness.from_mapping(
        readiness,
        analysis_label=_RULE_BASED_LABEL,
        blocker=_readiness_blocker(readiness),
    )

    def live_factory(_spec_value: RunSpec, symbols: tuple[str, ...]) -> OrchestratorRuntime:
        return _live_runtime(args, symbols)

    def automatic_factory(_spec_value: RunSpec) -> OrchestratorRuntime:
        return _automatic_runtime(args)

    automatic = _is_automatic(args)
    initial_spec = spec if args.demo or args.live or automatic_readiness.ready else None
    app = AlphaApp(
        initial_spec=initial_spec,
        demo_delay_seconds=args.demo_delay,
        artifact_root=args.artifacts,
        live_runtime_factory=live_factory,
        automatic_runtime_factory=automatic_factory,
        live_readiness=live_readiness,
        automatic_readiness=automatic_readiness,
        initial_tickers=tickers,
        startup_mode=AUTOMATIC_LIVE_MODE if automatic else "mission",
    )
    app.run()
    return 0
