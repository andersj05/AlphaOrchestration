"""Command-line entry point."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Sequence
from pathlib import Path

from alpha_orchestration.adapters.demo import DemoRuntime
from alpha_orchestration.controller import RunController
from alpha_orchestration.domain import ResearchDepth, RunEvent, RunSpec
from alpha_orchestration.journal import JsonlJournal, replay
from alpha_orchestration.tui.app import AlphaApp


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="alpha-orchestrate",
        description="Local-first public-equity research orchestration prototype",
    )
    parser.add_argument("--demo", action="store_true", help="launch a synthetic run immediately")
    parser.add_argument("--plain", action="store_true", help="run the demo without the full-screen TUI")
    parser.add_argument("--sector", default="Semiconductors")
    parser.add_argument("--depth", choices=[depth.value for depth in ResearchDepth], default="standard")
    parser.add_argument("--universe-size", type=int, default=18)
    parser.add_argument("--demo-delay", type=float, default=0.18, metavar="SECONDS")
    parser.add_argument("--artifacts", type=Path, default=Path("artifacts/runs"))
    parser.add_argument("--replay", type=Path, metavar="EVENTS_JSONL")
    return parser


def _spec(args: argparse.Namespace) -> RunSpec:
    return RunSpec(
        sector=args.sector,
        depth=ResearchDepth(args.depth),
        universe_size=args.universe_size,
    )


async def _plain_demo(args: argparse.Namespace) -> int:
    spec = _spec(args)
    path = args.artifacts / spec.run_id / "events.jsonl"

    def print_event(event: RunEvent) -> None:
        agent = f" [{event.agent_id}]" if event.agent_id else ""
        print(f"{event.sequence:03d} {event.kind.value:<20}{agent} {event.message}")

    controller = RunController(
        spec,
        DemoRuntime(args.demo_delay),
        JsonlJournal(path),
        subscribers=[print_event],
    )
    state = await controller.run()
    print()
    print(f"{state.status.value.upper()}: {len(state.candidates)} research candidates")
    for candidate in sorted(state.candidates.values(), key=lambda item: item.priority_score, reverse=True):
        print(f"  {candidate.ticker:<8} {candidate.priority_score:>3}  {candidate.bucket.label}")
    print(f"Event journal: {path.resolve()}")
    return 0 if state.status.value == "complete" else 1


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
    args = build_parser().parse_args(argv)
    if args.replay is not None:
        return _print_replay(args.replay)
    if args.plain:
        return asyncio.run(_plain_demo(args))
    initial_spec = _spec(args) if args.demo else None
    app = AlphaApp(
        initial_spec=initial_spec,
        demo_delay_seconds=args.demo_delay,
        artifact_root=args.artifacts,
    )
    app.run()
    return 0
