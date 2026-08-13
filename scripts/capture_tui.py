"""Capture a deterministic SVG of the completed TUI for visual QA."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from alpha_orchestration.adapters.demo import DemoRuntime
from alpha_orchestration.domain import RunSpec
from alpha_orchestration.journal import MemoryJournal
from alpha_orchestration.tui.app import AlphaApp, RunScreen


async def capture(output: Path, *, width: int, height: int) -> Path:
    app = AlphaApp(
        initial_spec=RunSpec(run_id="run-visual-qa"),
        runtime_factory=lambda _: DemoRuntime(0),
        journal_factory=lambda _: MemoryJournal(),
    )
    async with app.run_test(size=(width, height)) as pilot:
        await pilot.pause()
        screen = app.screen
        if not isinstance(screen, RunScreen):
            raise RuntimeError("run screen did not mount")
        await asyncio.wait_for(screen.completed.wait(), timeout=3)
        await pilot.pause()
        output.parent.mkdir(parents=True, exist_ok=True)
        saved = app.save_screenshot(output.name, str(output.parent))
    return Path(saved).resolve()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path, nargs="?", default=Path("artifacts/tui-demo.svg"))
    parser.add_argument("--width", type=int, default=150)
    parser.add_argument("--height", type=int, default=44)
    args = parser.parse_args()
    print(asyncio.run(capture(args.output, width=args.width, height=args.height)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
