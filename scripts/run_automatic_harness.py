"""Run the hermetic 300-issuer automatic-universe acceptance harness."""

from __future__ import annotations

import argparse
import asyncio
import json
import tempfile
from collections.abc import Sequence
from pathlib import Path

from alpha_orchestration.automatic_harness import run_harness


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Execute and replay the deterministic 300-issuer scale harness."
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="New JSONL path to retain; it must not already exist.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    with tempfile.TemporaryDirectory(prefix="alpha-automatic-harness-") as directory:
        root = Path(directory)
        journal = args.output if args.output is not None else root / "events.jsonl"
        summary = asyncio.run(run_harness(journal, cache_root=root / "cache"))
        if args.output is None:
            summary["journal"] = "temporary (removed after verification)"
        print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
