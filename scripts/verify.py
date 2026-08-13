"""Run AlphaOrchestration's deterministic, offline development gate."""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True, slots=True)
class Check:
    """One subprocess in the ordered verification gate."""

    name: str
    command: tuple[str, ...]


def build_checks(python: str = sys.executable) -> tuple[Check, ...]:
    """Return the complete gate without executing it."""

    return (
        Check("Ruff lint", (python, "-m", "ruff", "check", ".")),
        Check(
            "Strict offline test suite",
            (python, "-m", "pytest", "--strict-config", "--strict-markers", "-ra"),
        ),
        Check(
            "Deterministic execution/replay harness",
            (python, str(REPOSITORY_ROOT / "scripts" / "run_dag_harness.py")),
        ),
        Check(
            "Installed package smoke",
            (
                python,
                "-c",
                (
                    "from importlib.metadata import version; "
                    "import alpha_orchestration; "
                    "assert alpha_orchestration.__package__ == 'alpha_orchestration'; "
                    "assert version('alpha-orchestration')"
                ),
            ),
        ),
        Check("CLI smoke", (python, "-m", "alpha_orchestration", "--help")),
    )


def verification_environment(source: Mapping[str, str] | None = None) -> dict[str, str]:
    """Build a stable child environment without provider identity or GPU access."""

    environment = dict(os.environ if source is None else source)
    for name in (
        "ALPHA_ALLOW_LIVE_NETWORK",
        "ALPHA_SEC_USER_AGENT",
        "PYTHONPATH",
        "PYTEST_ADDOPTS",
    ):
        environment.pop(name, None)
    environment.update(
        {
            "ALPHA_VERIFY_OFFLINE": "1",
            "CUDA_VISIBLE_DEVICES": "",
            "HF_HUB_OFFLINE": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONHASHSEED": "0",
            "PYTHONNOUSERSITE": "1",
            "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
            "TRANSFORMERS_OFFLINE": "1",
        }
    )
    return environment


def run_checks(
    checks: Sequence[Check],
    *,
    environment: Mapping[str, str] | None = None,
) -> int:
    """Run checks in order and stop at the first failure."""

    child_environment = verification_environment(environment)
    for index, check in enumerate(checks, start=1):
        print(f"\n[{index}/{len(checks)}] {check.name}", flush=True)
        print(f"$ {shlex.join(check.command)}", flush=True)
        try:
            completed = subprocess.run(
                check.command,
                cwd=REPOSITORY_ROOT,
                env=child_environment,
                check=False,
            )
        except OSError as exc:
            print(f"FAILED: could not start {check.name}: {exc}", file=sys.stderr)
            return 127
        if completed.returncode != 0:
            print(
                f"FAILED: {check.name} exited with status {completed.returncode}",
                file=sys.stderr,
            )
            return completed.returncode
        print(f"PASSED: {check.name}", flush=True)
    print("\nOffline verification passed.", flush=True)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the repository's deterministic offline development gate."
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="print the ordered checks without running them",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    checks = build_checks()
    if args.list:
        for check in checks:
            print(f"{check.name}: {shlex.join(check.command)}")
        return 0
    return run_checks(checks)


if __name__ == "__main__":
    raise SystemExit(main())
