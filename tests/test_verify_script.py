from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

VERIFY_PATH = Path(__file__).resolve().parents[1] / "scripts" / "verify.py"
SPEC = importlib.util.spec_from_file_location("alpha_verify_script", VERIFY_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"could not load verification script from {VERIFY_PATH}")
verify = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = verify
SPEC.loader.exec_module(verify)


def test_check_plan_covers_the_complete_offline_gate() -> None:
    checks = verify.build_checks("python-under-test")

    assert [check.name for check in checks] == [
        "Ruff lint",
        "Strict offline test suite",
        "Deterministic execution/replay harness",
        "Installed package smoke",
        "CLI smoke",
    ]
    assert checks[0].command == ("python-under-test", "-m", "ruff", "check", ".")
    assert "--strict-config" in checks[1].command
    assert "--strict-markers" in checks[1].command
    assert checks[2].command[-1].endswith("scripts/run_dag_harness.py")
    assert checks[4].command == (
        "python-under-test",
        "-m",
        "alpha_orchestration",
        "--help",
    )


def test_verification_environment_removes_ambient_provider_and_pytest_controls() -> None:
    environment = verify.verification_environment(
        {
            "ALPHA_ALLOW_LIVE_NETWORK": "1",
            "ALPHA_SEC_USER_AGENT": "must-not-leak",
            "PYTHONPATH": "/ambient/path",
            "PYTEST_ADDOPTS": "--capture=no",
            "RETAINED": "yes",
        }
    )

    assert environment["RETAINED"] == "yes"
    assert "ALPHA_ALLOW_LIVE_NETWORK" not in environment
    assert "ALPHA_SEC_USER_AGENT" not in environment
    assert "PYTHONPATH" not in environment
    assert "PYTEST_ADDOPTS" not in environment
    assert environment["ALPHA_VERIFY_OFFLINE"] == "1"
    assert environment["CUDA_VISIBLE_DEVICES"] == ""
    assert environment["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] == "1"


def test_gate_stops_at_the_first_failed_check(monkeypatch, capsys) -> None:
    calls: list[tuple[str, ...]] = []

    def fake_run(command, **kwargs):
        del kwargs
        calls.append(command)
        return subprocess.CompletedProcess(command, 9 if len(calls) == 2 else 0)

    monkeypatch.setattr(verify.subprocess, "run", fake_run)
    checks = (
        verify.Check("first", ("python", "first")),
        verify.Check("second", ("python", "second")),
        verify.Check("never", ("python", "never")),
    )

    assert verify.run_checks(checks, environment={}) == 9
    assert calls == [("python", "first"), ("python", "second")]
    assert "FAILED: second exited with status 9" in capsys.readouterr().err
