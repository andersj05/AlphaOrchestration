from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERIFY_PATH = ROOT / "scripts" / "verify.py"
SPEC = importlib.util.spec_from_file_location("alpha_verify_process_guard", VERIFY_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"could not load verifier from {VERIFY_PATH}")
verify = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = verify
SPEC.loader.exec_module(verify)


def test_non_pytest_network_guard_blocks_connect_ex_sendto_and_dns() -> None:
    completed = subprocess.run(
        (sys.executable, str(ROOT / "scripts" / "check_offline_network.py")),
        cwd=ROOT,
        env=verify.verification_environment({}),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert "Offline network isolation passed" in completed.stdout
