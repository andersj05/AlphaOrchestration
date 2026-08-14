from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

CHECK_PATH = Path(__file__).resolve().parents[1] / "scripts" / "check_project_memory.py"
SPEC = importlib.util.spec_from_file_location("alpha_check_project_memory", CHECK_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"could not load project-memory checker from {CHECK_PATH}")
memory_check = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = memory_check
SPEC.loader.exec_module(memory_check)


def _write_protocol(root: Path) -> None:
    for relative_path, headings in memory_check.REQUIRED_HEADINGS.items():
        path = root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        body = "\n\n".join(headings)
        if path.name in {"current-status.md", "backlog.md"}:
            body += "\n\nLast updated: 2026-08-13"
        if path.name == "decisions.md":
            body += "\n\nEntries are append-only.\n\n### 2026-08-13 — Test decision"
        if path.name == "README.md":
            body += "\n\n" + "\n".join(
                f"- [{target}]({target})" for target in memory_check.INDEX_LINKS
            )
        path.write_text(body + "\n", encoding="utf-8")
    (root / "AGENTS.md").write_text(
        ".agents/memory/README.md .agents/memory/current-status.md End of slice\n",
        encoding="utf-8",
    )


def test_repository_project_memory_is_complete() -> None:
    assert memory_check.validate_project_memory() == []


def test_checker_reports_missing_heading_and_broken_link(tmp_path: Path) -> None:
    _write_protocol(tmp_path)
    status = tmp_path / ".agents" / "memory" / "current-status.md"
    status.write_text(
        status.read_text(encoding="utf-8").replace("## Active milestone", "## Removed"),
        encoding="utf-8",
    )
    index = tmp_path / ".agents" / "memory" / "README.md"
    index.write_text(index.read_text(encoding="utf-8") + "[missing](missing.md)\n", encoding="utf-8")

    errors = memory_check.validate_project_memory(tmp_path)

    assert any("missing heading '## Active milestone'" in error for error in errors)
    assert any("broken local link" in error for error in errors)
