"""Validate the small, durable project-memory protocol."""

from __future__ import annotations

import re
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
MEMORY_ROOT = REPOSITORY_ROOT / ".agents" / "memory"

REQUIRED_HEADINGS: Mapping[Path, tuple[str, ...]] = {
    Path(".agents/memory/README.md"): (
        "# Project memory protocol",
        "## Startup",
        "## During work",
        "## End of slice",
        "## Content rules",
    ),
    Path(".agents/memory/current-status.md"): (
        "# Current status",
        "## Snapshot",
        "## Active milestone",
        "## Current branch model",
        "## Verification baseline",
        "## Known boundaries",
        "## Next handoff",
    ),
    Path(".agents/memory/decisions.md"): (
        "# Decisions",
        "## Protocol",
        "## Decision log",
    ),
    Path(".agents/memory/backlog.md"): (
        "# Backlog",
        "## Now",
        "## Next",
        "## Later",
    ),
    Path(".agents/memory/handoff-template.md"): (
        "# Handoff template",
        "## Scope",
        "## Files changed",
        "## Validation",
        "## Decisions and invariants",
        "## Remaining risks",
        "## Next action",
    ),
}

INDEX_LINKS = (
    "current-status.md",
    "decisions.md",
    "backlog.md",
    "handoff-template.md",
)
DATE_PATTERN = re.compile(r"^Last (?:updated|reviewed): \d{4}-\d{2}-\d{2}$", re.MULTILINE)
MARKDOWN_LINK = re.compile(r"\[[^]]+\]\(([^)]+)\)")
MAX_FILE_BYTES = 20_000


def validate_project_memory(root: Path = REPOSITORY_ROOT) -> list[str]:
    """Return protocol violations without changing the repository."""

    errors: list[str] = []
    for relative_path, headings in REQUIRED_HEADINGS.items():
        path = root / relative_path
        if not path.is_file():
            errors.append(f"missing required memory file: {relative_path.as_posix()}")
            continue
        if path.stat().st_size > MAX_FILE_BYTES:
            errors.append(f"memory file exceeds {MAX_FILE_BYTES} bytes: {relative_path.as_posix()}")
        text = path.read_text(encoding="utf-8")
        for heading in headings:
            if heading not in text.splitlines():
                errors.append(f"missing heading {heading!r} in {relative_path.as_posix()}")
        errors.extend(_broken_local_links(path, text, root))

    index_path = root / ".agents" / "memory" / "README.md"
    if index_path.is_file():
        index = index_path.read_text(encoding="utf-8")
        for link in INDEX_LINKS:
            if f"]({link})" not in index:
                errors.append(f"project-memory index does not link {link}")

    agents_path = root / "AGENTS.md"
    if not agents_path.is_file():
        errors.append("missing AGENTS.md")
    else:
        agents = agents_path.read_text(encoding="utf-8")
        for required_phrase in (
            ".agents/memory/README.md",
            ".agents/memory/current-status.md",
            "End of slice",
        ):
            if required_phrase not in agents:
                errors.append(f"AGENTS.md does not require {required_phrase!r}")

    for dated_file in ("current-status.md", "backlog.md"):
        path = root / ".agents" / "memory" / dated_file
        if path.is_file() and DATE_PATTERN.search(path.read_text(encoding="utf-8")) is None:
            errors.append(f"{dated_file} needs a YYYY-MM-DD Last updated/reviewed line")

    decisions_path = root / ".agents" / "memory" / "decisions.md"
    if decisions_path.is_file():
        decisions = decisions_path.read_text(encoding="utf-8")
        if "append-only" not in decisions.lower():
            errors.append("decisions.md must state its append-only policy")
        if re.search(r"^### \d{4}-\d{2}-\d{2} — ", decisions, re.MULTILINE) is None:
            errors.append("decisions.md needs at least one dated decision entry")

    return errors


def _broken_local_links(path: Path, text: str, root: Path) -> list[str]:
    errors: list[str] = []
    for target in MARKDOWN_LINK.findall(text):
        if target.startswith(("#", "http://", "https://", "mailto:")):
            continue
        clean_target = target.split("#", 1)[0]
        if not clean_target:
            continue
        resolved = (path.parent / clean_target).resolve()
        try:
            resolved.relative_to(root.resolve())
        except ValueError:
            errors.append(f"local link escapes repository in {path.relative_to(root)}: {target}")
            continue
        if not resolved.exists():
            errors.append(f"broken local link in {path.relative_to(root)}: {target}")
    return errors


def main(argv: Sequence[str] | None = None) -> int:
    del argv
    errors = validate_project_memory()
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print("Project memory integrity passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
