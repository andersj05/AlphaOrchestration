"""Append-only JSONL persistence and deterministic replay."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TextIO

from alpha_orchestration.domain import EventKind, RunEvent, RunSpec, RunState
from alpha_orchestration.reducer import reduce_event


class MemoryJournal:
    def __init__(self) -> None:
        self.events: list[RunEvent] = []
        self.closed = False

    def append(self, event: RunEvent) -> None:
        if self.closed:
            raise RuntimeError("journal is closed")
        self.events.append(event)

    def close(self) -> None:
        self.closed = True


class JsonlJournal:
    """Write one canonical event per line and never rewrite prior records."""

    def __init__(self, path: Path) -> None:
        self.path = path.expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle: TextIO = self.path.open("x", encoding="utf-8", newline="\n")
        self._closed = False

    def append(self, event: RunEvent) -> None:
        if self._closed:
            raise RuntimeError("journal is closed")
        self._handle.write(json.dumps(event.to_dict(), ensure_ascii=False, separators=(",", ":")))
        self._handle.write("\n")
        self._handle.flush()

    def close(self) -> None:
        if not self._closed:
            self._handle.close()
            self._closed = True

    def __enter__(self) -> JsonlJournal:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        del exc_type, exc, traceback
        self.close()


def load_events(path: Path) -> tuple[RunEvent, ...]:
    events: list[RunEvent] = []
    with path.expanduser().open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
                events.append(RunEvent.from_dict(payload))
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ValueError(f"invalid event at {path}:{line_number}: {exc}") from exc
    return tuple(events)


def replay(path: Path) -> RunState:
    events = load_events(path)
    if not events:
        raise ValueError(f"journal is empty: {path}")
    first = events[0]
    if first.kind is not EventKind.RUN_CREATED:
        raise ValueError("journal must begin with run_created")
    raw_spec = first.payload.get("spec")
    if not isinstance(raw_spec, dict):
        raise ValueError("run_created event is missing its spec")
    state = RunState(spec=RunSpec.from_dict(raw_spec))
    for event in events:
        state = reduce_event(state, event)
    return state
