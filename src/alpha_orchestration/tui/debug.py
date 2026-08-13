"""Pure presentation helpers for the run debugger.

The debugger deliberately receives the screen-owned event stream rather than
``RunState.recent_events``.  The state projection is a useful bounded summary;
the debug view is an inspection surface and must not silently lose old events.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC
from typing import Final

from rich.text import Text

from alpha_orchestration.domain import EventKind, RunEvent

ALL_EVENTS: Final = "all"
UNASSIGNED_AGENT: Final = "__unassigned__"
EVENT_FAMILIES: Final[tuple[str, ...]] = (
    ALL_EVENTS,
    "run",
    "workflow",
    "stage",
    "task",
    "model",
    "action",
    "tool",
    "agent",
    "evidence",
    "candidate",
)


@dataclass(frozen=True, slots=True)
class EventQuery:
    """Filters applied to an immutable event snapshot."""

    family: str = ALL_EVENTS
    agent_id: str = ALL_EVENTS
    search: str = ""


@dataclass(frozen=True, slots=True)
class DebugCounters:
    """Small operational summary shown above the event table."""

    total: int
    visible: int
    agents: int
    tool_calls: int
    rejections: int
    failures: int


def event_family(kind: EventKind) -> str:
    """Return the stable family prefix used by the kind filter."""

    return kind.value.split("_", maxsplit=1)[0]


def available_agents(events: tuple[RunEvent, ...] | list[RunEvent]) -> tuple[str, ...]:
    """Return sorted agent identifiers represented in the stream."""

    return tuple(sorted({event.agent_id for event in events if event.agent_id is not None}))


def filter_events(
    events: tuple[RunEvent, ...] | list[RunEvent],
    query: EventQuery,
) -> tuple[RunEvent, ...]:
    """Filter events without mutating or truncating the source collection."""

    needle = query.search.strip().casefold()
    visible: list[RunEvent] = []
    for event in events:
        if query.family != ALL_EVENTS and event_family(event.kind) != query.family:
            continue
        if query.agent_id == UNASSIGNED_AGENT:
            if event.agent_id is not None:
                continue
        elif query.agent_id != ALL_EVENTS and event.agent_id != query.agent_id:
            continue
        if needle and needle not in _search_document(event):
            continue
        visible.append(event)
    return tuple(visible)


def event_row(event: RunEvent) -> tuple[str, str, str, str, str]:
    """Create compact table cells for one event."""

    timestamp = event.timestamp.astimezone(UTC).strftime("%H:%M:%S.%f")[:-3]
    return (
        str(event.sequence),
        timestamp,
        event.kind.value,
        event.agent_id or "—",
        summarize_event(event),
    )


def summarize_event(event: RunEvent, *, limit: int = 96) -> str:
    """Summarize an event while retaining useful correlation identifiers."""

    details: list[str] = []
    for key in ("task_id", "tool", "call_id"):
        value = event.payload.get(key)
        if value not in (None, ""):
            details.append(f"{key}={value}")
    summary = event.message.strip() or "(no message)"
    if details:
        summary = f"{summary} | {', '.join(details)}"
    if len(summary) > limit:
        return f"{summary[: limit - 1]}…"
    return summary


def format_event_detail(event: RunEvent | None) -> Text:
    """Return a markup-safe, complete representation of an event."""

    if event is None:
        return Text("Select an event to inspect its complete journal record.", style="dim")
    header = (
        f"EVENT {event.sequence}  {event.kind.value}\n"
        f"agent={event.agent_id or '—'}  time={event.timestamp.astimezone(UTC).isoformat()}\n\n"
    )
    detail = Text(header, style="#D7E3EE")
    detail.append(
        json.dumps(event.to_dict(), ensure_ascii=False, indent=2, sort_keys=True),
        style="#AFC3D6",
    )
    return detail


def format_agent_transcript(
    events: tuple[RunEvent, ...] | list[RunEvent],
    selected: RunEvent | None,
) -> Text:
    """Render the complete per-agent history surrounding the selected event."""

    if selected is None:
        return Text("Select an event to inspect its agent transcript.", style="dim")
    agent_id = selected.agent_id
    matching = tuple(event for event in events if event.agent_id == agent_id)
    label = agent_id or "unassigned / controller"
    transcript = Text(f"{label}  ·  {len(matching)} events\n", style="bold #54D6FF")
    for event in matching:
        transcript.append(
            f"{event.sequence:04d}  {event.kind.value:<22}  {event.message}\n",
            style="#AFC3D6",
        )
    return transcript


def count_events(
    events: tuple[RunEvent, ...] | list[RunEvent],
    visible: tuple[RunEvent, ...] | list[RunEvent],
) -> DebugCounters:
    """Compute counters from the full stream and current filtered view."""

    return DebugCounters(
        total=len(events),
        visible=len(visible),
        agents=len(available_agents(events)),
        tool_calls=sum(event.kind is EventKind.TOOL_STARTED for event in events),
        rejections=sum(event.kind.value.endswith("_rejected") for event in events),
        failures=sum(event.kind.value.endswith("_failed") for event in events),
    )


def format_counters(counters: DebugCounters) -> str:
    """Format the compact counter strip."""

    return (
        f"TOTAL {counters.total}   VISIBLE {counters.visible}   "
        f"AGENTS {counters.agents}   TOOLS {counters.tool_calls}   "
        f"REJECTED {counters.rejections}   FAILED {counters.failures}"
    )


def follow_row_index(*, enabled: bool, current: int | None, row_count: int) -> int | None:
    """Choose a cursor row without conflating follow-tail with selection."""

    if row_count <= 0:
        return None
    if enabled:
        return row_count - 1
    if current is None:
        return 0
    return min(max(current, 0), row_count - 1)


def _search_document(event: RunEvent) -> str:
    payload = json.dumps(event.payload, ensure_ascii=False, sort_keys=True)
    return " ".join(
        (
            str(event.sequence),
            event.kind.value,
            event.agent_id or "",
            event.message,
            payload,
        )
    ).casefold()
