"""Stable ports owned by AlphaOrchestration."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol

from alpha_orchestration.domain import EventKind, JsonValue, RunEvent, RunSpec


@dataclass(frozen=True, slots=True)
class EventDraft:
    """An unsequenced runtime event; the controller owns ordering and time."""

    kind: EventKind
    message: str
    agent_id: str | None = None
    payload: dict[str, JsonValue] = field(default_factory=dict)


class OrchestratorRuntime(Protocol):
    def stream(self, spec: RunSpec) -> AsyncIterator[EventDraft]: ...


class EventJournal(Protocol):
    def append(self, event: RunEvent) -> None: ...

    def close(self) -> None: ...


EventSubscriber = Callable[[RunEvent], None]


@dataclass(frozen=True, slots=True)
class EngineRequest:
    """A complete, already-tokenized turn submitted to the local engine."""

    session_id: str
    prompt_ids: Sequence[int]
    max_new_tokens: int
    request_id: str
    sampling: Mapping[str, JsonValue] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class EngineDelta:
    request_id: str
    session_id: str
    delta_ids: tuple[int, ...]
    delta_text: str
    generated_tokens: int
    finished: bool
    finish_reason: str | None = None
    telemetry: Mapping[str, JsonValue] = field(default_factory=dict)


class InferenceEngine(Protocol):
    def create_session(self, session_id: str) -> str: ...

    async def stream(self, request: EngineRequest) -> AsyncIterator[EngineDelta]: ...

    async def cancel(self, request_id: str) -> bool: ...

    async def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class ToolCall:
    name: str
    arguments: Mapping[str, JsonValue]
    call_id: str


@dataclass(frozen=True, slots=True)
class ToolResult:
    call_id: str
    payload: Mapping[str, JsonValue]
    source_ids: tuple[str, ...]
    retryable: bool = False


class ToolExecutor(Protocol):
    async def execute(self, call: ToolCall) -> ToolResult: ...
