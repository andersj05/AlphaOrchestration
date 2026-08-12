"""Stable ports owned by AlphaOrchestration."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

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
class ActionModelRequest:
    """One bounded microtask turn rendered by Alpha for an action model."""

    run_id: str
    workflow_id: str
    task_id: str
    agent_id: str
    turn: int
    request_id: str
    session_id: str
    prompt_key: str
    transcript: Sequence[Mapping[str, JsonValue]]
    tool_contracts: Sequence[Mapping[str, JsonValue]]
    output_schema: Mapping[str, JsonValue]
    allowed_source_ids: Sequence[str]
    max_new_tokens: int
    max_action_bytes: int
    evidence_packet: Mapping[str, JsonValue] | None = None
    sampling: Mapping[str, JsonValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Detach every model-visible container from controller-owned state."""

        object.__setattr__(
            self,
            "transcript",
            tuple(_strict_json_copy(item, "transcript item") for item in self.transcript),
        )
        object.__setattr__(
            self,
            "tool_contracts",
            tuple(_strict_json_copy(item, "tool contract") for item in self.tool_contracts),
        )
        object.__setattr__(
            self,
            "output_schema",
            _strict_json_copy(self.output_schema, "output schema"),
        )
        object.__setattr__(self, "allowed_source_ids", tuple(self.allowed_source_ids))
        object.__setattr__(
            self,
            "evidence_packet",
            None
            if self.evidence_packet is None
            else _strict_json_copy(self.evidence_packet, "evidence packet"),
        )
        object.__setattr__(
            self,
            "sampling",
            _strict_json_copy(self.sampling, "sampling controls"),
        )

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "run_id": self.run_id,
            "workflow_id": self.workflow_id,
            "task_id": self.task_id,
            "agent_id": self.agent_id,
            "turn": self.turn,
            "request_id": self.request_id,
            "session_id": self.session_id,
            "prompt_key": self.prompt_key,
            "transcript": [_strict_json_copy(item, "transcript item") for item in self.transcript],
            "tool_contracts": [_strict_json_copy(item, "tool contract") for item in self.tool_contracts],
            "output_schema": _strict_json_copy(self.output_schema, "output schema"),
            "allowed_source_ids": list(self.allowed_source_ids),
            "evidence_packet": (
                None
                if self.evidence_packet is None
                else _strict_json_copy(self.evidence_packet, "evidence packet")
            ),
            "max_new_tokens": self.max_new_tokens,
            "max_action_bytes": self.max_action_bytes,
            "sampling": _strict_json_copy(self.sampling, "sampling controls"),
        }


@dataclass(frozen=True, slots=True)
class ActionModelResult:
    """Exact visible action output and deterministic generation trace."""

    request_id: str
    output_text: str
    prompt_ids: tuple[int, ...] = ()
    output_ids: tuple[int, ...] = ()
    finish_reason: str | None = "stop"
    telemetry: Mapping[str, JsonValue] = field(default_factory=dict)
    model_fingerprint: str = "unknown"
    tokenizer_fingerprint: str = "unknown"


class ActionModel(Protocol):
    async def complete(self, request: ActionModelRequest) -> ActionModelResult: ...


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


def _strict_json_copy(value: Any, label: str) -> Any:
    try:
        return json.loads(
            json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
        )
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{label} must contain only strict JSON values") from exc
