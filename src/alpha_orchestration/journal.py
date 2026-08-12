"""Append-only JSONL persistence and deterministic replay."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import TextIO

from alpha_orchestration.domain import EventKind, RunEvent, RunSpec, RunState
from alpha_orchestration.reducer import reduce_event


class _DuplicateKeyError(ValueError):
    pass


class _NonFiniteConstantError(ValueError):
    pass


class _IntegrityVerifier:
    """Verify self-contained lifecycle hashes while accepting legacy journals."""

    def __init__(self) -> None:
        self._workflow_identity: tuple[str, str, str] | None = None

    def verify(self, event: RunEvent) -> None:
        payload = event.payload
        if event.kind is EventKind.WORKFLOW_PLANNED:
            self._verify_workflow_planned(payload)
        elif event.kind is EventKind.WORKFLOW_COMPLETED:
            self._verify_workflow_completed(payload)
        elif event.kind is EventKind.MODEL_TURN_STARTED:
            _verify_canonical_field(payload, "request", "request_hash")
        elif event.kind is EventKind.MODEL_TURN_COMPLETED:
            self._verify_model_turn_completed(payload)
        elif event.kind is EventKind.TOOL_STARTED:
            self._verify_tool_started(payload)
        elif event.kind in {EventKind.TOOL_COMPLETED, EventKind.TOOL_REJECTED}:
            self._verify_tool_result(payload)

    def _verify_workflow_planned(self, payload: dict[str, object]) -> None:
        plan = payload.get("plan")
        if plan is None:
            return
        if not isinstance(plan, dict):
            raise ValueError("workflow_planned payload plan must be an object")
        _verify_canonical_field(payload, "plan", "plan_hash")

        workflow_id = _required_string(payload, "workflow_id", "workflow_planned")
        workflow_version = _required_string(
            payload,
            "workflow_version",
            "workflow_planned",
        )
        plan_workflow_id = _required_string(plan, "workflow_id", "workflow plan")
        plan_version = _required_string(plan, "version", "workflow plan")
        if workflow_id != plan_workflow_id:
            raise ValueError("workflow_planned workflow_id does not match plan")
        if workflow_version != plan_version:
            raise ValueError("workflow_planned workflow_version does not match plan")
        if payload.get("tasks") != plan.get("tasks"):
            raise ValueError("workflow_planned tasks do not match plan")

        plan_hash = _required_digest(payload, "plan_hash", "workflow_planned")
        identity = (workflow_id, workflow_version, plan_hash)
        if self._workflow_identity is not None:
            raise ValueError("journal contains multiple integrity workflow plans")
        self._workflow_identity = identity

    def _verify_workflow_completed(self, payload: dict[str, object]) -> None:
        if self._workflow_identity is None:
            return
        actual = (
            _required_string(payload, "workflow_id", "workflow_completed"),
            _required_string(payload, "workflow_version", "workflow_completed"),
            _required_digest(payload, "plan_hash", "workflow_completed"),
        )
        if actual != self._workflow_identity:
            raise ValueError(
                "workflow_completed identity or plan_hash does not match workflow_planned"
            )

    def _verify_model_turn_completed(self, payload: dict[str, object]) -> None:
        trace = _verify_canonical_field(payload, "trace", "trace_hash")
        if not isinstance(trace, dict):
            raise ValueError("model_turn_completed payload trace must be an object")

        truncated = payload.get("output_truncated")
        if not isinstance(truncated, bool):
            raise ValueError("model_turn_completed payload output_truncated must be a boolean")
        output_hash = _required_digest(payload, "output_hash", "model_turn_completed")
        if truncated:
            if payload.get("output") is not None:
                raise ValueError("truncated model_turn_completed output must be null")
        else:
            output = payload.get("output")
            if not isinstance(output, str):
                raise ValueError("untruncated model_turn_completed output must be text")
            if _text_hash(output) != output_hash:
                raise ValueError("model_turn_completed output_hash mismatch")

        aliases = {
            "output_text": payload.get("output"),
            "output_hash": output_hash,
            "output_truncated": truncated,
        }
        for key, expected in aliases.items():
            if trace.get(key) != expected:
                raise ValueError(f"model_turn_completed trace {key} does not match payload")

    def _verify_tool_started(self, payload: dict[str, object]) -> None:
        is_fixed_lifecycle = self._workflow_identity is not None or "task_id" in payload
        if not is_fixed_lifecycle and "arguments_hash" not in payload:
            return
        _verify_canonical_field(payload, "arguments", "arguments_hash")
        if "proposed_arguments" in payload or "proposed_arguments_hash" in payload:
            _verify_canonical_field(
                payload,
                "proposed_arguments",
                "proposed_arguments_hash",
            )

    def _verify_tool_result(self, payload: dict[str, object]) -> None:
        envelope = payload.get("result_envelope")
        if envelope is None:
            if self._workflow_identity is not None:
                raise ValueError("fixed-DAG tool result is missing result_envelope")
            if "result_hash" not in payload:
                return
            envelope = {
                "call_id": payload.get("call_id"),
                "payload": payload.get("result"),
                "source_ids": payload.get("source_ids"),
                "retryable": payload.get("retryable"),
            }
        if not isinstance(envelope, dict):
            raise ValueError("tool result_envelope must be an object")

        expected_hash = _required_digest(payload, "result_hash", "tool result")
        if _canonical_hash(envelope) != expected_hash:
            raise ValueError("tool result_hash mismatch")

        aliases = {
            "call_id": payload.get("call_id"),
            "payload": payload.get("result"),
            "source_ids": payload.get("source_ids"),
            "retryable": payload.get("retryable"),
        }
        for key, expected in aliases.items():
            if envelope.get(key) != expected:
                raise ValueError(f"tool result_envelope {key} does not match payload")
        envelope_payload = envelope.get("payload")
        if isinstance(envelope_payload, dict) and payload.get("error") != envelope_payload.get(
            "error"
        ):
            raise ValueError("tool result error does not match result_envelope payload")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateKeyError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _reject_non_finite(value: str) -> None:
    raise _NonFiniteConstantError(f"non-finite JSON constant is not allowed: {value}")


def _required_string(
    payload: Mapping[str, object],
    key: str,
    label: str,
) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} payload {key} must be a non-empty string")
    return value


def _required_digest(
    payload: Mapping[str, object],
    key: str,
    label: str,
) -> str:
    value = payload.get(key)
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{label} payload {key} must be a lowercase SHA-256 digest")
    return value


def _verify_canonical_field(
    payload: Mapping[str, object],
    value_key: str,
    hash_key: str,
) -> object:
    if value_key not in payload:
        raise ValueError(f"lifecycle payload is missing {value_key}")
    expected = _required_digest(payload, hash_key, value_key)
    value = payload[value_key]
    if _canonical_hash(value) != expected:
        raise ValueError(f"{hash_key} mismatch for {value_key}")
    return value


def _canonical_hash(value: object) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _text_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


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
        self._handle.write(
            json.dumps(
                event.to_dict(),
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
            )
        )
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
    verifier = _IntegrityVerifier()
    with path.expanduser().open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                payload = json.loads(
                    line,
                    object_pairs_hook=_unique_object,
                    parse_constant=_reject_non_finite,
                )
                if not isinstance(payload, dict):
                    raise ValueError("event must be a JSON object")
                event = RunEvent.from_dict(payload)
                verifier.verify(event)
                events.append(event)
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
