"""Strict, bounded action envelopes emitted by a local research model.

The model is allowed to propose either a bounded batch of tool calls or one
final payload.  It does not supply call IDs, retry policy, task transitions, or
workflow structure; those remain controller-owned concerns.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from alpha_orchestration.domain import JsonValue

DEFAULT_MAX_ACTION_BYTES = 32_768
DEFAULT_MAX_CALLS = 4
MAX_TOOL_NAME_LENGTH = 200


class ActionErrorCode(StrEnum):
    INVALID_TEXT = "invalid_text"
    ACTION_TOO_LARGE = "action_too_large"
    INVALID_JSON = "invalid_json"
    DUPLICATE_KEY = "duplicate_key"
    INVALID_ENVELOPE = "invalid_envelope"
    UNKNOWN_KIND = "unknown_kind"
    TOOL_CALL_LIMIT = "tool_call_limit"


class ActionParseError(ValueError):
    """A stable, policy-readable failure returned by strict action parsing."""

    def __init__(
        self,
        code: ActionErrorCode,
        message: str,
        *,
        repairable: bool,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.repairable = repairable


@dataclass(frozen=True, slots=True)
class ProposedToolCall:
    """A model proposal; the controller assigns its durable ``call_id``."""

    name: str
    arguments: dict[str, JsonValue]


@dataclass(frozen=True, slots=True)
class ToolCallsAction:
    calls: tuple[ProposedToolCall, ...]
    kind: str = "tool_calls"


@dataclass(frozen=True, slots=True)
class FinalAction:
    payload: dict[str, JsonValue]
    kind: str = "final"


AgentAction = ToolCallsAction | FinalAction


class _DuplicateKey(ValueError):
    def __init__(self, key: str) -> None:
        super().__init__(key)
        self.key = key


class _NonFiniteNumber(ValueError):
    pass


def parse_action(
    raw: str,
    *,
    max_bytes: int = DEFAULT_MAX_ACTION_BYTES,
    max_calls: int = DEFAULT_MAX_CALLS,
) -> AgentAction:
    """Parse exactly one JSON action and reject ambiguous or oversized output.

    Duplicate keys are rejected at every nesting level.  This matters because
    silently accepting the last duplicate could make the audited action differ
    from the value a human or another JSON implementation reads.
    """

    if max_bytes < 1:
        raise ValueError("max_bytes must be positive")
    if max_calls < 1:
        raise ValueError("max_calls must be positive")
    if not isinstance(raw, str):
        raise ActionParseError(
            ActionErrorCode.INVALID_TEXT,
            "action must be text",
            repairable=True,
        )
    try:
        encoded_size = len(raw.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise ActionParseError(
            ActionErrorCode.INVALID_TEXT,
            "action is not valid UTF-8 text",
            repairable=True,
        ) from exc
    if encoded_size > max_bytes:
        raise ActionParseError(
            ActionErrorCode.ACTION_TOO_LARGE,
            f"action is {encoded_size} bytes; limit is {max_bytes}",
            repairable=False,
        )

    try:
        decoded = json.loads(
            raw,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_non_finite,
        )
    except _DuplicateKey as exc:
        raise ActionParseError(
            ActionErrorCode.DUPLICATE_KEY,
            f"action contains duplicate key {exc.key!r}",
            repairable=True,
        ) from exc
    except _NonFiniteNumber as exc:
        raise ActionParseError(
            ActionErrorCode.INVALID_JSON,
            str(exc),
            repairable=True,
        ) from exc
    except (json.JSONDecodeError, RecursionError) as exc:
        raise ActionParseError(
            ActionErrorCode.INVALID_JSON,
            f"action is not one complete JSON value: {exc}",
            repairable=True,
        ) from exc

    envelope = _require_object(decoded, "action")
    kind = envelope.get("kind")
    if not isinstance(kind, str):
        raise ActionParseError(
            ActionErrorCode.INVALID_ENVELOPE,
            "$.kind must be a string",
            repairable=True,
        )
    if kind == "tool_calls":
        _require_exact_keys(envelope, {"kind", "calls"}, "$")
        return _parse_tool_calls(envelope["calls"], max_calls=max_calls)
    if kind == "final":
        _require_exact_keys(envelope, {"kind", "payload"}, "$")
        payload = _require_object(envelope["payload"], "$.payload")
        return FinalAction(payload=payload)
    raise ActionParseError(
        ActionErrorCode.UNKNOWN_KIND,
        "$.kind must be 'tool_calls' or 'final'",
        repairable=True,
    )


def _parse_tool_calls(value: Any, *, max_calls: int) -> ToolCallsAction:
    if not isinstance(value, list):
        raise ActionParseError(
            ActionErrorCode.INVALID_ENVELOPE,
            "$.calls must be an array",
            repairable=True,
        )
    if not value:
        raise ActionParseError(
            ActionErrorCode.INVALID_ENVELOPE,
            "$.calls must contain at least one call",
            repairable=True,
        )
    if len(value) > max_calls:
        raise ActionParseError(
            ActionErrorCode.TOOL_CALL_LIMIT,
            f"$.calls contains {len(value)} calls; limit is {max_calls}",
            repairable=False,
        )

    calls: list[ProposedToolCall] = []
    for index, raw_call in enumerate(value):
        path = f"$.calls[{index}]"
        call = _require_object(raw_call, path)
        _require_exact_keys(call, {"name", "arguments"}, path)
        name = call["name"]
        if not isinstance(name, str):
            raise ActionParseError(
                ActionErrorCode.INVALID_ENVELOPE,
                f"{path}.name must be a string",
                repairable=True,
            )
        if not name or name != name.strip() or len(name) > MAX_TOOL_NAME_LENGTH:
            raise ActionParseError(
                ActionErrorCode.INVALID_ENVELOPE,
                f"{path}.name must be non-empty, unpadded, and at most {MAX_TOOL_NAME_LENGTH} characters",
                repairable=True,
            )
        arguments = _require_object(call["arguments"], f"{path}.arguments")
        calls.append(ProposedToolCall(name=name, arguments=arguments))
    return ToolCallsAction(calls=tuple(calls))


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateKey(key)
        result[key] = value
    return result


def _reject_non_finite(value: str) -> None:
    raise _NonFiniteNumber(f"non-finite JSON number is not allowed: {value}")


def _require_object(value: Any, path: str) -> dict[str, JsonValue]:
    if not isinstance(value, Mapping):
        raise ActionParseError(
            ActionErrorCode.INVALID_ENVELOPE,
            f"{path} must be an object",
            repairable=True,
        )
    result = dict(value)
    _validate_json_value(result, path)
    return result


def _require_exact_keys(value: Mapping[str, Any], expected: set[str], path: str) -> None:
    actual = set(value)
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    if missing or extra:
        details: list[str] = []
        if missing:
            details.append(f"missing {missing!r}")
        if extra:
            details.append(f"unexpected {extra!r}")
        raise ActionParseError(
            ActionErrorCode.INVALID_ENVELOPE,
            f"{path} has invalid keys: {', '.join(details)}",
            repairable=True,
        )


def _validate_json_value(value: Any, path: str) -> None:
    if value is None or isinstance(value, (bool, str, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ActionParseError(
                ActionErrorCode.INVALID_JSON,
                f"{path} contains a non-finite number",
                repairable=True,
            )
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_json_value(item, f"{path}[{index}]")
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ActionParseError(
                    ActionErrorCode.INVALID_JSON,
                    f"{path} contains a non-string object key",
                    repairable=True,
                )
            _validate_json_value(item, f"{path}.{key}")
        return
    raise ActionParseError(
        ActionErrorCode.INVALID_JSON,
        f"{path} contains unsupported JSON value {type(value).__name__}",
        repairable=True,
    )
