"""Strict, bounded dispatch for deterministic read-only tools."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from alpha_orchestration.domain import JsonValue
from alpha_orchestration.ports import ToolCall, ToolResult
from alpha_orchestration.tools.schema import SchemaValidationError, validate_json, validate_schema

ToolHandler = Callable[[Mapping[str, JsonValue]], Mapping[str, JsonValue]]


@dataclass(frozen=True, slots=True)
class ToolDefinition:
    """A model-visible contract paired with a local deterministic handler."""

    name: str
    description: str
    input_schema: Mapping[str, JsonValue]
    handler: ToolHandler
    version: str = "1.0.0"
    read_only: bool = True
    idempotent: bool = True
    _input_schema_snapshot: str = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        try:
            encoded = json.dumps(
                self.input_schema,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            decoded = json.loads(encoded)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("tool input schema must contain only strict JSON values") from exc
        if not isinstance(decoded, dict) or decoded.get("type") != "object":
            raise ValueError("tool input schema must describe an object")
        object.__setattr__(self, "input_schema", decoded)
        try:
            validate_schema(decoded)
        except SchemaValidationError as exc:
            raise ValueError(f"invalid tool input schema: {exc}") from exc
        object.__setattr__(self, "_input_schema_snapshot", encoded)

    def input_schema_copy(self) -> dict[str, JsonValue]:
        """Return a detached copy of the trusted validation policy."""

        decoded = json.loads(self._input_schema_snapshot)
        if not isinstance(decoded, dict):  # pragma: no cover - guarded at construction
            raise AssertionError("tool schema snapshot is not an object")
        return decoded

    def public_contract(self) -> dict[str, JsonValue]:
        return {
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "input_schema": self.input_schema_copy(),
            "annotations": {
                "read_only": self.read_only,
                "idempotent": self.idempotent,
            },
        }


class ToolRegistry:
    """Validate, execute, bound, and idempotently replay local tool calls.

    Tool failures are returned as compact structured payloads. That gives a small
    model one opportunity to repair an argument envelope without asking it to parse
    Python exceptions. Reusing a call ID with different content is always rejected.
    """

    def __init__(
        self,
        definitions: Sequence[ToolDefinition] = (),
        *,
        max_result_bytes: int = 256_000,
    ) -> None:
        if max_result_bytes < 1_024:
            raise ValueError("max_result_bytes must be at least 1024")
        self._definitions: dict[str, ToolDefinition] = {}
        self._cache: dict[str, tuple[str, ToolResult]] = {}
        self._max_result_bytes = max_result_bytes
        self._lock = asyncio.Lock()
        for definition in definitions:
            self.register(definition)

    def register(self, definition: ToolDefinition) -> None:
        name = definition.name.strip()
        if not name or name != definition.name:
            raise ValueError("tool names must be non-empty and have no surrounding whitespace")
        if name in self._definitions:
            raise ValueError(f"tool already registered: {name}")
        if definition.input_schema_copy().get("type") != "object":
            raise ValueError(f"tool input schema must describe an object: {name}")
        self._definitions[name] = definition

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._definitions))

    def contracts(self, allowed_tools: Sequence[str] | None = None) -> tuple[dict[str, JsonValue], ...]:
        """Return compact contracts, optionally narrowed to a task allowlist."""

        selected = self.names if allowed_tools is None else tuple(allowed_tools)
        unknown = sorted(set(selected).difference(self._definitions))
        if unknown:
            raise ValueError(f"unknown tools in allowlist: {unknown!r}")
        return tuple(self._definitions[name].public_contract() for name in selected)

    def scoped(
        self,
        allowed_tools: Sequence[str],
        *,
        allowed_source_ids: Sequence[str] | None = None,
    ) -> ScopedToolExecutor:
        """Return an executor constrained by controller-owned tool and source allowlists."""

        return ScopedToolExecutor(self, allowed_tools, allowed_source_ids=allowed_source_ids)

    async def execute(self, call: ToolCall) -> ToolResult:
        async with self._lock:
            fingerprint = _call_fingerprint(call)
            cached = self._cache.get(call.call_id)
            if cached is not None:
                prior_fingerprint, result = cached
                if prior_fingerprint == fingerprint:
                    return result
                return _failure(
                    call,
                    "call_id_conflict",
                    "call_id was already used with different tool arguments",
                )

            result = self._execute_once(call)
            self._cache[call.call_id] = (fingerprint, result)
            return result

    def _execute_once(self, call: ToolCall) -> ToolResult:
        if not call.call_id.strip():
            return _failure(call, "invalid_call_id", "call_id must not be empty")

        definition = self._definitions.get(call.name)
        if definition is None:
            return _failure(
                call,
                "unknown_tool",
                f"tool is not registered: {call.name}",
                details={"allowed_tools": list(self.names)},
            )

        try:
            validate_json(call.arguments, definition.input_schema_copy())
            arguments = dict(call.arguments)
            source_ids = _source_ids(arguments.pop("source_ids", []))
            data = dict(definition.handler(arguments))
            payload: dict[str, JsonValue] = {"ok": True, "tool": call.name, "data": data}
            encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
            if len(encoded) > self._max_result_bytes:
                return _failure(
                    call,
                    "result_too_large",
                    f"tool result exceeds {self._max_result_bytes} bytes",
                )
            return ToolResult(call_id=call.call_id, payload=payload, source_ids=source_ids)
        except SchemaValidationError as exc:
            return _failure(
                call,
                "invalid_schema",
                str(exc),
                details={"phase": "schema"},
            )
        except (ToolInputError, ValueError) as exc:
            return _failure(
                call,
                "invalid_arguments",
                str(exc),
                details={"phase": "semantic"},
            )
        except (ArithmeticError, OverflowError) as exc:
            return _failure(call, "calculation_error", str(exc))


class ScopedToolExecutor:
    """A registry view that cannot escape its fixed tool and evidence policy."""

    def __init__(
        self,
        registry: ToolRegistry,
        allowed_tools: Sequence[str],
        *,
        allowed_source_ids: Sequence[str] | None = None,
    ) -> None:
        names = tuple(sorted(set(allowed_tools)))
        registry.contracts(names)
        self._registry = registry
        self._names = names
        self._allowed_source_ids = None if allowed_source_ids is None else frozenset(allowed_source_ids)

    @property
    def names(self) -> tuple[str, ...]:
        return self._names

    def contracts(self) -> tuple[dict[str, JsonValue], ...]:
        return self._registry.contracts(self._names)

    async def execute(self, call: ToolCall) -> ToolResult:
        if call.name not in self._names:
            return _failure(
                call,
                "tool_not_allowed",
                f"tool is not allowed in this scope: {call.name}",
                details={"allowed_tools": list(self._names)},
            )
        if self._allowed_source_ids is not None:
            raw_source_ids = call.arguments.get("source_ids", [])
            if isinstance(raw_source_ids, list) and all(isinstance(item, str) for item in raw_source_ids):
                unknown_source_ids = sorted(set(raw_source_ids).difference(self._allowed_source_ids))
                if unknown_source_ids:
                    return _failure(
                        call,
                        "source_not_allowed",
                        "one or more source_ids are outside the trusted evidence set",
                        details={"unknown_source_ids": unknown_source_ids},
                    )
        return await self._registry.execute(call)


class ToolInputError(ValueError):
    """A semantically invalid, but schema-shaped, tool request."""


def source_ids_schema() -> dict[str, JsonValue]:
    return {
        "type": "array",
        "items": {"type": "string", "minLength": 1, "maxLength": 200},
        "maxItems": 100,
        "uniqueItems": True,
        "description": "Evidence IDs for every fact used by this calculation.",
    }


def context_schema() -> dict[str, JsonValue]:
    """Shared unit/period metadata echoed into calculation results."""

    return {
        "type": "object",
        "properties": {
            "currency": {"type": "string", "minLength": 3, "maxLength": 12},
            "scale": {
                "type": "string",
                "enum": ["units", "thousands", "millions", "billions"],
            },
            "current_period": {"type": "string", "minLength": 1, "maxLength": 50},
            "prior_period": {"type": "string", "minLength": 1, "maxLength": 50},
            "period_type": {
                "type": "string",
                "enum": ["quarter", "year", "trailing_twelve_months", "other"],
            },
            "ticker": {"type": "string", "minLength": 1, "maxLength": 32},
            "entity_id": {"type": "string", "minLength": 1, "maxLength": 200},
            "as_of": {"type": "string", "minLength": 1, "maxLength": 50},
        },
        "additionalProperties": False,
    }


def _source_ids(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ToolInputError("source_ids must be an array of strings")
    return tuple(value)


def _call_fingerprint(call: ToolCall) -> str:
    try:
        encoded = json.dumps(
            {"name": call.name, "arguments": call.arguments},
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
    except (TypeError, ValueError) as exc:
        encoded = f"invalid:{call.name}:{exc}:{call.arguments!r}".encode()
    return hashlib.sha256(encoded).hexdigest()


def _failure(
    call: ToolCall,
    code: str,
    message: str,
    *,
    details: Mapping[str, JsonValue] | None = None,
) -> ToolResult:
    error: dict[str, JsonValue] = {"code": code, "message": message}
    if details:
        error["details"] = dict(details)
    return ToolResult(
        call_id=call.call_id,
        payload={"ok": False, "tool": call.name, "error": error},
        source_ids=(),
        retryable=False,
    )
