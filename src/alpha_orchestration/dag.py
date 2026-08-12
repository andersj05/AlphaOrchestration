"""Immutable contracts for controller-authored, bounded task DAGs."""

from __future__ import annotations

import hashlib
import heapq
import json
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from alpha_orchestration.domain import JsonValue
from alpha_orchestration.tools.schema import SchemaValidationError, validate_schema

MAX_TASKS = 256
MAX_ACTIVE_SLOTS = 8
MAX_DEPENDENCIES = 64
MAX_ALLOWED_TOOLS = 32
MAX_TURNS = 16
MAX_TOOL_CALLS = 64
MAX_CALLS_PER_TURN = 8
MAX_NEW_TOKENS = 4_096
MIN_ACTION_BYTES = 128
MAX_ACTION_BYTES = 262_144
MAX_REPAIR_BUDGET = 1


class DagErrorCode(StrEnum):
    INVALID_FIELD = "invalid_field"
    DUPLICATE_TASK = "duplicate_task"
    UNKNOWN_DEPENDENCY = "unknown_dependency"
    DEPENDENCY_CYCLE = "dependency_cycle"
    UNKNOWN_TOOL = "unknown_tool"
    TOOL_NOT_READ_ONLY = "tool_not_read_only"


class DagValidationError(ValueError):
    def __init__(self, code: DagErrorCode, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class TaskDefinition:
    """One fixed microtask and its controller-owned execution policy."""

    task_id: str
    agent_id: str
    depends_on: tuple[str, ...] = ()
    allowed_tools: tuple[str, ...] = ()
    prompt_key: str = "default"
    output_schema: Mapping[str, JsonValue] = field(
        default_factory=lambda: {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        }
    )
    required: bool = True
    allow_failed_dependencies: bool = False
    max_turns: int = 4
    max_tool_calls: int = 8
    max_calls_per_turn: int = 4
    max_new_tokens: int = 512
    max_action_bytes: int = 32_768
    repair_budget: int = 1
    _output_schema_snapshot: str = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        _validate_name(self.task_id, "task_id", maximum=200)
        _validate_name(self.agent_id, "agent_id", maximum=200)
        _validate_name(self.prompt_key, "prompt_key", maximum=200)
        if not isinstance(self.required, bool):
            raise DagValidationError(DagErrorCode.INVALID_FIELD, "required must be a boolean")
        if not isinstance(self.allow_failed_dependencies, bool):
            raise DagValidationError(
                DagErrorCode.INVALID_FIELD,
                "allow_failed_dependencies must be a boolean",
            )
        dependencies = _string_tuple(self.depends_on, "depends_on", maximum=MAX_DEPENDENCIES)
        tools = _string_tuple(self.allowed_tools, "allowed_tools", maximum=MAX_ALLOWED_TOOLS)
        if self.task_id in dependencies:
            raise DagValidationError(
                DagErrorCode.DEPENDENCY_CYCLE,
                f"task {self.task_id!r} cannot depend on itself",
            )
        for dependency in dependencies:
            _validate_name(dependency, "dependency", maximum=200)
        for tool in tools:
            _validate_name(tool, "allowed tool", maximum=200)
        _validate_unique(dependencies, f"task {self.task_id!r} dependencies")
        _validate_unique(tools, f"task {self.task_id!r} allowed tools")
        _bounded_int(self.max_turns, "max_turns", 1, MAX_TURNS)
        _bounded_int(self.max_tool_calls, "max_tool_calls", 0, MAX_TOOL_CALLS)
        _bounded_int(self.max_calls_per_turn, "max_calls_per_turn", 1, MAX_CALLS_PER_TURN)
        _bounded_int(self.max_new_tokens, "max_new_tokens", 1, MAX_NEW_TOKENS)
        _bounded_int(self.max_action_bytes, "max_action_bytes", MIN_ACTION_BYTES, MAX_ACTION_BYTES)
        _bounded_int(self.repair_budget, "repair_budget", 0, MAX_REPAIR_BUDGET)
        if self.max_tool_calls and self.max_calls_per_turn > self.max_tool_calls:
            raise DagValidationError(
                DagErrorCode.INVALID_FIELD,
                "max_calls_per_turn cannot exceed max_tool_calls",
            )
        if tools and self.max_tool_calls == 0:
            raise DagValidationError(
                DagErrorCode.INVALID_FIELD,
                "allowed_tools requires a positive max_tool_calls budget",
            )
        schema = _canonical_object(self.output_schema, "output_schema")
        try:
            validate_schema(schema)
        except SchemaValidationError as exc:
            raise DagValidationError(DagErrorCode.INVALID_FIELD, f"invalid output_schema: {exc}") from exc
        if schema.get("type") != "object":
            raise DagValidationError(
                DagErrorCode.INVALID_FIELD,
                "output_schema must describe a JSON object",
            )
        object.__setattr__(self, "depends_on", dependencies)
        object.__setattr__(self, "allowed_tools", tools)
        object.__setattr__(self, "output_schema", schema)
        object.__setattr__(
            self,
            "_output_schema_snapshot",
            json.dumps(schema, sort_keys=True, separators=(",", ":"), allow_nan=False),
        )

    def output_schema_copy(self) -> dict[str, JsonValue]:
        """Return a detached copy of the controller-authored output policy."""

        decoded = json.loads(self._output_schema_snapshot)
        if not isinstance(decoded, dict):  # pragma: no cover - guarded at construction
            raise AssertionError("output schema snapshot is not an object")
        return decoded

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "task_id": self.task_id,
            "agent_id": self.agent_id,
            "depends_on": list(self.depends_on),
            "allowed_tools": list(self.allowed_tools),
            "prompt_key": self.prompt_key,
            "output_schema": self.output_schema_copy(),
            "required": self.required,
            "allow_failed_dependencies": self.allow_failed_dependencies,
            "max_turns": self.max_turns,
            "max_tool_calls": self.max_tool_calls,
            "max_calls_per_turn": self.max_calls_per_turn,
            "max_new_tokens": self.max_new_tokens,
            "max_action_bytes": self.max_action_bytes,
            "repair_budget": self.repair_budget,
        }


@dataclass(frozen=True, slots=True)
class WorkflowDefinition:
    """A fixed graph whose topology and budgets are never model-authored."""

    workflow_id: str
    version: str
    tasks: tuple[TaskDefinition, ...]
    active_slots: int = 4

    def __post_init__(self) -> None:
        _validate_name(self.workflow_id, "workflow_id", maximum=200)
        _validate_name(self.version, "version", maximum=50)
        if isinstance(self.tasks, (str, bytes)) or not isinstance(self.tasks, Sequence):
            raise DagValidationError(DagErrorCode.INVALID_FIELD, "tasks must be a sequence")
        tasks = tuple(self.tasks)
        if not tasks or len(tasks) > MAX_TASKS:
            raise DagValidationError(
                DagErrorCode.INVALID_FIELD,
                f"tasks must contain between 1 and {MAX_TASKS} items",
            )
        if any(not isinstance(task, TaskDefinition) for task in tasks):
            raise DagValidationError(DagErrorCode.INVALID_FIELD, "every task must be a TaskDefinition")
        _bounded_int(self.active_slots, "active_slots", 1, MAX_ACTIVE_SLOTS)

        ids = [task.task_id for task in tasks]
        duplicates = _duplicates(ids)
        if duplicates:
            raise DagValidationError(
                DagErrorCode.DUPLICATE_TASK,
                f"duplicate task IDs: {duplicates!r}",
            )
        known = set(ids)
        for task in tasks:
            unknown = sorted(set(task.depends_on) - known)
            if unknown:
                raise DagValidationError(
                    DagErrorCode.UNKNOWN_DEPENDENCY,
                    f"task {task.task_id!r} has unknown dependencies: {unknown!r}",
                )
        _stable_topological_ids(tasks)
        object.__setattr__(self, "tasks", tasks)

    @property
    def task_ids(self) -> tuple[str, ...]:
        return tuple(task.task_id for task in self.tasks)

    @property
    def tasks_by_id(self) -> dict[str, TaskDefinition]:
        return {task.task_id: task for task in self.tasks}

    @property
    def topological_task_ids(self) -> tuple[str, ...]:
        return _stable_topological_ids(self.tasks)

    @property
    def topological_tasks(self) -> tuple[TaskDefinition, ...]:
        by_id = self.tasks_by_id
        return tuple(by_id[task_id] for task_id in self.topological_task_ids)

    @property
    def plan_hash(self) -> str:
        encoded = json.dumps(
            self.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def validate_tools(
        self,
        known_tools: Iterable[str],
        *,
        read_only_tools: Iterable[str] | None = None,
    ) -> None:
        """Fail closed when the DAG references tools outside a runtime catalog."""

        known = set(known_tools)
        requested = {name for task in self.tasks for name in task.allowed_tools}
        unknown = sorted(requested - known)
        if unknown:
            raise DagValidationError(
                DagErrorCode.UNKNOWN_TOOL,
                f"workflow references unknown tools: {unknown!r}",
            )
        if read_only_tools is not None:
            non_read_only = sorted(requested - set(read_only_tools))
            if non_read_only:
                raise DagValidationError(
                    DagErrorCode.TOOL_NOT_READ_ONLY,
                    f"fixed DAG tools must be read-only: {non_read_only!r}",
                )

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "workflow_id": self.workflow_id,
            "version": self.version,
            "active_slots": self.active_slots,
            "tasks": [task.to_dict() for task in self.tasks],
        }


def _stable_topological_ids(tasks: tuple[TaskDefinition, ...]) -> tuple[str, ...]:
    index = {task.task_id: position for position, task in enumerate(tasks)}
    indegree = {task.task_id: len(task.depends_on) for task in tasks}
    dependents: dict[str, list[str]] = {task.task_id: [] for task in tasks}
    for task in tasks:
        for dependency in task.depends_on:
            dependents[dependency].append(task.task_id)

    ready = [(index[task_id], task_id) for task_id, degree in indegree.items() if degree == 0]
    heapq.heapify(ready)
    ordered: list[str] = []
    while ready:
        _, task_id = heapq.heappop(ready)
        ordered.append(task_id)
        for dependent in dependents[task_id]:
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                heapq.heappush(ready, (index[dependent], dependent))
    if len(ordered) != len(tasks):
        cycle = _find_cycle(tasks)
        rendered = " -> ".join(cycle) if cycle else repr(sorted(set(indegree) - set(ordered)))
        raise DagValidationError(
            DagErrorCode.DEPENDENCY_CYCLE,
            f"workflow dependency cycle: {rendered}",
        )
    return tuple(ordered)


def _find_cycle(tasks: tuple[TaskDefinition, ...]) -> tuple[str, ...]:
    dependencies = {task.task_id: task.depends_on for task in tasks}
    state: dict[str, int] = {}
    stack: list[str] = []

    def visit(task_id: str) -> tuple[str, ...]:
        state[task_id] = 1
        stack.append(task_id)
        for dependency in dependencies[task_id]:
            if state.get(dependency, 0) == 0:
                cycle = visit(dependency)
                if cycle:
                    return cycle
            elif state.get(dependency) == 1:
                start = stack.index(dependency)
                return (*stack[start:], dependency)
        stack.pop()
        state[task_id] = 2
        return ()

    for task in tasks:
        if state.get(task.task_id, 0) == 0:
            cycle = visit(task.task_id)
            if cycle:
                return cycle
    return ()


def _validate_name(value: Any, field_name: str, *, maximum: int) -> None:
    if not isinstance(value, str) or not value or value != value.strip() or len(value) > maximum:
        raise DagValidationError(
            DagErrorCode.INVALID_FIELD,
            f"{field_name} must be non-empty, unpadded, and at most {maximum} characters",
        )


def _string_tuple(value: Any, field_name: str, *, maximum: int) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise DagValidationError(DagErrorCode.INVALID_FIELD, f"{field_name} must be a sequence of strings")
    result = tuple(value)
    if len(result) > maximum or any(not isinstance(item, str) for item in result):
        raise DagValidationError(
            DagErrorCode.INVALID_FIELD,
            f"{field_name} must contain at most {maximum} strings",
        )
    return result


def _validate_unique(values: tuple[str, ...], label: str) -> None:
    duplicates = _duplicates(values)
    if duplicates:
        raise DagValidationError(DagErrorCode.INVALID_FIELD, f"{label} contain duplicates: {duplicates!r}")


def _duplicates(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    return sorted(duplicates)


def _bounded_int(value: Any, field_name: str, minimum: int, maximum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise DagValidationError(
            DagErrorCode.INVALID_FIELD,
            f"{field_name} must be an integer between {minimum} and {maximum}",
        )


def _canonical_object(value: Any, field_name: str) -> dict[str, JsonValue]:
    if not isinstance(value, Mapping):
        raise DagValidationError(DagErrorCode.INVALID_FIELD, f"{field_name} must be an object")
    _validate_json_value(value, f"$.{field_name}")
    try:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
        decoded = json.loads(encoded)
    except (TypeError, ValueError, OverflowError) as exc:
        raise DagValidationError(DagErrorCode.INVALID_FIELD, f"{field_name} must contain only JSON values") from exc
    if not isinstance(decoded, dict):
        raise DagValidationError(DagErrorCode.INVALID_FIELD, f"{field_name} must be an object")
    return decoded


def _validate_json_value(value: Any, path: str) -> None:
    if value is None or isinstance(value, (bool, str, int)):
        return
    if isinstance(value, float):
        if math.isfinite(value):
            return
        raise DagValidationError(DagErrorCode.INVALID_FIELD, f"{path} contains a non-finite number")
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _validate_json_value(item, f"{path}[{index}]")
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise DagValidationError(DagErrorCode.INVALID_FIELD, f"{path} contains a non-string key")
            _validate_json_value(item, f"{path}.{key}")
        return
    raise DagValidationError(
        DagErrorCode.INVALID_FIELD,
        f"{path} contains unsupported value {type(value).__name__}",
    )
