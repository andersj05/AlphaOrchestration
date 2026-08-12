"""A deliberately small JSON-Schema validator for model-facing tool inputs.

Alpha only needs a strict, auditable subset of JSON Schema for its local tools.
Keeping that subset in-process avoids adding a large runtime dependency and, more
importantly, makes the validation behavior identical in offline tests and live runs.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any


class SchemaValidationError(ValueError):
    """Raised when a tool argument does not match its advertised schema."""


def validate_json(value: Any, schema: Mapping[str, Any], *, path: str = "$") -> None:
    """Validate ``value`` against the JSON-Schema subset used by Alpha tools."""

    if "anyOf" in schema:
        failures: list[str] = []
        for option in schema["anyOf"]:
            try:
                validate_json(value, option, path=path)
                break
            except SchemaValidationError as exc:
                failures.append(str(exc))
        else:
            raise SchemaValidationError(f"{path}: did not match any allowed schema ({'; '.join(failures)})")

    if "enum" in schema and value not in schema["enum"]:
        raise SchemaValidationError(f"{path}: expected one of {schema['enum']!r}")

    expected = schema.get("type")
    if expected is not None:
        allowed = [expected] if isinstance(expected, str) else list(expected)
        if not any(_matches_type(value, item) for item in allowed):
            joined = " or ".join(allowed)
            raise SchemaValidationError(f"{path}: expected {joined}, got {_json_type(value)}")

    if isinstance(value, Mapping):
        _validate_object(value, schema, path)
    elif isinstance(value, list):
        _validate_array(value, schema, path)
    elif isinstance(value, str):
        _validate_string(value, schema, path)
    elif _is_number(value):
        _validate_number(float(value), schema, path)


def _validate_object(value: Mapping[Any, Any], schema: Mapping[str, Any], path: str) -> None:
    if not all(isinstance(key, str) for key in value):
        raise SchemaValidationError(f"{path}: object keys must be strings")

    required = schema.get("required", [])
    missing = [key for key in required if key not in value]
    if missing:
        raise SchemaValidationError(f"{path}: missing required properties {missing!r}")

    minimum = schema.get("minProperties")
    maximum = schema.get("maxProperties")
    if minimum is not None and len(value) < minimum:
        raise SchemaValidationError(f"{path}: expected at least {minimum} properties")
    if maximum is not None and len(value) > maximum:
        raise SchemaValidationError(f"{path}: expected at most {maximum} properties")

    properties = schema.get("properties", {})
    additional = schema.get("additionalProperties", True)
    for key, item in value.items():
        child_path = f"{path}.{key}"
        if key in properties:
            validate_json(item, properties[key], path=child_path)
        elif additional is False:
            raise SchemaValidationError(f"{child_path}: unexpected property")
        elif isinstance(additional, Mapping):
            validate_json(item, additional, path=child_path)


def _validate_array(value: list[Any], schema: Mapping[str, Any], path: str) -> None:
    minimum = schema.get("minItems")
    maximum = schema.get("maxItems")
    if minimum is not None and len(value) < minimum:
        raise SchemaValidationError(f"{path}: expected at least {minimum} items")
    if maximum is not None and len(value) > maximum:
        raise SchemaValidationError(f"{path}: expected at most {maximum} items")
    if schema.get("uniqueItems"):
        fingerprints = [repr(item) for item in value]
        if len(fingerprints) != len(set(fingerprints)):
            raise SchemaValidationError(f"{path}: array items must be unique")
    if "items" in schema:
        for index, item in enumerate(value):
            validate_json(item, schema["items"], path=f"{path}[{index}]")


def _validate_string(value: str, schema: Mapping[str, Any], path: str) -> None:
    minimum = schema.get("minLength")
    maximum = schema.get("maxLength")
    if minimum is not None and len(value) < minimum:
        raise SchemaValidationError(f"{path}: string must contain at least {minimum} characters")
    if maximum is not None and len(value) > maximum:
        raise SchemaValidationError(f"{path}: string must contain at most {maximum} characters")


def _validate_number(value: float, schema: Mapping[str, Any], path: str) -> None:
    if not math.isfinite(value):
        raise SchemaValidationError(f"{path}: number must be finite")
    if "minimum" in schema and value < schema["minimum"]:
        raise SchemaValidationError(f"{path}: must be >= {schema['minimum']}")
    if "maximum" in schema and value > schema["maximum"]:
        raise SchemaValidationError(f"{path}: must be <= {schema['maximum']}")
    if "exclusiveMinimum" in schema and value <= schema["exclusiveMinimum"]:
        raise SchemaValidationError(f"{path}: must be > {schema['exclusiveMinimum']}")
    if "exclusiveMaximum" in schema and value >= schema["exclusiveMaximum"]:
        raise SchemaValidationError(f"{path}: must be < {schema['exclusiveMaximum']}")


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _matches_type(value: Any, expected: str) -> bool:
    return {
        "null": value is None,
        "boolean": isinstance(value, bool),
        "integer": isinstance(value, int) and not isinstance(value, bool),
        "number": _is_number(value),
        "string": isinstance(value, str),
        "array": isinstance(value, list),
        "object": isinstance(value, Mapping),
    }.get(expected, False)


def _json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, str):
        return "string"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, list):
        return "array"
    if isinstance(value, Mapping):
        return "object"
    return type(value).__name__
