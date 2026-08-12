"""Small, explicit arithmetic operations for model-authored calculation plans."""

from __future__ import annotations

import math
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation, localcontext
from statistics import median
from typing import Any

from alpha_orchestration.domain import JsonValue

SUPPORTED_OPERATIONS: tuple[str, ...] = (
    "sum",
    "difference",
    "product",
    "ratio",
    "percent_change",
    "basis_point_change",
    "cagr",
    "mean",
    "median",
    "weighted_average",
)


def calculate_arithmetic(arguments: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
    """Execute a bounded batch of named operations without evaluating expressions."""

    precision = _precision(arguments.get("precision", 6))
    raw_operations = arguments.get("operations")
    if not isinstance(raw_operations, list) or not raw_operations:
        raise ValueError("operations must be a non-empty array")

    seen: set[str] = set()
    results: list[JsonValue] = []
    for index, raw in enumerate(raw_operations):
        if not isinstance(raw, Mapping):
            raise ValueError(f"operations[{index}] must be an object")
        operation_id = str(raw.get("id", "")).strip()
        if not operation_id:
            raise ValueError(f"operations[{index}].id must not be empty")
        if operation_id in seen:
            raise ValueError(f"duplicate operation id: {operation_id}")
        seen.add(operation_id)
        operation = str(raw.get("operation", ""))
        if operation not in SUPPORTED_OPERATIONS:
            raise ValueError(f"unsupported operation: {operation}")
        results.append(_execute(operation_id, operation, raw, precision))

    context = arguments.get("context", {})
    return {
        "results": results,
        "context": dict(context) if isinstance(context, Mapping) else {},
        "rounding": {"mode": "half_even", "decimal_places": precision},
        "formula_version": "finance-arithmetic-v1",
    }


def _execute(
    operation_id: str,
    operation: str,
    raw: Mapping[str, Any],
    precision: int,
) -> dict[str, JsonValue]:
    with localcontext() as context:
        context.prec = 34
        if operation in {"sum", "product", "mean", "median", "weighted_average"}:
            values = _decimal_array(raw.get("values"), f"{operation_id}.values")
            if not values:
                raise ValueError(f"{operation_id}.values must not be empty")

        if operation == "sum":
            result = sum(values, Decimal(0))
            formula = "sum(values)"
            unit = "number"
        elif operation == "product":
            result = Decimal(1)
            for value in values:
                result *= value
            formula = "product(values)"
            unit = "number"
        elif operation == "mean":
            result = sum(values, Decimal(0)) / Decimal(len(values))
            formula = "sum(values) / count(values)"
            unit = "number"
        elif operation == "median":
            result = Decimal(str(median(values)))
            formula = "median(values)"
            unit = "number"
        elif operation == "weighted_average":
            weights = _decimal_array(raw.get("weights"), f"{operation_id}.weights")
            if len(values) != len(weights):
                raise ValueError(f"{operation_id}: values and weights must have equal length")
            if any(weight < 0 for weight in weights):
                raise ValueError(f"{operation_id}: weights must be non-negative")
            weight_sum = sum(weights, Decimal(0))
            if weight_sum == 0:
                raise ValueError(f"{operation_id}: weights must sum to more than zero")
            result = sum((value * weight for value, weight in zip(values, weights, strict=True)), Decimal(0))
            result /= weight_sum
            formula = "sum(value * weight) / sum(weights)"
            unit = "number"
        elif operation == "difference":
            current, prior = _current_prior(raw, operation_id)
            result = current - prior
            formula = "current - prior"
            unit = "number"
        elif operation == "ratio":
            numerator = _decimal(raw.get("numerator"), f"{operation_id}.numerator")
            denominator = _decimal(raw.get("denominator"), f"{operation_id}.denominator")
            result = _divide(numerator, denominator, operation_id)
            formula = "numerator / denominator"
            unit = "ratio"
        elif operation == "percent_change":
            current, prior = _current_prior(raw, operation_id)
            result = _divide(current - prior, prior, operation_id)
            formula = "(current - prior) / prior"
            unit = "ratio"
        elif operation == "basis_point_change":
            current, prior = _current_prior(raw, operation_id)
            result = (current - prior) * Decimal(10_000)
            formula = "(current - prior) * 10000"
            unit = "basis_points"
        elif operation == "cagr":
            start = _decimal(raw.get("start"), f"{operation_id}.start")
            end = _decimal(raw.get("end"), f"{operation_id}.end")
            periods = _decimal(raw.get("periods"), f"{operation_id}.periods")
            if start <= 0 or end <= 0:
                raise ValueError(f"{operation_id}: CAGR requires positive start and end values")
            if periods <= 0:
                raise ValueError(f"{operation_id}: periods must be greater than zero")
            result = Decimal(str(math.pow(float(end / start), float(Decimal(1) / periods)) - 1))
            formula = "(end / start) ** (1 / periods) - 1"
            unit = "ratio"
        else:  # pragma: no cover - guarded by SUPPORTED_OPERATIONS
            raise ValueError(f"unsupported operation: {operation}")

    return {
        "id": operation_id,
        "operation": operation,
        "value": _rounded(result, precision),
        "unit": unit,
        "formula": formula,
    }


def _current_prior(raw: Mapping[str, Any], operation_id: str) -> tuple[Decimal, Decimal]:
    return (
        _decimal(raw.get("current"), f"{operation_id}.current"),
        _decimal(raw.get("prior"), f"{operation_id}.prior"),
    )


def _divide(numerator: Decimal, denominator: Decimal, operation_id: str) -> Decimal:
    if denominator == 0:
        raise ValueError(f"{operation_id}: denominator must not be zero")
    return numerator / denominator


def _decimal_array(value: Any, name: str) -> list[Decimal]:
    if not isinstance(value, list):
        raise ValueError(f"{name} must be an array")
    return [_decimal(item, f"{name}[{index}]") for index, item in enumerate(value)]


def _decimal(value: Any, name: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a number")
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{name} must be finite")
    try:
        return Decimal(str(value))
    except InvalidOperation as exc:  # pragma: no cover - guarded above
        raise ValueError(f"{name} must be a number") from exc


def _precision(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 12:
        raise ValueError("precision must be an integer between 0 and 12")
    return value


def _rounded(value: Decimal, precision: int) -> float | int:
    quantum = Decimal(1).scaleb(-precision)
    rounded = value.quantize(quantum)
    if rounded == 0:
        rounded = abs(rounded)
    return int(rounded) if precision == 0 else float(rounded)
