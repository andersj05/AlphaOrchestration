"""Deterministic multi-criterion ranking with explicit missing-data policy."""

from __future__ import annotations

import math
from collections.abc import Mapping
from decimal import Decimal, localcontext
from typing import Any

from alpha_orchestration.domain import JsonValue

_DIRECTIONS = {"higher", "lower"}
_MISSING_POLICIES = {"exclude", "worst", "neutral"}


def rank_entities(arguments: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
    """Rank entities using tie-aware percentile scores and normalized weights."""

    precision = _precision(arguments.get("precision", 6))
    context = _context(arguments.get("context", {}))
    rows = _rows(arguments.get("rows"))
    criteria = _criteria(arguments.get("criteria"))
    _populate_row_values(rows, criteria)
    missing_policy = arguments.get("missing_policy")
    if missing_policy not in _MISSING_POLICIES:
        raise ValueError("missing_policy must be one of: exclude, neutral, worst")

    top_n_value = arguments.get("top_n")
    if top_n_value is None:
        top_n = len(rows)
    elif isinstance(top_n_value, bool) or not isinstance(top_n_value, int) or top_n_value <= 0:
        raise ValueError("top_n must be a positive integer")
    else:
        top_n = top_n_value

    eligible: list[dict[str, Any]] = []
    excluded: list[JsonValue] = []
    for row in rows:
        missing_metrics = [criterion["metric"] for criterion in criteria if row["values"][criterion["metric"]] is None]
        if missing_policy == "exclude" and missing_metrics:
            excluded.append(
                {
                    "id": row["id"],
                    "reason": "missing one or more ranking metrics",
                    "missing_metrics": missing_metrics,
                }
            )
        else:
            row["missing_metrics"] = missing_metrics
            eligible.append(row)

    scores_by_id: dict[str, dict[str, float]] = {row["id"]: {} for row in eligible}
    ranks_by_id: dict[str, dict[str, float | None]] = {row["id"]: {} for row in eligible}
    for criterion in criteria:
        metric = criterion["metric"]
        observed = [(row["id"], row["values"][metric]) for row in eligible if row["values"][metric] is not None]
        metric_scores, metric_ranks = _percentile_scores(observed, criterion["direction"])
        for row in eligible:
            entity_id = row["id"]
            value = row["values"][metric]
            if value is None:
                scores_by_id[entity_id][metric] = 0.0 if missing_policy == "worst" else 50.0
                ranks_by_id[entity_id][metric] = None
            else:
                scores_by_id[entity_id][metric] = metric_scores[entity_id]
                ranks_by_id[entity_id][metric] = metric_ranks[entity_id]

    weight_sum = math.fsum(criterion["weight"] for criterion in criteria)
    normalized_weights = {criterion["metric"]: criterion["weight"] / weight_sum for criterion in criteria}
    ranked: list[dict[str, Any]] = []
    for row in eligible:
        entity_id = row["id"]
        composite = math.fsum(
            scores_by_id[entity_id][criterion["metric"]] * normalized_weights[criterion["metric"]]
            for criterion in criteria
        )
        ranked.append(
            {
                "id": entity_id,
                "metrics": {
                    criterion["metric"]: row["values"][criterion["metric"]]
                    for criterion in criteria
                },
                "metric_ranks": {
                    criterion["metric"]: (
                        None
                        if ranks_by_id[entity_id][criterion["metric"]] is None
                        else _rounded(ranks_by_id[entity_id][criterion["metric"]], precision)
                    )
                    for criterion in criteria
                },
                "metric_scores": {
                    criterion["metric"]: _rounded(scores_by_id[entity_id][criterion["metric"]], precision)
                    for criterion in criteria
                },
                "composite_score": _rounded(composite, precision),
                "missing_metrics": row["missing_metrics"],
            }
        )

    ranked.sort(key=lambda row: (-row["composite_score"], row["id"]))
    returned = ranked[:top_n]
    return {
        "ranked": returned,
        "excluded": excluded,
        "normalized_weights": {
            criterion["metric"]: _rounded(normalized_weights[criterion["metric"]], precision)
            for criterion in criteria
        },
        "missing_policy": missing_policy,
        "input_count": len(rows),
        "eligible_count": len(eligible),
        "returned_count": len(returned),
        "context": context,
        "rounding": {"mode": "half_even", "decimal_places": precision},
        "formula_version": "entity-ranking-v1",
    }


def _percentile_scores(
    observed: list[tuple[str, float | None]],
    direction: str,
) -> tuple[dict[str, float], dict[str, float]]:
    ordered = sorted(observed, key=lambda item: item[1], reverse=direction == "higher")
    count = len(ordered)
    scores: dict[str, float] = {}
    ranks: dict[str, float] = {}
    index = 0
    while index < count:
        end = index + 1
        while end < count and ordered[end][1] == ordered[index][1]:
            end += 1
        average_rank = ((index + 1) + end) / 2
        percentile = 100.0 if count == 1 else 100.0 * (count - average_rank) / (count - 1)
        for entity_id, _value in ordered[index:end]:
            scores[entity_id] = percentile
            ranks[entity_id] = average_rank
        index = end
    return scores, ranks


def _rows(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise ValueError("rows must be a non-empty array")
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, raw in enumerate(value):
        if not isinstance(raw, Mapping):
            raise ValueError(f"rows[{index}] must be an object")
        entity_id = raw.get("id")
        if not isinstance(entity_id, str) or not entity_id.strip():
            raise ValueError(f"rows[{index}].id must be a non-empty string")
        if entity_id in seen:
            raise ValueError(f"duplicate row id: {entity_id}")
        seen.add(entity_id)
        metrics = raw.get("metrics")
        if not isinstance(metrics, Mapping):
            raise ValueError(f"rows[{index}].metrics must be an object")
        result.append({"id": entity_id, "raw_metrics": metrics})
    return result


def _criteria(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise ValueError("criteria must be a non-empty array")
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, raw in enumerate(value):
        if not isinstance(raw, Mapping):
            raise ValueError(f"criteria[{index}] must be an object")
        metric = raw.get("metric")
        if not isinstance(metric, str) or not metric.strip():
            raise ValueError(f"criteria[{index}].metric must be a non-empty string")
        if metric in seen:
            raise ValueError(f"duplicate criterion metric: {metric}")
        seen.add(metric)
        direction = raw.get("direction")
        if direction not in _DIRECTIONS:
            raise ValueError(f"criteria[{index}].direction must be one of: higher, lower")
        weight = _number(raw.get("weight"), f"criteria[{index}].weight")
        if weight < 0:
            raise ValueError(f"criteria[{index}].weight must be non-negative")
        result.append({"metric": metric, "direction": direction, "weight": weight})
    if math.fsum(criterion["weight"] for criterion in result) <= 0:
        raise ValueError("criteria weights must sum to more than zero")
    return result


def _populate_row_values(rows: list[dict[str, Any]], criteria: list[dict[str, Any]]) -> None:
    for row_index, row in enumerate(rows):
        metrics = row.pop("raw_metrics")
        values: dict[str, float | None] = {}
        for criterion in criteria:
            metric = criterion["metric"]
            raw_value = metrics.get(metric)
            values[metric] = None if raw_value is None else _number(raw_value, f"rows[{row_index}].metrics.{metric}")
        row["values"] = values


def _context(value: Any) -> dict[str, JsonValue]:
    if not isinstance(value, Mapping):
        raise ValueError("context must be an object")
    if any(not isinstance(key, str) for key in value):
        raise ValueError("context keys must be strings")
    return dict(value)


def _number(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a number")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{name} must be finite")
    return number


def _precision(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 12:
        raise ValueError("precision must be an integer between 0 and 12")
    return value


def _rounded(value: float, precision: int) -> float | int:
    with localcontext() as context:
        context.prec = 34
        quantum = Decimal(1).scaleb(-precision)
        rounded = Decimal(str(value)).quantize(quantum)
    if rounded == 0:
        rounded = abs(rounded)
    return int(rounded) if precision == 0 else float(rounded)
