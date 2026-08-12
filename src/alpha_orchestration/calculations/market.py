"""Deterministic market-return and benchmark statistics.

The functions in this module deliberately accept plain JSON-shaped data.  They
do not fetch prices, infer frequencies, or silently repair mismatched series;
those decisions belong at the data-normalization edge of the application.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from decimal import Decimal, localcontext
from typing import Any

from alpha_orchestration.domain import JsonValue


def market_statistics(arguments: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
    """Calculate a compact set of return, risk, and benchmark statistics.

    ``prices`` are converted to adjacent simple returns.  A supplied benchmark
    must already be aligned one-for-one with the resulting return observations.
    Metrics that are mathematically undefined are returned with ``value=None``
    and an ``unavailable_reason`` instead of leaking NaN or infinity into JSON.
    """

    precision = _precision(arguments.get("precision", 6))
    has_prices = "prices" in arguments
    has_returns = "returns" in arguments
    if has_prices == has_returns:
        raise ValueError("provide exactly one of prices or returns")

    periods_per_year = _positive_number(arguments.get("periods_per_year"), "periods_per_year")
    annual_risk_free_rate = _number(arguments.get("annual_risk_free_rate"), "annual_risk_free_rate")
    if annual_risk_free_rate <= -1:
        raise ValueError("annual_risk_free_rate must be greater than -1")

    if has_prices:
        prices = _number_array(arguments.get("prices"), "prices")
        if len(prices) < 2:
            raise ValueError("prices must contain at least 2 observations")
        if any(price <= 0 for price in prices):
            raise ValueError("prices must contain only positive values")
        returns = [
            _finite(current / prior - 1, "derived return")
            for prior, current in zip(prices, prices[1:], strict=False)
        ]
        input_kind = "prices"
        input_observations = len(prices)
    else:
        returns = _number_array(arguments.get("returns"), "returns")
        if not returns:
            raise ValueError("returns must not be empty")
        _validate_simple_returns(returns, "returns")
        input_kind = "returns"
        input_observations = len(returns)

    benchmark_value = arguments.get("benchmark_returns")
    benchmark: list[float] | None
    if benchmark_value is None:
        benchmark = None
    else:
        benchmark = _number_array(benchmark_value, "benchmark_returns")
        _validate_simple_returns(benchmark, "benchmark_returns")
        if len(benchmark) != len(returns):
            raise ValueError("benchmark_returns must have the same number of observations as returns")

    periodic_risk_free_rate = annual_risk_free_rate / periods_per_year
    total_return = _compound_return(returns)
    if total_return == -1:
        annualized_return = -1.0
    else:
        annualized_return = _finite(
            math.pow(1 + total_return, periods_per_year / len(returns)) - 1,
            "annualized return",
        )

    mean_return = math.fsum(returns) / len(returns)
    sample_volatility = _sample_standard_deviation(returns)
    annualized_volatility = (
        None if sample_volatility is None else _finite(sample_volatility * math.sqrt(periods_per_year), "volatility")
    )

    metrics: dict[str, JsonValue] = {
        "total_return": _available(
            total_return,
            precision,
            "ratio",
            "product(1 + period_return) - 1",
        ),
        "annualized_return": _available(
            annualized_return,
            precision,
            "ratio",
            "(1 + total_return) ** (periods_per_year / observations) - 1",
        ),
        "annualized_volatility": (
            _unavailable(
                "requires at least 2 return observations",
                "ratio",
                "sample_standard_deviation(returns) * sqrt(periods_per_year)",
            )
            if annualized_volatility is None
            else _available(
                annualized_volatility,
                precision,
                "ratio",
                "sample_standard_deviation(returns) * sqrt(periods_per_year)",
            )
        ),
        "sharpe_ratio": _sharpe_metric(
            mean_return,
            periodic_risk_free_rate,
            sample_volatility,
            periods_per_year,
            precision,
        ),
        "sortino_ratio": _sortino_metric(
            returns,
            mean_return,
            periodic_risk_free_rate,
            periods_per_year,
            precision,
        ),
        "max_drawdown": _available(
            _max_drawdown(returns),
            precision,
            "ratio",
            "min(cumulative_wealth / running_peak - 1)",
        ),
        "best_period_return": _available(max(returns), precision, "ratio", "max(returns)"),
        "worst_period_return": _available(min(returns), precision, "ratio", "min(returns)"),
        "positive_period_ratio": _available(
            sum(value > 0 for value in returns) / len(returns),
            precision,
            "ratio",
            "count(returns > 0) / observations",
        ),
    }
    metrics.update(
        _benchmark_metrics(
            returns,
            benchmark,
            periodic_risk_free_rate,
            periods_per_year,
            precision,
        )
    )

    context = arguments.get("context", {})
    return {
        "input_kind": input_kind,
        "input_observations": input_observations,
        "return_observations": len(returns),
        "benchmark_observations": 0 if benchmark is None else len(benchmark),
        "periods_per_year": periods_per_year,
        "annual_risk_free_rate": annual_risk_free_rate,
        "metrics": metrics,
        "context": dict(context) if isinstance(context, Mapping) else {},
        "rounding": {"mode": "half_even", "decimal_places": precision},
        "formula_version": "market-statistics-v1",
    }


def _sharpe_metric(
    mean_return: float,
    periodic_risk_free_rate: float,
    sample_volatility: float | None,
    periods_per_year: float,
    precision: int,
) -> dict[str, JsonValue]:
    formula = "(mean(returns) - annual_risk_free_rate / periods_per_year) / sample_volatility * sqrt(periods_per_year)"
    if sample_volatility is None:
        return _unavailable("requires at least 2 return observations", "ratio", formula)
    if sample_volatility == 0:
        return _unavailable("sample volatility is zero", "ratio", formula)
    value = (mean_return - periodic_risk_free_rate) / sample_volatility * math.sqrt(periods_per_year)
    return _available(value, precision, "ratio", formula)


def _sortino_metric(
    returns: list[float],
    mean_return: float,
    periodic_risk_free_rate: float,
    periods_per_year: float,
    precision: int,
) -> dict[str, JsonValue]:
    formula = (
        "annualized_mean_excess_return / "
        "(sqrt(mean(min(return - periodic_risk_free_rate, 0) ** 2)) * sqrt(periods_per_year))"
    )
    downside_squared = [min(value - periodic_risk_free_rate, 0.0) ** 2 for value in returns]
    downside_deviation = math.sqrt(math.fsum(downside_squared) / len(returns))
    if downside_deviation == 0:
        return _unavailable("no returns below the periodic risk-free rate", "ratio", formula)
    numerator = (mean_return - periodic_risk_free_rate) * periods_per_year
    denominator = downside_deviation * math.sqrt(periods_per_year)
    return _available(numerator / denominator, precision, "ratio", formula)


def _benchmark_metrics(
    returns: list[float],
    benchmark: list[float] | None,
    periodic_risk_free_rate: float,
    periods_per_year: float,
    precision: int,
) -> dict[str, JsonValue]:
    beta_formula = "sample_covariance(returns, benchmark_returns) / sample_variance(benchmark_returns)"
    alpha_formula = (
        "(mean(returns) - periodic_risk_free_rate - beta * "
        "(mean(benchmark_returns) - periodic_risk_free_rate)) * periods_per_year"
    )
    correlation_formula = (
        "sample_covariance(returns, benchmark_returns) / (sample_std(returns) * sample_std(benchmark_returns))"
    )
    if benchmark is None:
        reason = "benchmark_returns was not provided"
        return {
            "beta": _unavailable(reason, "ratio", beta_formula),
            "alpha": _unavailable(reason, "ratio", alpha_formula),
            "correlation": _unavailable(reason, "ratio", correlation_formula),
        }
    if len(returns) < 2:
        reason = "requires at least 2 aligned return observations"
        return {
            "beta": _unavailable(reason, "ratio", beta_formula),
            "alpha": _unavailable(reason, "ratio", alpha_formula),
            "correlation": _unavailable(reason, "ratio", correlation_formula),
        }

    benchmark_variance = _sample_variance(benchmark)
    if benchmark_variance == 0:
        reason = "benchmark sample variance is zero"
        return {
            "beta": _unavailable(reason, "ratio", beta_formula),
            "alpha": _unavailable(
                "beta is unavailable because benchmark sample variance is zero",
                "ratio",
                alpha_formula,
            ),
            "correlation": _unavailable(reason, "ratio", correlation_formula),
        }

    covariance = _sample_covariance(returns, benchmark)
    beta = covariance / benchmark_variance
    mean_return = math.fsum(returns) / len(returns)
    benchmark_mean = math.fsum(benchmark) / len(benchmark)
    alpha = (
        mean_return
        - periodic_risk_free_rate
        - beta * (benchmark_mean - periodic_risk_free_rate)
    ) * periods_per_year
    asset_standard_deviation = _sample_standard_deviation(returns)
    benchmark_standard_deviation = math.sqrt(benchmark_variance)
    correlation = (
        _unavailable("return sample variance is zero", "ratio", correlation_formula)
        if asset_standard_deviation == 0
        else _available(
            covariance / (asset_standard_deviation * benchmark_standard_deviation),
            precision,
            "ratio",
            correlation_formula,
        )
    )
    return {
        "beta": _available(beta, precision, "ratio", beta_formula),
        "alpha": _available(alpha, precision, "ratio", alpha_formula),
        "correlation": correlation,
    }


def _max_drawdown(returns: list[float]) -> float:
    wealth = 1.0
    peak = 1.0
    maximum_drawdown = 0.0
    for value in returns:
        wealth = _finite(wealth * (1 + value), "cumulative wealth")
        peak = max(peak, wealth)
        drawdown = -1.0 if peak == 0 else wealth / peak - 1
        maximum_drawdown = min(maximum_drawdown, drawdown)
    return maximum_drawdown


def _compound_return(returns: list[float]) -> float:
    wealth = 1.0
    for value in returns:
        wealth = _finite(wealth * (1 + value), "compounded return")
    return _finite(wealth - 1, "total return")


def _sample_standard_deviation(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    return math.sqrt(_sample_variance(values))


def _sample_variance(values: list[float]) -> float:
    mean = math.fsum(values) / len(values)
    return _finite(math.fsum((value - mean) ** 2 for value in values) / (len(values) - 1), "sample variance")


def _sample_covariance(left: list[float], right: list[float]) -> float:
    left_mean = math.fsum(left) / len(left)
    right_mean = math.fsum(right) / len(right)
    return _finite(
        math.fsum(
            (left_value - left_mean) * (right_value - right_mean)
            for left_value, right_value in zip(left, right, strict=True)
        )
        / (len(left) - 1),
        "sample covariance",
    )


def _available(value: float, precision: int, unit: str, formula: str) -> dict[str, JsonValue]:
    return {
        "value": _rounded(_finite(value, "metric"), precision),
        "unit": unit,
        "formula": formula,
    }


def _unavailable(reason: str, unit: str, formula: str) -> dict[str, JsonValue]:
    return {
        "value": None,
        "unit": unit,
        "formula": formula,
        "unavailable_reason": reason,
    }


def _number_array(value: Any, name: str) -> list[float]:
    if not isinstance(value, list):
        raise ValueError(f"{name} must be an array")
    return [_number(item, f"{name}[{index}]") for index, item in enumerate(value)]


def _number(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a number")
    return _finite(float(value), name)


def _positive_number(value: Any, name: str) -> float:
    number = _number(value, name)
    if number <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return number


def _validate_simple_returns(values: list[float], name: str) -> None:
    for index, value in enumerate(values):
        if value < -1:
            raise ValueError(f"{name}[{index}] must be greater than or equal to -1")


def _finite(value: float, name: str) -> float:
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite")
    return value


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
