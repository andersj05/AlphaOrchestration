"""Deterministic growth forecasting and discounted-cash-flow valuation.

The functions in this module intentionally accept only explicit assumptions.  They
perform the arithmetic that would otherwise be error-prone for a small language
model, while returning enough formula and timing metadata for that model to explain
or audit the result.
"""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal, InvalidOperation, localcontext
from typing import Any

from alpha_orchestration.domain import JsonValue

_DEFAULT_PRECISION = 6


def forecast_growth(arguments: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
    """Compound one or more explicit growth-rate scenarios from a positive base.

    ``labels`` (or its more explicit alias ``period_labels``) contains one label
    for each *forecast* period; it does not include a label for ``base_value``.
    A scenario may provide one growth rate, which is repeated for every period, or
    one rate per forecast period.  Rates are decimals (``0.10`` means 10%).
    """

    precision = _precision(arguments.get("precision", _DEFAULT_PRECISION))
    base_value = _decimal(arguments.get("base_value"), "base_value")
    if base_value <= 0:
        raise ValueError("base_value must be greater than zero")

    periods = arguments.get("periods")
    if isinstance(periods, bool) or not isinstance(periods, int) or periods <= 0:
        raise ValueError("periods must be a positive integer")

    labels = _labels(arguments, periods)
    unit = _unit(arguments.get("unit", "amount"))
    context = _context(arguments.get("context", {}))
    raw_scenarios = arguments.get("scenarios")
    if not isinstance(raw_scenarios, list) or not raw_scenarios:
        raise ValueError("scenarios must be a non-empty array")

    seen_names: set[str] = set()
    scenario_results: list[JsonValue] = []
    with localcontext() as decimal_context:
        decimal_context.prec = 50
        for scenario_index, raw_scenario in enumerate(raw_scenarios):
            if not isinstance(raw_scenario, Mapping):
                raise ValueError(f"scenarios[{scenario_index}] must be an object")
            name = _required_text(raw_scenario.get("name"), f"scenarios[{scenario_index}].name")
            if name in seen_names:
                raise ValueError(f"duplicate scenario name: {name}")
            seen_names.add(name)

            raw_rates = raw_scenario.get("growth_rates")
            if not isinstance(raw_rates, list):
                raise ValueError(f"scenarios[{scenario_index}].growth_rates must be an array")
            if len(raw_rates) not in {1, periods}:
                raise ValueError(
                    f"scenarios[{scenario_index}].growth_rates must contain one rate or exactly {periods} rates"
                )
            rates = [
                _growth_rate(value, f"scenarios[{scenario_index}].growth_rates[{rate_index}]")
                for rate_index, value in enumerate(raw_rates)
            ]
            rate_mode = "constant" if len(rates) == 1 else "period_specific"
            expanded_rates = rates * periods if len(rates) == 1 else rates

            value = base_value
            schedule: list[JsonValue] = []
            for period, (label, growth_rate) in enumerate(
                zip(labels, expanded_rates, strict=True),
                start=1,
            ):
                value *= Decimal(1) + growth_rate
                _require_finite(value, f"calculated value for scenario {name!r}, period {period}")
                schedule.append(
                    {
                        "period": period,
                        "label": label,
                        "growth_rate": _rounded(growth_rate, precision),
                        "value": _rounded(value, precision),
                    }
                )

            total_growth = value / base_value - Decimal(1)
            cagr = _decimal_root(value / base_value, periods) - Decimal(1)
            scenario_results.append(
                {
                    "name": name,
                    "rate_mode": rate_mode,
                    "growth_rates": [_rounded(rate, precision) for rate in expanded_rates],
                    "values": schedule,
                    "ending_value": _rounded(value, precision),
                    "total_growth": _rounded(total_growth, precision),
                    "cagr": _rounded(cagr, precision),
                }
            )

    return {
        "base_value": _rounded(base_value, precision),
        "periods": periods,
        "period_labels": labels,
        "scenarios": scenario_results,
        "unit": unit,
        "context": context,
        "formula": {
            "period_value": "value[t] = value[t-1] * (1 + growth_rate[t])",
            "total_growth": "ending_value / base_value - 1",
            "cagr": "(ending_value / base_value) ** (1 / periods) - 1",
        },
        "assumptions": {
            "growth_rates_are_decimals": True,
            "compounding": "each period compounds on the prior period value",
            "base_value_timing": "immediately before forecast period 1",
            "label_scope": "one label per forecast period; base value is not labeled",
        },
        "rounding": {
            "mode": "half_even",
            "decimal_places": precision,
            "calculations_use_unrounded_values": True,
        },
        "formula_version": "finance-growth-forecast-v1",
    }


def discounted_cash_flow(arguments: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
    """Discount explicit forecast cash flows and an explicit terminal-value method.

    End-of-period timing is the default.  With ``mid_year=True``, forecast period
    ``t`` and the terminal value are discounted at ``t - 0.5``.  For an exit
    multiple, ``terminal.metric_value`` is optional and defaults to the last cash
    flow; specifying it is preferable when the multiple is based on a different
    metric such as EBITDA.
    """

    precision = _precision(arguments.get("precision", _DEFAULT_PRECISION))
    cash_flows = _decimal_array(arguments.get("cash_flows"), "cash_flows")
    if not cash_flows:
        raise ValueError("cash_flows must be a non-empty array")

    discount_rate = _decimal(arguments.get("discount_rate"), "discount_rate")
    if discount_rate <= -1:
        raise ValueError("discount_rate must be greater than -1")
    discount_base = Decimal(1) + discount_rate

    labels = _labels(arguments, len(cash_flows))
    unit = _unit(arguments.get("unit", "amount"))
    context = _context(arguments.get("context", {}))
    net_debt = _decimal(arguments.get("net_debt", 0), "net_debt")
    mid_year = arguments.get("mid_year", False)
    if not isinstance(mid_year, bool):
        raise ValueError("mid_year must be a boolean")

    shares_outstanding: Decimal | None = None
    if "shares_outstanding" in arguments:
        shares_outstanding = _decimal(arguments["shares_outstanding"], "shares_outstanding")
        if shares_outstanding <= 0:
            raise ValueError("shares_outstanding must be greater than zero")

    raw_terminal = arguments.get("terminal")
    if not isinstance(raw_terminal, Mapping):
        raise ValueError("terminal must be an object")
    terminal_method = raw_terminal.get("method")
    if terminal_method not in {"perpetuity_growth", "exit_multiple"}:
        raise ValueError("terminal.method must be 'perpetuity_growth' or 'exit_multiple'")

    with localcontext() as decimal_context:
        decimal_context.prec = 50
        schedule: list[JsonValue] = []
        pv_forecast = Decimal(0)
        for period, (label, cash_flow) in enumerate(zip(labels, cash_flows, strict=True), start=1):
            discount_period = Decimal(period) - (Decimal("0.5") if mid_year else Decimal(0))
            discount_factor = _discount_factor(discount_base, period, mid_year)
            present_value = cash_flow * discount_factor
            _require_finite(present_value, f"present value for period {period}")
            pv_forecast += present_value
            schedule.append(
                {
                    "period": period,
                    "label": label,
                    "cash_flow": _rounded(cash_flow, precision),
                    "discount_period": _rounded(discount_period, 1 if mid_year else 0),
                    "discount_factor": _rounded(discount_factor, precision),
                    "present_value": _rounded(present_value, precision),
                }
            )

        terminal_value, terminal_inputs, terminal_formula = _terminal_value(
            raw_terminal,
            terminal_method,
            cash_flows[-1],
            discount_rate,
            precision,
        )
        terminal_discount_period = Decimal(len(cash_flows)) - (
            Decimal("0.5") if mid_year else Decimal(0)
        )
        terminal_discount_factor = _discount_factor(discount_base, len(cash_flows), mid_year)
        pv_terminal = terminal_value * terminal_discount_factor
        enterprise_value = pv_forecast + pv_terminal
        equity_value = enterprise_value - net_debt
        per_share_value = None if shares_outstanding is None else equity_value / shares_outstanding

        for value, name in (
            (terminal_value, "terminal value"),
            (pv_terminal, "present value of terminal value"),
            (enterprise_value, "enterprise value"),
            (equity_value, "equity value"),
        ):
            _require_finite(value, name)
        if per_share_value is not None:
            _require_finite(per_share_value, "per-share value")

    terminal_result: dict[str, JsonValue] = {
        "method": terminal_method,
        **terminal_inputs,
        "discount_period": _rounded(terminal_discount_period, 1 if mid_year else 0),
        "discount_factor": _rounded(terminal_discount_factor, precision),
        "terminal_value": _rounded(terminal_value, precision),
        "present_value": _rounded(pv_terminal, precision),
        "formula": terminal_formula,
    }
    per_share_unit = None if shares_outstanding is None else f"{unit}_per_share"

    return {
        "cash_flows": [_rounded(cash_flow, precision) for cash_flow in cash_flows],
        "period_labels": labels,
        "discount_rate": _rounded(discount_rate, precision),
        "mid_year": mid_year,
        "pv_schedule": schedule,
        "pv_forecast_cash_flows": _rounded(pv_forecast, precision),
        "terminal": terminal_result,
        "terminal_value": _rounded(terminal_value, precision),
        "pv_terminal_value": _rounded(pv_terminal, precision),
        "enterprise_value": _rounded(enterprise_value, precision),
        "net_debt": _rounded(net_debt, precision),
        "equity_value": _rounded(equity_value, precision),
        "shares_outstanding": None if shares_outstanding is None else _rounded(shares_outstanding, precision),
        "per_share_value": None if per_share_value is None else _rounded(per_share_value, precision),
        "unit": unit,
        "per_share_unit": per_share_unit,
        "context": context,
        "formula": {
            "forecast_present_value": "cash_flow[t] / (1 + discount_rate) ** discount_period[t]",
            "terminal_value": terminal_formula,
            "enterprise_value": "sum(forecast present values) + present value of terminal value",
            "equity_value": "enterprise_value - net_debt",
            "per_share_value": "equity_value / shares_outstanding",
        },
        "assumptions": {
            "rates_are_decimals": True,
            "cash_flow_timing": "mid-year" if mid_year else "end-of-period",
            "terminal_value_timing": "same discount period as the final forecast cash flow",
            "net_debt_bridge": "net debt is subtracted; a negative value represents net cash",
            "exit_multiple_metric_default": "last forecast cash flow",
        },
        "rounding": {
            "mode": "half_even",
            "decimal_places": precision,
            "calculations_use_unrounded_values": True,
        },
        "formula_version": "finance-dcf-v1",
    }


def _terminal_value(
    terminal: Mapping[str, Any],
    method: Any,
    last_cash_flow: Decimal,
    discount_rate: Decimal,
    precision: int,
) -> tuple[Decimal, dict[str, JsonValue], str]:
    if method == "perpetuity_growth":
        growth_rate = _growth_rate(terminal.get("growth_rate"), "terminal.growth_rate")
        if growth_rate >= discount_rate:
            raise ValueError("terminal.growth_rate must be less than discount_rate")
        terminal_cash_flow = last_cash_flow * (Decimal(1) + growth_rate)
        terminal_value = terminal_cash_flow / (discount_rate - growth_rate)
        return (
            terminal_value,
            {
                "growth_rate": _rounded(growth_rate, precision),
                "terminal_cash_flow": _rounded(terminal_cash_flow, precision),
            },
            "last_cash_flow * (1 + terminal_growth_rate) / (discount_rate - terminal_growth_rate)",
        )

    multiple = _decimal(terminal.get("multiple"), "terminal.multiple")
    if multiple <= 0:
        raise ValueError("terminal.multiple must be greater than zero")
    metric_was_supplied = "metric_value" in terminal
    metric_value = (
        _decimal(terminal["metric_value"], "terminal.metric_value") if metric_was_supplied else last_cash_flow
    )
    terminal_value = metric_value * multiple
    return (
        terminal_value,
        {
            "multiple": _rounded(multiple, precision),
            "metric_value": _rounded(metric_value, precision),
            "metric_source": "terminal.metric_value" if metric_was_supplied else "last forecast cash flow",
        },
        "terminal_metric_value * exit_multiple",
    )


def _labels(arguments: Mapping[str, Any], periods: int) -> list[str]:
    has_labels = "labels" in arguments
    has_period_labels = "period_labels" in arguments
    if has_labels and has_period_labels:
        raise ValueError("provide only one of labels or period_labels")
    raw_labels = arguments.get("labels" if has_labels else "period_labels")
    if raw_labels is None:
        return [f"Period {period}" for period in range(1, periods + 1)]
    if not isinstance(raw_labels, list) or len(raw_labels) != periods:
        raise ValueError(f"labels must contain exactly {periods} forecast-period labels")
    return [_required_text(label, f"labels[{index}]") for index, label in enumerate(raw_labels)]


def _decimal_array(value: Any, name: str) -> list[Decimal]:
    if not isinstance(value, list):
        raise ValueError(f"{name} must be an array")
    return [_decimal(item, f"{name}[{index}]") for index, item in enumerate(value)]


def _decimal(value: Any, name: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a number")
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:  # pragma: no cover - guarded above
        raise ValueError(f"{name} must be a number") from exc
    if not result.is_finite():
        raise ValueError(f"{name} must be finite")
    return result


def _growth_rate(value: Any, name: str) -> Decimal:
    rate = _decimal(value, name)
    if rate <= -1:
        raise ValueError(f"{name} must be greater than -1")
    return rate


def _precision(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 12:
        raise ValueError("precision must be an integer between 0 and 12")
    return value


def _unit(value: Any) -> str:
    return _required_text(value, "unit")


def _context(value: Any) -> dict[str, JsonValue]:
    if not isinstance(value, Mapping):
        raise ValueError("context must be an object")
    if any(not isinstance(key, str) for key in value):
        raise ValueError("context keys must be strings")
    return dict(value)


def _required_text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def _discount_factor(discount_base: Decimal, period: int, mid_year: bool) -> Decimal:
    denominator = discount_base**period
    if mid_year:
        denominator /= discount_base.sqrt()
    factor = Decimal(1) / denominator
    _require_finite(factor, "discount factor")
    return factor


def _decimal_root(value: Decimal, periods: int) -> Decimal:
    # ``value`` is positive because base value is positive and every rate is > -1.
    result = (value.ln() / Decimal(periods)).exp()
    _require_finite(result, "CAGR")
    return result


def _require_finite(value: Decimal, name: str) -> None:
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")


def _rounded(value: Decimal, precision: int) -> float | int:
    _require_finite(value, "calculated result")
    quantum = Decimal(1).scaleb(-precision)
    with localcontext() as decimal_context:
        decimal_context.prec = max(50, value.adjusted() + precision + 10) if value else 50
        rounded = value.quantize(quantum)
    if rounded == 0:
        rounded = abs(rounded)
    if precision == 0:
        return int(rounded)
    result = float(rounded)
    if result in {float("inf"), float("-inf")}:
        raise ValueError("calculated result is outside the finite JSON number range")
    return result
