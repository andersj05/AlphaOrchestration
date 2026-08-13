"""Deterministic, provider-neutral public-equity financial metrics.

The model selects metric names and supplies normalized facts.  This module owns
all arithmetic, denominator checks, derivations, rounding, and formula metadata;
it deliberately does not guess periods, currencies, or missing financial facts.
Ratios are returned as decimals (``0.125`` means 12.5%).
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation, localcontext
from typing import Any

from alpha_orchestration.domain import JsonValue


@dataclass(frozen=True, slots=True)
class MetricDefinition:
    """Stable metadata advertised for a supported metric."""

    category: str
    unit: str
    formula: str


_DEFINITIONS: dict[str, MetricDefinition] = {
    "gross_profit": MetricDefinition("derived_amount", "currency", "revenue - cost_of_revenue"),
    "free_cash_flow": MetricDefinition(
        "derived_amount", "currency", "operating_cash_flow - capital_expenditures"
    ),
    "working_capital": MetricDefinition("derived_amount", "currency", "current_assets - current_liabilities"),
    "net_debt": MetricDefinition("derived_amount", "currency", "total_debt - cash_and_equivalents"),
    "enterprise_value": MetricDefinition(
        "derived_amount",
        "currency",
        "market_cap + total_debt + preferred_stock + minority_interest - cash_and_equivalents",
    ),
    "earnings_per_share": MetricDefinition(
        "derived_amount", "currency_per_share", "net_income / diluted_shares_outstanding"
    ),
    "book_value_per_share": MetricDefinition(
        "derived_amount", "currency_per_share", "shareholders_equity / diluted_shares_outstanding"
    ),
    "revenue_growth": MetricDefinition("growth", "ratio", "(revenue - prior_revenue) / prior_revenue"),
    "gross_profit_growth": MetricDefinition(
        "growth", "ratio", "(gross_profit - prior_gross_profit) / prior_gross_profit"
    ),
    "operating_income_growth": MetricDefinition(
        "growth", "ratio", "(operating_income - prior_operating_income) / prior_operating_income"
    ),
    "net_income_growth": MetricDefinition(
        "growth", "ratio", "(net_income - prior_net_income) / prior_net_income"
    ),
    "earnings_per_share_growth": MetricDefinition(
        "growth", "ratio", "(earnings_per_share - prior_earnings_per_share) / prior_earnings_per_share"
    ),
    "gross_margin": MetricDefinition("margin", "ratio", "gross_profit / revenue"),
    "operating_margin": MetricDefinition("margin", "ratio", "operating_income / revenue"),
    "ebit_margin": MetricDefinition("margin", "ratio", "ebit / revenue"),
    "ebitda_margin": MetricDefinition("margin", "ratio", "ebitda / revenue"),
    "net_margin": MetricDefinition("margin", "ratio", "net_income / revenue"),
    "operating_cash_flow_margin": MetricDefinition("margin", "ratio", "operating_cash_flow / revenue"),
    "free_cash_flow_margin": MetricDefinition("margin", "ratio", "free_cash_flow / revenue"),
    "capex_intensity": MetricDefinition("margin", "ratio", "capital_expenditures / revenue"),
    "incremental_operating_margin": MetricDefinition(
        "margin",
        "ratio",
        "(operating_income - prior_operating_income) / (revenue - prior_revenue)",
    ),
    "current_ratio": MetricDefinition("liquidity", "ratio", "current_assets / current_liabilities"),
    "quick_ratio": MetricDefinition("liquidity", "ratio", "quick_assets / current_liabilities"),
    "debt_to_equity": MetricDefinition("leverage", "ratio", "total_debt / shareholders_equity"),
    "debt_to_ebitda": MetricDefinition("leverage", "multiple", "total_debt / ebitda"),
    "net_debt_to_ebitda": MetricDefinition("leverage", "multiple", "net_debt / ebitda"),
    "interest_coverage": MetricDefinition("leverage", "multiple", "ebit / interest_expense"),
    "asset_turnover": MetricDefinition("efficiency", "multiple", "revenue / average_total_assets"),
    "return_on_assets": MetricDefinition("return", "ratio", "net_income / average_total_assets"),
    "return_on_equity": MetricDefinition("return", "ratio", "net_income / average_shareholders_equity"),
    "return_on_invested_capital": MetricDefinition(
        "return", "ratio", "net_operating_profit_after_tax / average_invested_capital"
    ),
    "cash_conversion_ratio": MetricDefinition("efficiency", "ratio", "operating_cash_flow / net_income"),
    "days_sales_outstanding": MetricDefinition(
        "cash_conversion", "days", "average_accounts_receivable / revenue * days_in_period"
    ),
    "days_inventory_outstanding": MetricDefinition(
        "cash_conversion", "days", "average_inventory / cost_of_revenue * days_in_period"
    ),
    "days_payables_outstanding": MetricDefinition(
        "cash_conversion", "days", "average_accounts_payable / cost_of_revenue * days_in_period"
    ),
    "cash_conversion_cycle": MetricDefinition(
        "cash_conversion",
        "days",
        "days_sales_outstanding + days_inventory_outstanding - days_payables_outstanding",
    ),
    "price_to_earnings": MetricDefinition("valuation", "multiple", "market_cap / net_income"),
    "price_to_sales": MetricDefinition("valuation", "multiple", "market_cap / revenue"),
    "price_to_book": MetricDefinition("valuation", "multiple", "market_cap / shareholders_equity"),
    "enterprise_value_to_revenue": MetricDefinition("valuation", "multiple", "enterprise_value / revenue"),
    "enterprise_value_to_ebitda": MetricDefinition("valuation", "multiple", "enterprise_value / ebitda"),
    "enterprise_value_to_ebit": MetricDefinition("valuation", "multiple", "enterprise_value / ebit"),
    "free_cash_flow_yield": MetricDefinition("valuation", "ratio", "free_cash_flow / market_cap"),
    "earnings_yield": MetricDefinition("valuation", "ratio", "net_income / market_cap"),
    "dividend_yield": MetricDefinition("valuation", "ratio", "dividends_paid / market_cap"),
    "payout_ratio": MetricDefinition("valuation", "ratio", "dividends_paid / net_income"),
    "rule_of_40": MetricDefinition("composite", "ratio", "revenue_growth + ebitda_margin"),
}

SUPPORTED_METRICS: tuple[str, ...] = tuple(_DEFINITIONS)
METRIC_NAMES: tuple[str, ...] = SUPPORTED_METRICS

_ALIASES: dict[str, tuple[str, ...]] = {
    "revenue": ("sales",),
    "prior_revenue": ("prior_sales",),
    "cost_of_revenue": ("cost_of_goods_sold", "cogs"),
    "prior_cost_of_revenue": ("prior_cost_of_goods_sold", "prior_cogs"),
    "capital_expenditures": ("capex",),
    "cash_and_equivalents": ("cash",),
    "market_cap": ("market_capitalization",),
    "total_debt": ("debt",),
    "shareholders_equity": ("stockholders_equity", "total_equity", "common_equity"),
    "average_shareholders_equity": ("average_stockholders_equity", "average_total_equity"),
    "diluted_shares_outstanding": ("weighted_average_diluted_shares", "shares_outstanding"),
    "prior_diluted_shares_outstanding": (
        "prior_weighted_average_diluted_shares",
        "prior_shares_outstanding",
    ),
    "preferred_stock": ("preferred_equity",),
    "minority_interest": ("noncontrolling_interest",),
    "ebit": ("earnings_before_interest_and_taxes",),
    "ebitda": ("earnings_before_interest_taxes_depreciation_amortization",),
    "depreciation_and_amortization": ("depreciation_amortization",),
    "interest_expense": ("net_interest_expense",),
    "average_total_assets": ("average_assets",),
    "average_accounts_receivable": ("average_receivables",),
    "average_accounts_payable": ("average_payables",),
    "net_operating_profit_after_tax": ("nopat",),
    "average_invested_capital": ("avg_invested_capital",),
    "effective_tax_rate": ("tax_rate",),
    "dividends_paid": ("common_dividends_paid", "cash_dividends_paid"),
}


@dataclass(frozen=True, slots=True)
class _Component:
    name: str
    value: Decimal | None
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class _Evaluation:
    value: Decimal | None
    inputs: dict[str, Decimal]
    reason: str | None = None
    formula: str | None = None


def calculate_metrics(arguments: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
    """Calculate a selected set of financial metrics from normalized numeric facts.

    Omitted inputs and zero denominators make only the affected metric unavailable.
    Structurally invalid requests (unknown metrics, non-numeric facts, or invalid
    precision) raise ``ValueError`` so the tool registry can return a repairable
    ``invalid_arguments`` response.
    """

    precision = _precision(arguments.get("precision", 6))
    raw_values = arguments.get("values")
    if not isinstance(raw_values, Mapping):
        raise ValueError("values must be an object")
    numeric_values = _numeric_values(raw_values)
    requested = _requested_metrics(arguments.get("metrics"))

    raw_context = arguments.get("context", {})
    if not isinstance(raw_context, Mapping):
        raise ValueError("context must be an object")
    if any(not isinstance(key, str) for key in raw_context):
        raise ValueError("context keys must be strings")

    calculator = _MetricCalculator(numeric_values)
    calculated: dict[str, JsonValue] = {}
    details: dict[str, JsonValue] = {}
    unavailable: dict[str, JsonValue] = {}
    with localcontext() as decimal_context:
        decimal_context.prec = 50
        decimal_context.rounding = ROUND_HALF_EVEN
        for metric in requested:
            evaluation = calculator.evaluate(metric)
            definition = _DEFINITIONS[metric]
            details[metric] = {
                "category": definition.category,
                "unit": definition.unit,
                "formula": evaluation.formula or definition.formula,
                "inputs": {
                    name: _rounded(value, precision) for name, value in evaluation.inputs.items()
                },
            }
            if evaluation.value is None:
                unavailable[metric] = evaluation.reason or "metric is unavailable"
            elif not evaluation.value.is_finite():
                unavailable[metric] = "calculation produced a non-finite result"
            else:
                calculated[metric] = _rounded(evaluation.value, precision)

    return {
        "values": calculated,
        "details": details,
        "unavailable": unavailable,
        "context": dict(raw_context),
        "rounding": {"mode": "half_even", "decimal_places": precision},
        "formula_version": "finance-metrics-v1",
    }


class _MetricCalculator:
    def __init__(self, values: Mapping[str, Decimal]) -> None:
        self.values = values
        self._metric_cache: dict[str, _Evaluation] = {}

    def evaluate(self, metric: str) -> _Evaluation:
        cached = self._metric_cache.get(metric)
        if cached is not None:
            return cached

        if metric in {
            "gross_profit",
            "free_cash_flow",
            "working_capital",
            "net_debt",
            "enterprise_value",
            "earnings_per_share",
            "book_value_per_share",
        }:
            result = self._derived_metric(metric)
        elif metric in {
            "revenue_growth",
            "gross_profit_growth",
            "operating_income_growth",
            "net_income_growth",
            "earnings_per_share_growth",
        }:
            result = self._growth_metric(metric)
        elif metric == "incremental_operating_margin":
            result = self._incremental_operating_margin()
        elif metric in {
            "gross_margin",
            "operating_margin",
            "ebit_margin",
            "ebitda_margin",
            "net_margin",
            "operating_cash_flow_margin",
            "free_cash_flow_margin",
            "capex_intensity",
            "current_ratio",
            "quick_ratio",
            "debt_to_equity",
            "debt_to_ebitda",
            "net_debt_to_ebitda",
            "interest_coverage",
            "asset_turnover",
            "return_on_assets",
            "return_on_equity",
            "return_on_invested_capital",
            "cash_conversion_ratio",
            "price_to_earnings",
            "price_to_sales",
            "price_to_book",
            "enterprise_value_to_revenue",
            "enterprise_value_to_ebitda",
            "enterprise_value_to_ebit",
            "free_cash_flow_yield",
            "earnings_yield",
            "dividend_yield",
            "payout_ratio",
        }:
            result = self._ratio_metric(metric)
        elif metric in {
            "days_sales_outstanding",
            "days_inventory_outstanding",
            "days_payables_outstanding",
        }:
            result = self._days_metric(metric)
        elif metric == "cash_conversion_cycle":
            result = self._cash_conversion_cycle()
        elif metric == "rule_of_40":
            result = self._rule_of_40()
        else:  # pragma: no cover - guarded by _requested_metrics
            raise ValueError(f"unsupported metric: {metric}")

        self._metric_cache[metric] = result
        return result

    def _derived_metric(self, metric: str) -> _Evaluation:
        difference_inputs = {
            "gross_profit": ("revenue", "cost_of_revenue"),
            "free_cash_flow": ("operating_cash_flow", "capital_expenditures"),
            "working_capital": ("current_assets", "current_liabilities"),
            "net_debt": ("total_debt", "cash_and_equivalents"),
        }
        if metric in difference_inputs:
            left_name, right_name = difference_inputs[metric]
            left = self._direct(left_name)
            right = self._direct(right_name)
            unavailable = self._unavailable(left, right)
            if unavailable is not None:
                return unavailable
            assert left.value is not None and right.value is not None
            return _Evaluation(left.value - right.value, self._inputs(left, right))

        if metric == "enterprise_value":
            market_cap = self._market_cap()
            debt = self._direct("total_debt")
            cash = self._direct("cash_and_equivalents")
            unavailable = self._unavailable(market_cap, debt, cash)
            if unavailable is not None:
                return unavailable
            preferred = self._optional_zero("preferred_stock")
            minority = self._optional_zero("minority_interest")
            assert market_cap.value is not None and debt.value is not None and cash.value is not None
            assert preferred.value is not None and minority.value is not None
            value = market_cap.value + debt.value + preferred.value + minority.value - cash.value
            return _Evaluation(value, self._inputs(market_cap, debt, preferred, minority, cash))

        numerator_name = "net_income" if metric == "earnings_per_share" else "shareholders_equity"
        numerator = self._direct(numerator_name)
        shares = self._direct("diluted_shares_outstanding")
        return self._ratio(metric, numerator, shares)

    def _growth_metric(self, metric: str) -> _Evaluation:
        components = {
            "revenue_growth": ("revenue", "prior_revenue"),
            "gross_profit_growth": ("gross_profit", "prior_gross_profit"),
            "operating_income_growth": ("operating_income", "prior_operating_income"),
            "net_income_growth": ("net_income", "prior_net_income"),
            "earnings_per_share_growth": ("earnings_per_share", "prior_earnings_per_share"),
        }
        current_name, prior_name = components[metric]
        current = self._component(current_name)
        prior = self._component(prior_name)
        unavailable = self._unavailable(current, prior)
        if unavailable is not None:
            return unavailable
        assert current.value is not None and prior.value is not None
        if prior.value == 0:
            return _Evaluation(None, self._inputs(current, prior), f"zero denominator: {prior.name}")
        return _Evaluation((current.value - prior.value) / prior.value, self._inputs(current, prior))

    def _incremental_operating_margin(self) -> _Evaluation:
        operating_income = self._direct("operating_income")
        prior_operating_income = self._direct("prior_operating_income")
        revenue = self._direct("revenue")
        prior_revenue = self._direct("prior_revenue")
        components = (operating_income, prior_operating_income, revenue, prior_revenue)
        unavailable = self._unavailable(*components)
        if unavailable is not None:
            return unavailable
        assert operating_income.value is not None and prior_operating_income.value is not None
        assert revenue.value is not None and prior_revenue.value is not None
        revenue_change = revenue.value - prior_revenue.value
        if revenue_change == 0:
            return _Evaluation(None, self._inputs(*components), "zero denominator: revenue - prior_revenue")
        value = (operating_income.value - prior_operating_income.value) / revenue_change
        return _Evaluation(value, self._inputs(*components))

    def _ratio_metric(self, metric: str) -> _Evaluation:
        pairs = {
            "gross_margin": ("gross_profit", "revenue"),
            "operating_margin": ("operating_income", "revenue"),
            "ebit_margin": ("ebit", "revenue"),
            "ebitda_margin": ("ebitda", "revenue"),
            "net_margin": ("net_income", "revenue"),
            "operating_cash_flow_margin": ("operating_cash_flow", "revenue"),
            "free_cash_flow_margin": ("free_cash_flow", "revenue"),
            "capex_intensity": ("capital_expenditures", "revenue"),
            "current_ratio": ("current_assets", "current_liabilities"),
            "quick_ratio": ("quick_assets", "current_liabilities"),
            "debt_to_equity": ("total_debt", "shareholders_equity"),
            "debt_to_ebitda": ("total_debt", "ebitda"),
            "net_debt_to_ebitda": ("net_debt", "ebitda"),
            "interest_coverage": ("ebit", "interest_expense"),
            "asset_turnover": ("revenue", "average_total_assets"),
            "return_on_assets": ("net_income", "average_total_assets"),
            "return_on_equity": ("net_income", "average_shareholders_equity"),
            "return_on_invested_capital": ("net_operating_profit_after_tax", "average_invested_capital"),
            "cash_conversion_ratio": ("operating_cash_flow", "net_income"),
            "price_to_earnings": ("market_cap", "net_income"),
            "price_to_sales": ("market_cap", "revenue"),
            "price_to_book": ("market_cap", "shareholders_equity"),
            "enterprise_value_to_revenue": ("enterprise_value", "revenue"),
            "enterprise_value_to_ebitda": ("enterprise_value", "ebitda"),
            "enterprise_value_to_ebit": ("enterprise_value", "ebit"),
            "free_cash_flow_yield": ("free_cash_flow", "market_cap"),
            "earnings_yield": ("net_income", "market_cap"),
            "dividend_yield": ("dividends_paid", "market_cap"),
            "payout_ratio": ("dividends_paid", "net_income"),
        }
        numerator_name, denominator_name = pairs[metric]
        return self._ratio(metric, self._component(numerator_name), self._component(denominator_name))

    def _days_metric(self, metric: str) -> _Evaluation:
        pairs = {
            "days_sales_outstanding": ("average_accounts_receivable", "revenue"),
            "days_inventory_outstanding": ("average_inventory", "cost_of_revenue"),
            "days_payables_outstanding": ("average_accounts_payable", "cost_of_revenue"),
        }
        numerator_name, denominator_name = pairs[metric]
        numerator = self._component(numerator_name)
        denominator = self._component(denominator_name)
        days = self._days_in_period()
        unavailable = self._unavailable(numerator, denominator)
        if unavailable is not None:
            return unavailable
        assert numerator.value is not None and denominator.value is not None and days.value is not None
        if denominator.value == 0:
            return _Evaluation(
                None,
                self._inputs(numerator, denominator, days),
                f"zero denominator: {denominator.name}",
            )
        return _Evaluation(
            numerator.value / denominator.value * days.value,
            self._inputs(numerator, denominator, days),
        )

    def _cash_conversion_cycle(self) -> _Evaluation:
        dso = self.evaluate("days_sales_outstanding")
        dio = self.evaluate("days_inventory_outstanding")
        dpo = self.evaluate("days_payables_outstanding")
        evaluations = {
            "days_sales_outstanding": dso,
            "days_inventory_outstanding": dio,
            "days_payables_outstanding": dpo,
        }
        inputs = {name: item.value for name, item in evaluations.items() if item.value is not None}
        failures = [f"{name}: {item.reason}" for name, item in evaluations.items() if item.value is None]
        if failures:
            return _Evaluation(None, inputs, "; ".join(failures))
        assert dso.value is not None and dio.value is not None and dpo.value is not None
        return _Evaluation(dso.value + dio.value - dpo.value, inputs)

    def _rule_of_40(self) -> _Evaluation:
        growth = self._revenue_growth()
        margin = self._ebitda_margin()
        unavailable = self._unavailable(growth, margin)
        if unavailable is not None:
            return unavailable
        assert growth.value is not None and margin.value is not None
        return _Evaluation(growth.value + margin.value, self._inputs(growth, margin))

    def _ratio(self, metric: str, numerator: _Component, denominator: _Component) -> _Evaluation:
        unavailable = self._unavailable(numerator, denominator)
        if unavailable is not None:
            return unavailable
        assert numerator.value is not None and denominator.value is not None
        if denominator.value == 0:
            return _Evaluation(
                None,
                self._inputs(numerator, denominator),
                f"zero denominator: {denominator.name}",
            )
        return _Evaluation(numerator.value / denominator.value, self._inputs(numerator, denominator))

    def _component(self, name: str) -> _Component:
        special = {
            "gross_profit": self._gross_profit,
            "prior_gross_profit": self._prior_gross_profit,
            "free_cash_flow": self._free_cash_flow,
            "net_debt": self._net_debt,
            "enterprise_value": self._enterprise_value,
            "earnings_per_share": self._earnings_per_share,
            "prior_earnings_per_share": self._prior_earnings_per_share,
            "market_cap": self._market_cap,
            "quick_assets": self._quick_assets,
            "ebit": self._ebit,
            "ebitda": self._ebitda,
            "average_total_assets": self._average_total_assets,
            "average_shareholders_equity": self._average_shareholders_equity,
            "average_accounts_receivable": self._average_accounts_receivable,
            "average_inventory": self._average_inventory,
            "average_accounts_payable": self._average_accounts_payable,
            "net_operating_profit_after_tax": self._net_operating_profit_after_tax,
            "average_invested_capital": self._average_invested_capital,
            "dividends_paid": self._dividends_paid,
        }
        resolver = special.get(name)
        return resolver() if resolver is not None else self._direct(name)

    def _gross_profit(self) -> _Component:
        direct = self._direct("gross_profit")
        if direct.value is not None:
            return direct
        return self._derived_difference("gross_profit", "revenue", "cost_of_revenue")

    def _prior_gross_profit(self) -> _Component:
        direct = self._direct("prior_gross_profit")
        if direct.value is not None:
            return direct
        return self._derived_difference("prior_gross_profit", "prior_revenue", "prior_cost_of_revenue")

    def _free_cash_flow(self) -> _Component:
        direct = self._direct("free_cash_flow")
        if direct.value is not None:
            return direct
        return self._derived_difference("free_cash_flow", "operating_cash_flow", "capital_expenditures")

    def _net_debt(self) -> _Component:
        direct = self._direct("net_debt")
        if direct.value is not None:
            return direct
        return self._derived_difference("net_debt", "total_debt", "cash_and_equivalents")

    def _enterprise_value(self) -> _Component:
        direct = self._direct("enterprise_value")
        if direct.value is not None:
            return direct
        evaluation = self._derived_metric("enterprise_value")
        return self._from_evaluation("enterprise_value", evaluation)

    def _earnings_per_share(self) -> _Component:
        direct = self._direct("earnings_per_share")
        if direct.value is not None:
            return direct
        return self._derived_ratio_component(
            "earnings_per_share", "net_income", "diluted_shares_outstanding"
        )

    def _prior_earnings_per_share(self) -> _Component:
        direct = self._direct("prior_earnings_per_share")
        if direct.value is not None:
            return direct
        return self._derived_ratio_component(
            "prior_earnings_per_share", "prior_net_income", "prior_diluted_shares_outstanding"
        )

    def _market_cap(self) -> _Component:
        direct = self._direct("market_cap")
        if direct.value is not None:
            return direct
        share_price = self._direct("share_price")
        shares = self._direct("diluted_shares_outstanding")
        if share_price.value is None or shares.value is None:
            return _Component(
                "market_cap",
                None,
                "missing required input(s): market_cap or (share_price and diluted_shares_outstanding)",
            )
        return _Component("market_cap", share_price.value * shares.value)

    def _quick_assets(self) -> _Component:
        direct = self._direct("quick_assets")
        if direct.value is not None:
            return direct

        current_assets = self._direct("current_assets")
        inventory = self._direct("inventory")
        if current_assets.value is not None and inventory.value is not None:
            prepaid = self._optional_zero("prepaid_expenses")
            assert prepaid.value is not None
            return _Component("quick_assets", current_assets.value - inventory.value - prepaid.value)

        cash = self._direct("cash_and_equivalents")
        receivables = self._direct("accounts_receivable")
        if cash.value is not None and receivables.value is not None:
            investments = self._optional_zero("short_term_investments")
            assert investments.value is not None
            return _Component("quick_assets", cash.value + investments.value + receivables.value)

        return _Component(
            "quick_assets",
            None,
            "missing required input(s): quick_assets, "
            "(current_assets and inventory), or (cash_and_equivalents and accounts_receivable)",
        )

    def _ebit(self) -> _Component:
        direct = self._direct("ebit")
        return direct if direct.value is not None else self._renamed(self._direct("operating_income"), "ebit")

    def _ebitda(self) -> _Component:
        direct = self._direct("ebitda")
        if direct.value is not None:
            return direct
        ebit = self._ebit()
        depreciation = self._direct("depreciation_and_amortization")
        if ebit.value is None or depreciation.value is None:
            return _Component(
                "ebitda",
                None,
                "missing required input(s): ebitda or (ebit and depreciation_and_amortization)",
            )
        return _Component("ebitda", ebit.value + depreciation.value)

    def _average_total_assets(self) -> _Component:
        direct = self._direct("average_total_assets")
        return direct if direct.value is not None else self._renamed(self._direct("total_assets"), direct.name)

    def _average_shareholders_equity(self) -> _Component:
        direct = self._direct("average_shareholders_equity")
        return direct if direct.value is not None else self._renamed(self._direct("shareholders_equity"), direct.name)

    def _average_accounts_receivable(self) -> _Component:
        direct = self._direct("average_accounts_receivable")
        return direct if direct.value is not None else self._renamed(self._direct("accounts_receivable"), direct.name)

    def _average_inventory(self) -> _Component:
        direct = self._direct("average_inventory")
        return direct if direct.value is not None else self._renamed(self._direct("inventory"), direct.name)

    def _average_accounts_payable(self) -> _Component:
        direct = self._direct("average_accounts_payable")
        return direct if direct.value is not None else self._renamed(self._direct("accounts_payable"), direct.name)

    def _net_operating_profit_after_tax(self) -> _Component:
        direct = self._direct("net_operating_profit_after_tax")
        if direct.value is not None:
            return direct
        ebit = self._ebit()
        tax_rate = self._effective_tax_rate()
        if ebit.value is None or tax_rate.value is None:
            return _Component(
                "net_operating_profit_after_tax",
                None,
                "missing required input(s): net_operating_profit_after_tax or (ebit and effective_tax_rate)",
            )
        return _Component("net_operating_profit_after_tax", ebit.value * (Decimal(1) - tax_rate.value))

    def _effective_tax_rate(self) -> _Component:
        direct = self._direct("effective_tax_rate")
        if direct.value is not None:
            return direct
        taxes = self._direct("income_tax_expense")
        pretax_income = self._direct("income_before_tax")
        if taxes.value is None or pretax_income.value is None:
            return _Component(
                "effective_tax_rate",
                None,
                "missing required input(s): effective_tax_rate or (income_tax_expense and income_before_tax)",
            )
        if pretax_income.value == 0:
            return _Component("effective_tax_rate", None, "zero denominator: income_before_tax")
        return _Component("effective_tax_rate", taxes.value / pretax_income.value)

    def _average_invested_capital(self) -> _Component:
        direct = self._direct("average_invested_capital")
        if direct.value is not None:
            return direct
        invested = self._direct("invested_capital")
        if invested.value is not None:
            return self._renamed(invested, "average_invested_capital")
        debt = self._direct("total_debt")
        equity = self._direct("shareholders_equity")
        cash = self._direct("cash_and_equivalents")
        if debt.value is None or equity.value is None or cash.value is None:
            return _Component(
                "average_invested_capital",
                None,
                "missing required input(s): average_invested_capital, invested_capital, "
                "or (total_debt, shareholders_equity, and cash_and_equivalents)",
            )
        return _Component("average_invested_capital", debt.value + equity.value - cash.value)

    def _dividends_paid(self) -> _Component:
        direct = self._direct("dividends_paid")
        if direct.value is not None:
            return direct
        per_share = self._direct("dividends_per_share")
        shares = self._direct("diluted_shares_outstanding")
        if per_share.value is None or shares.value is None:
            return _Component(
                "dividends_paid",
                None,
                "missing required input(s): dividends_paid or (dividends_per_share and diluted_shares_outstanding)",
            )
        return _Component("dividends_paid", per_share.value * shares.value)

    def _revenue_growth(self) -> _Component:
        direct = self._direct("revenue_growth")
        if direct.value is not None:
            return direct
        evaluation = self._growth_metric("revenue_growth")
        return self._from_evaluation("revenue_growth", evaluation)

    def _ebitda_margin(self) -> _Component:
        direct = self._direct("ebitda_margin")
        if direct.value is not None:
            return direct
        evaluation = self._ratio("ebitda_margin", self._ebitda(), self._direct("revenue"))
        return self._from_evaluation("ebitda_margin", evaluation)

    def _days_in_period(self) -> _Component:
        direct = self._direct("days_in_period")
        return direct if direct.value is not None else _Component("days_in_period", Decimal(365))

    def _derived_difference(self, name: str, left_name: str, right_name: str) -> _Component:
        left = self._direct(left_name)
        right = self._direct(right_name)
        if left.value is None or right.value is None:
            return _Component(
                name,
                None,
                f"missing required input(s): {name} or ({left_name} and {right_name})",
            )
        return _Component(name, left.value - right.value)

    def _derived_ratio_component(self, name: str, numerator_name: str, denominator_name: str) -> _Component:
        numerator = self._direct(numerator_name)
        denominator = self._direct(denominator_name)
        if numerator.value is None or denominator.value is None:
            return _Component(
                name,
                None,
                f"missing required input(s): {name} or ({numerator_name} and {denominator_name})",
            )
        if denominator.value == 0:
            return _Component(name, None, f"zero denominator: {denominator_name}")
        return _Component(name, numerator.value / denominator.value)

    def _direct(self, name: str) -> _Component:
        for candidate in (name, *_ALIASES.get(name, ())):
            value = self.values.get(candidate)
            if value is not None:
                return _Component(name, value)
        return _Component(name, None, f"missing required input(s): {name}")

    def _optional_zero(self, name: str) -> _Component:
        component = self._direct(name)
        return component if component.value is not None else _Component(name, Decimal(0))

    @staticmethod
    def _renamed(component: _Component, name: str) -> _Component:
        if component.value is None:
            return _Component(name, None, component.reason)
        return _Component(name, component.value)

    @staticmethod
    def _from_evaluation(name: str, evaluation: _Evaluation) -> _Component:
        return _Component(name, evaluation.value, evaluation.reason)

    @staticmethod
    def _inputs(*components: _Component) -> dict[str, Decimal]:
        return {component.name: component.value for component in components if component.value is not None}

    def _unavailable(self, *components: _Component) -> _Evaluation | None:
        failures = [component.reason for component in components if component.value is None]
        if not failures:
            return None
        return _Evaluation(None, self._inputs(*components), "; ".join(reason for reason in failures if reason))


def _requested_metrics(value: JsonValue | None) -> tuple[str, ...]:
    if value is None:
        return SUPPORTED_METRICS
    if not isinstance(value, list) or not value:
        raise ValueError("metrics must be a non-empty array")
    if any(not isinstance(metric, str) for metric in value):
        raise ValueError("metrics must contain only strings")
    requested = tuple(value)
    if len(requested) != len(set(requested)):
        raise ValueError("metrics must not contain duplicates")
    unsupported = [metric for metric in requested if metric not in _DEFINITIONS]
    if unsupported:
        raise ValueError(f"unsupported metrics: {unsupported!r}")
    return requested


def _numeric_values(values: Mapping[Any, Any]) -> dict[str, Decimal]:
    normalized: dict[str, Decimal] = {}
    for name, value in values.items():
        if not isinstance(name, str) or not name:
            raise ValueError("values keys must be non-empty strings")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"values.{name} must be a number")
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError(f"values.{name} must be finite")
        try:
            normalized[name] = Decimal(str(value))
        except InvalidOperation as exc:  # pragma: no cover - guarded above
            raise ValueError(f"values.{name} must be a number") from exc
    return normalized


def _precision(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 12:
        raise ValueError("precision must be an integer between 0 and 12")
    return value


def _rounded(value: Decimal, precision: int) -> float | int:
    quantum = Decimal(1).scaleb(-precision)
    with localcontext() as decimal_context:
        decimal_context.prec = 1_000
        decimal_context.rounding = ROUND_HALF_EVEN
        rounded = value.quantize(quantum)
    if rounded == 0:
        rounded = abs(rounded)
    if precision == 0:
        return int(rounded)
    as_float = float(rounded)
    if not math.isfinite(as_float):
        if rounded == rounded.to_integral_value():
            return int(rounded)
        raise OverflowError("rounded result is outside the finite JSON number range")
    return as_float
