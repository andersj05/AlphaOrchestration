"""Compact agent-facing contracts for the deterministic finance tool suite."""

from __future__ import annotations

from collections.abc import Sequence

from alpha_orchestration.calculations.arithmetic import SUPPORTED_OPERATIONS, calculate_arithmetic
from alpha_orchestration.calculations.forecasts import discounted_cash_flow, forecast_growth
from alpha_orchestration.calculations.market import market_statistics
from alpha_orchestration.calculations.metrics import METRIC_NAMES, calculate_metrics
from alpha_orchestration.calculations.ranking import rank_entities
from alpha_orchestration.domain import JsonValue
from alpha_orchestration.tools.registry import ToolDefinition, ToolRegistry, context_schema, source_ids_schema

NUMBER: dict[str, JsonValue] = {"type": "number"}
PRECISION: dict[str, JsonValue] = {"type": "integer", "minimum": 0, "maximum": 12}
METRIC_VALUE_NAMES: tuple[str, ...] = (
    "revenue",
    "prior_revenue",
    "cost_of_revenue",
    "prior_cost_of_revenue",
    "gross_profit",
    "prior_gross_profit",
    "operating_income",
    "prior_operating_income",
    "net_income",
    "prior_net_income",
    "operating_cash_flow",
    "capital_expenditures",
    "free_cash_flow",
    "current_assets",
    "current_liabilities",
    "cash_and_equivalents",
    "total_debt",
    "net_debt",
    "market_cap",
    "share_price",
    "preferred_stock",
    "minority_interest",
    "enterprise_value",
    "diluted_shares_outstanding",
    "prior_diluted_shares_outstanding",
    "earnings_per_share",
    "prior_earnings_per_share",
    "shareholders_equity",
    "ebit",
    "ebitda",
    "depreciation_and_amortization",
    "quick_assets",
    "inventory",
    "prepaid_expenses",
    "short_term_investments",
    "accounts_receivable",
    "accounts_payable",
    "interest_expense",
    "total_assets",
    "average_total_assets",
    "average_shareholders_equity",
    "net_operating_profit_after_tax",
    "effective_tax_rate",
    "income_tax_expense",
    "income_before_tax",
    "average_invested_capital",
    "invested_capital",
    "average_accounts_receivable",
    "average_inventory",
    "average_accounts_payable",
    "days_in_period",
    "dividends_paid",
    "dividends_per_share",
    "revenue_growth",
    "ebitda_margin",
)


def financial_tool_definitions() -> tuple[ToolDefinition, ...]:
    """Return the explicit internal catalog; no import-time registration occurs."""

    return (
        ToolDefinition(
            name="finance.calculate",
            version="1.0.0",
            description=(
                "Batch explicit arithmetic such as percent change, CAGR, ratios, basis-point changes, and weighted "
                "averages. Use finance.metrics for named statement ratios; never do arithmetic in prose."
            ),
            input_schema=_arithmetic_schema(),
            handler=calculate_arithmetic,
        ),
        ToolDefinition(
            name="finance.metrics",
            version="1.0.0",
            description=(
                "Calculate named profitability, growth, liquidity, leverage, return, cash-cycle, and valuation metrics "
                "from normalized same-unit financial values. Omit metrics to calculate every metric supported by the "
                "available inputs. Capital expenditures are positive cash outlays; rates use decimal form."
            ),
            input_schema=_metrics_schema(),
            handler=calculate_metrics,
        ),
        ToolDefinition(
            name="finance.forecast_growth",
            version="1.0.0",
            description=(
                "Compound a positive base value through explicit bull/base/bear or other growth-rate scenarios. "
                "The model supplies assumptions; the tool performs every period calculation. Rates are decimals."
            ),
            input_schema=_forecast_schema(),
            handler=forecast_growth,
        ),
        ToolDefinition(
            name="finance.discounted_cash_flow",
            version="1.0.0",
            description=(
                "Value an explicit forecast cash-flow series with either perpetual growth or an exit multiple, then "
                "bridge enterprise value to equity and per-share value. Rates are decimals."
            ),
            input_schema=_dcf_schema(),
            handler=discounted_cash_flow,
        ),
        ToolDefinition(
            name="finance.market_statistics",
            version="1.0.0",
            description=(
                "Calculate return, annualized volatility, Sharpe, Sortino, drawdown, and optional benchmark beta, "
                "alpha, and correlation from a bounded price or return series."
            ),
            input_schema=_market_schema(),
            handler=market_statistics,
        ),
        ToolDefinition(
            name="finance.rank",
            version="1.0.0",
            description=(
                "Deterministically rank entities across weighted higher-is-better or lower-is-better criteria using "
                "tie-aware percentile scores and an explicit missing-value policy."
            ),
            input_schema=_ranking_schema(),
            handler=rank_entities,
        ),
    )


def build_financial_tool_registry(*, max_result_bytes: int = 256_000) -> ToolRegistry:
    return ToolRegistry(financial_tool_definitions(), max_result_bytes=max_result_bytes)


# The controller chooses one of these trusted allowlists. The model never supplies it.
AGENT_FINANCIAL_TOOLSETS: dict[str, tuple[str, ...]] = {
    "universe": ("finance.rank",),
    "filings": ("finance.calculate",),
    "market": ("finance.calculate", "finance.market_statistics", "finance.rank"),
    "fundamentals": ("finance.calculate", "finance.metrics", "finance.forecast_growth"),
    "valuation": (
        "finance.calculate",
        "finance.metrics",
        "finance.forecast_growth",
        "finance.discounted_cash_flow",
    ),
    "catalysts": ("finance.calculate", "finance.market_statistics"),
    "risk": (
        "finance.metrics",
        "finance.forecast_growth",
        "finance.discounted_cash_flow",
        "finance.rank",
    ),
    "lead": ("finance.rank",),
}


def financial_tools_for_agent(agent_id: str) -> tuple[str, ...]:
    try:
        return AGENT_FINANCIAL_TOOLSETS[agent_id]
    except KeyError as exc:
        raise ValueError(f"no financial tool allowlist for agent: {agent_id}") from exc


def _base_properties() -> dict[str, JsonValue]:
    return {
        "source_ids": source_ids_schema(),
        "context": context_schema(),
        "precision": PRECISION,
    }


def _arithmetic_schema() -> dict[str, JsonValue]:
    operation_properties: dict[str, JsonValue] = {
        "id": {"type": "string", "minLength": 1, "maxLength": 80},
        "operation": {"type": "string", "enum": list(SUPPORTED_OPERATIONS)},
        "current": NUMBER,
        "prior": NUMBER,
        "numerator": NUMBER,
        "denominator": NUMBER,
        "start": NUMBER,
        "end": NUMBER,
        "periods": {"type": "number", "exclusiveMinimum": 0},
        "values": {"type": "array", "items": NUMBER, "minItems": 1, "maxItems": 1_000},
        "weights": {"type": "array", "items": NUMBER, "minItems": 1, "maxItems": 1_000},
    }
    properties = _base_properties()
    properties["operations"] = {
        "type": "array",
        "minItems": 1,
        "maxItems": 100,
        "items": {
            "type": "object",
            "required": ["id", "operation"],
            "properties": operation_properties,
            "additionalProperties": False,
        },
    }
    return {
        "type": "object",
        "required": ["operations"],
        "properties": properties,
        "additionalProperties": False,
    }


def _metrics_schema() -> dict[str, JsonValue]:
    properties = _base_properties()
    properties.update(
        {
            "values": {
                "type": "object",
                "minProperties": 1,
                "properties": {name: NUMBER for name in METRIC_VALUE_NAMES},
                "additionalProperties": False,
                "description": "Normalized values using one currency/scale and comparable periods.",
            },
            "metrics": {
                "type": "array",
                "items": {"type": "string", "enum": list(METRIC_NAMES)},
                "minItems": 1,
                "maxItems": len(METRIC_NAMES),
                "uniqueItems": True,
            },
        }
    )
    return {
        "type": "object",
        "required": ["values"],
        "properties": properties,
        "additionalProperties": False,
    }


def _forecast_schema() -> dict[str, JsonValue]:
    properties = _base_properties()
    properties.update(
        {
            "base_value": {"type": "number", "exclusiveMinimum": 0},
            "periods": {"type": "integer", "minimum": 1, "maximum": 50},
            "scenarios": {
                "type": "array",
                "minItems": 1,
                "maxItems": 12,
                "items": {
                    "type": "object",
                    "required": ["name", "growth_rates"],
                    "properties": {
                        "name": {"type": "string", "minLength": 1, "maxLength": 50},
                        "growth_rates": {
                            "type": "array",
                            "items": {"type": "number", "exclusiveMinimum": -1},
                            "minItems": 1,
                            "maxItems": 50,
                        },
                    },
                    "additionalProperties": False,
                },
            },
            "period_labels": {
                "type": "array",
                "items": {"type": "string", "minLength": 1, "maxLength": 50},
                "minItems": 1,
                "maxItems": 50,
            },
            "unit": {"type": "string", "minLength": 1, "maxLength": 50},
        }
    )
    return {
        "type": "object",
        "required": ["base_value", "periods", "scenarios"],
        "properties": properties,
        "additionalProperties": False,
    }


def _dcf_schema() -> dict[str, JsonValue]:
    properties = _base_properties()
    properties.update(
        {
            "cash_flows": {"type": "array", "items": NUMBER, "minItems": 1, "maxItems": 50},
            "discount_rate": {"type": "number", "exclusiveMinimum": -1},
            "terminal": {
                "type": "object",
                "required": ["method"],
                "properties": {
                    "method": {"type": "string", "enum": ["perpetuity_growth", "exit_multiple"]},
                    "growth_rate": {"type": "number", "exclusiveMinimum": -1},
                    "multiple": {"type": "number", "exclusiveMinimum": 0},
                    "metric_value": NUMBER,
                },
                "additionalProperties": False,
            },
            "period_labels": {
                "type": "array",
                "items": {"type": "string", "minLength": 1, "maxLength": 50},
                "minItems": 1,
                "maxItems": 50,
            },
            "net_debt": NUMBER,
            "shares_outstanding": {"type": "number", "exclusiveMinimum": 0},
            "mid_year": {"type": "boolean"},
            "unit": {"type": "string", "minLength": 1, "maxLength": 50},
        }
    )
    return {
        "type": "object",
        "required": ["cash_flows", "discount_rate", "terminal"],
        "properties": properties,
        "additionalProperties": False,
    }


def _market_schema() -> dict[str, JsonValue]:
    properties = _base_properties()
    properties.update(
        {
            "prices": {
                "type": "array",
                "items": {"type": "number", "exclusiveMinimum": 0},
                "minItems": 2,
                "maxItems": 10_000,
            },
            "returns": {
                "type": "array",
                "items": {"type": "number", "minimum": -1},
                "minItems": 1,
                "maxItems": 9_999,
            },
            "benchmark_returns": {
                "type": "array",
                "items": {"type": "number", "minimum": -1},
                "minItems": 1,
                "maxItems": 9_999,
            },
            "periods_per_year": {"type": "integer", "minimum": 1, "maximum": 100_000},
            "annual_risk_free_rate": {"type": "number", "exclusiveMinimum": -1},
        }
    )
    return {
        "type": "object",
        "required": ["periods_per_year", "annual_risk_free_rate"],
        "properties": properties,
        "additionalProperties": False,
    }


def _ranking_schema() -> dict[str, JsonValue]:
    metric_map: dict[str, JsonValue] = {"type": "object", "additionalProperties": NUMBER}
    properties = _base_properties()
    properties.update(
        {
            "rows": {
                "type": "array",
                "minItems": 1,
                "maxItems": 5_000,
                "items": {
                    "type": "object",
                    "required": ["id", "metrics"],
                    "properties": {
                        "id": {"type": "string", "minLength": 1, "maxLength": 200},
                        "metrics": metric_map,
                    },
                    "additionalProperties": False,
                },
            },
            "criteria": {
                "type": "array",
                "minItems": 1,
                "maxItems": 50,
                "items": {
                    "type": "object",
                    "required": ["metric", "direction", "weight"],
                    "properties": {
                        "metric": {"type": "string", "minLength": 1, "maxLength": 100},
                        "direction": {"type": "string", "enum": ["higher", "lower"]},
                        "weight": {"type": "number", "minimum": 0},
                    },
                    "additionalProperties": False,
                },
            },
            "missing_policy": {"type": "string", "enum": ["exclude", "worst", "neutral"]},
            "top_n": {"type": "integer", "minimum": 1, "maximum": 5_000},
        }
    )
    return {
        "type": "object",
        "required": ["rows", "criteria", "missing_policy"],
        "properties": properties,
        "additionalProperties": False,
    }


def contracts_for_tools(names: Sequence[str]) -> tuple[dict[str, JsonValue], ...]:
    """Convenience helper used when rendering a model turn."""

    return build_financial_tool_registry().contracts(names)
