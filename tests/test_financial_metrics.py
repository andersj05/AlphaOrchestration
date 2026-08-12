from __future__ import annotations

import math

import pytest

from alpha_orchestration.calculations.metrics import SUPPORTED_METRICS, calculate_metrics


def test_revenue_percent_change_and_context_are_explicit() -> None:
    result = calculate_metrics(
        {
            "values": {"revenue": 125, "prior_revenue": 100},
            "metrics": ["revenue_growth"],
            "precision": 4,
            "context": {"currency": "USD", "current_period": "FY2026", "prior_period": "FY2025"},
        }
    )

    assert result["values"] == {"revenue_growth": 0.25}
    assert result["unavailable"] == {}
    assert result["details"] == {
        "revenue_growth": {
            "category": "growth",
            "unit": "ratio",
            "formula": "(revenue - prior_revenue) / prior_revenue",
            "inputs": {"revenue": 125.0, "prior_revenue": 100.0},
        }
    }
    assert result["context"] == {
        "currency": "USD",
        "current_period": "FY2026",
        "prior_period": "FY2025",
    }
    assert result["rounding"] == {"mode": "half_even", "decimal_places": 4}
    assert result["formula_version"] == "finance-metrics-v1"


def test_operating_margin_uses_decimal_ratio_not_percent_points() -> None:
    result = calculate_metrics(
        {
            "values": {"operating_income": 18, "revenue": 120},
            "metrics": ["operating_margin"],
        }
    )

    assert result["values"]["operating_margin"] == 0.15
    assert result["details"]["operating_margin"] == {
        "category": "margin",
        "unit": "ratio",
        "formula": "operating_income / revenue",
        "inputs": {"operating_income": 18.0, "revenue": 120.0},
    }


def test_zero_denominator_marks_only_that_metric_unavailable() -> None:
    result = calculate_metrics(
        {
            "values": {"operating_income": 10, "revenue": 0, "current_assets": 60, "current_liabilities": 30},
            "metrics": ["operating_margin", "current_ratio"],
        }
    )

    assert result["values"] == {"current_ratio": 2.0}
    assert result["unavailable"] == {"operating_margin": "zero denominator: revenue"}
    assert result["details"]["operating_margin"]["inputs"] == {
        "operating_income": 10.0,
        "revenue": 0.0,
    }


def test_enterprise_value_derives_optional_adjustments_as_zero() -> None:
    result = calculate_metrics(
        {
            "values": {"market_cap": 1_000, "total_debt": 300, "cash_and_equivalents": 125},
            "metrics": ["enterprise_value"],
            "precision": 0,
        }
    )

    assert result["values"] == {"enterprise_value": 1_175}
    assert result["details"]["enterprise_value"] == {
        "category": "derived_amount",
        "unit": "currency",
        "formula": "market_cap + total_debt + preferred_stock + minority_interest - cash_and_equivalents",
        "inputs": {
            "market_cap": 1_000,
            "total_debt": 300,
            "preferred_stock": 0,
            "minority_interest": 0,
            "cash_and_equivalents": 125,
        },
    }


def test_missing_input_has_stable_reason_and_formula_metadata() -> None:
    result = calculate_metrics({"values": {"revenue": 100}, "metrics": ["gross_profit"]})

    assert result["values"] == {}
    assert result["unavailable"] == {"gross_profit": "missing required input(s): cost_of_revenue"}
    assert result["details"]["gross_profit"]["formula"] == "revenue - cost_of_revenue"
    assert result["details"]["gross_profit"]["inputs"] == {"revenue": 100.0}


def test_derived_components_feed_margin_leverage_and_valuation() -> None:
    result = calculate_metrics(
        {
            "values": {
                "revenue": 500,
                "cost_of_revenue": 300,
                "operating_cash_flow": 90,
                "capital_expenditures": 30,
                "market_cap": 1_200,
                "total_debt": 240,
                "cash_and_equivalents": 40,
                "ebitda": 120,
            },
            "metrics": [
                "gross_margin",
                "free_cash_flow_margin",
                "net_debt_to_ebitda",
                "enterprise_value_to_ebitda",
                "free_cash_flow_yield",
            ],
            "precision": 4,
        }
    )

    assert result["values"] == {
        "gross_margin": 0.4,
        "free_cash_flow_margin": 0.12,
        "net_debt_to_ebitda": 1.6667,
        "enterprise_value_to_ebitda": 11.6667,
        "free_cash_flow_yield": 0.05,
    }
    assert result["unavailable"] == {}


def test_cash_conversion_cycle_defaults_to_365_days() -> None:
    result = calculate_metrics(
        {
            "values": {
                "average_accounts_receivable": 100,
                "average_inventory": 80,
                "average_accounts_payable": 60,
                "revenue": 1_000,
                "cost_of_revenue": 400,
            },
            "metrics": [
                "days_sales_outstanding",
                "days_inventory_outstanding",
                "days_payables_outstanding",
                "cash_conversion_cycle",
            ],
            "precision": 2,
        }
    )

    assert result["values"] == {
        "days_sales_outstanding": 36.5,
        "days_inventory_outstanding": 73.0,
        "days_payables_outstanding": 54.75,
        "cash_conversion_cycle": 54.75,
    }
    assert result["details"]["days_sales_outstanding"]["inputs"]["days_in_period"] == 365.0


def test_return_on_invested_capital_can_derive_nopat() -> None:
    result = calculate_metrics(
        {
            "values": {"operating_income": 100, "effective_tax_rate": 0.25, "average_invested_capital": 500},
            "metrics": ["return_on_invested_capital"],
        }
    )

    assert result["values"] == {"return_on_invested_capital": 0.15}
    assert result["details"]["return_on_invested_capital"]["inputs"] == {
        "net_operating_profit_after_tax": 75.0,
        "average_invested_capital": 500.0,
    }


def test_rule_of_40_adds_growth_and_ebitda_margin_as_ratios() -> None:
    result = calculate_metrics(
        {
            "values": {"revenue": 120, "prior_revenue": 100, "ebitda": 24},
            "metrics": ["rule_of_40"],
        }
    )

    assert result["values"] == {"rule_of_40": 0.4}
    assert result["details"]["rule_of_40"]["inputs"] == {
        "revenue_growth": 0.2,
        "ebitda_margin": 0.2,
    }


def test_rounding_is_decimal_half_even() -> None:
    result = calculate_metrics(
        {
            "values": {"operating_income": 2.345, "revenue": 1},
            "metrics": ["operating_margin"],
            "precision": 2,
        }
    )

    assert result["values"] == {"operating_margin": 2.34}


@pytest.mark.parametrize("bad_value", [True, math.nan, math.inf, -math.inf, "100", None])
def test_values_must_be_finite_json_numbers(bad_value: object) -> None:
    with pytest.raises(ValueError, match=r"values\.revenue must (?:be a number|be finite)"):
        calculate_metrics({"values": {"revenue": bad_value}, "metrics": ["operating_margin"]})  # type: ignore[dict-item]


def test_omitted_metric_list_returns_metadata_for_entire_catalog() -> None:
    result = calculate_metrics({"values": {}})

    assert tuple(result["details"]) == SUPPORTED_METRICS
    assert set(result["unavailable"]) == set(SUPPORTED_METRICS)
    assert result["values"] == {}


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        ({"values": {}, "metrics": []}, "metrics must be a non-empty array"),
        ({"values": {}, "metrics": ["not_a_metric"]}, "unsupported metrics"),
        ({"values": {}, "metrics": ["gross_margin", "gross_margin"]}, "metrics must not contain duplicates"),
        ({"values": {}, "precision": 13}, "precision must be an integer between 0 and 12"),
    ],
)
def test_structurally_invalid_requests_raise(arguments: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        calculate_metrics(arguments)  # type: ignore[arg-type]
