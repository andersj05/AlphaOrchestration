import math

import pytest

from alpha_orchestration.calculations.forecasts import discounted_cash_flow, forecast_growth


def test_growth_forecast_golden_scenarios_and_metadata() -> None:
    result = forecast_growth(
        {
            "base_value": 100,
            "periods": 3,
            "labels": ["FY26", "FY27", "FY28"],
            "scenarios": [
                {"name": "Base", "growth_rates": [0.10]},
                {"name": "Uneven", "growth_rates": [0.0, -0.10, 0.20]},
            ],
            "unit": "USD millions",
            "context": {"currency": "USD", "scale": "millions"},
            "precision": 6,
        }
    )

    base, uneven = result["scenarios"]
    assert [row["value"] for row in base["values"]] == [110.0, 121.0, 133.1]
    assert base["growth_rates"] == [0.1, 0.1, 0.1]
    assert base["rate_mode"] == "constant"
    assert base["total_growth"] == 0.331
    assert base["cagr"] == 0.1
    assert [row["value"] for row in uneven["values"]] == [100.0, 90.0, 108.0]
    assert uneven["total_growth"] == 0.08
    assert uneven["cagr"] == pytest.approx(0.025986)
    assert result["period_labels"] == ["FY26", "FY27", "FY28"]
    assert result["unit"] == "USD millions"
    assert result["context"] == {"currency": "USD", "scale": "millions"}
    assert result["formula_version"] == "finance-growth-forecast-v1"
    assert result["rounding"]["calculations_use_unrounded_values"] is True


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (
            {
                "base_value": 100,
                "periods": 1,
                "scenarios": [
                    {"name": "same", "growth_rates": [0.1]},
                    {"name": "same", "growth_rates": [0.2]},
                ],
            },
            "duplicate scenario name",
        ),
        (
            {
                "base_value": 100,
                "periods": 2,
                "scenarios": [{"name": "bad rate", "growth_rates": [-1]}],
            },
            "greater than -1",
        ),
        (
            {
                "base_value": 100,
                "periods": 3,
                "scenarios": [{"name": "wrong length", "growth_rates": [0.1, 0.2]}],
            },
            "one rate or exactly 3 rates",
        ),
        (
            {
                "base_value": 100,
                "periods": 2,
                "labels": ["only one"],
                "scenarios": [{"name": "base", "growth_rates": [0.1]}],
            },
            "exactly 2 forecast-period labels",
        ),
        (
            {
                "base_value": math.inf,
                "periods": 1,
                "scenarios": [{"name": "base", "growth_rates": [0.1]}],
            },
            "finite",
        ),
    ],
)
def test_growth_forecast_rejects_invalid_inputs(arguments: dict, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        forecast_growth(arguments)


def test_dcf_perpetuity_growth_golden_case() -> None:
    result = discounted_cash_flow(
        {
            "cash_flows": [100, 100],
            "discount_rate": 0.10,
            "terminal": {"method": "perpetuity_growth", "growth_rate": 0.0},
            "net_debt": 100,
            "shares_outstanding": 90,
            "labels": ["FY26", "FY27"],
            "unit": "USD millions",
            "precision": 6,
        }
    )

    assert [row["present_value"] for row in result["pv_schedule"]] == [90.909091, 82.644628]
    assert result["pv_forecast_cash_flows"] == 173.553719
    assert result["terminal"]["terminal_cash_flow"] == 100.0
    assert result["terminal_value"] == 1000.0
    assert result["pv_terminal_value"] == 826.446281
    assert result["enterprise_value"] == 1000.0
    assert result["equity_value"] == 900.0
    assert result["per_share_value"] == 10.0
    assert result["per_share_unit"] == "USD millions_per_share"
    assert result["formula_version"] == "finance-dcf-v1"


def test_dcf_mid_year_exit_multiple_timing() -> None:
    result = discounted_cash_flow(
        {
            "cash_flows": [100],
            "discount_rate": 0.21,
            "terminal": {"method": "exit_multiple", "multiple": 10},
            "mid_year": True,
            "precision": 6,
        }
    )

    assert result["pv_schedule"][0]["discount_period"] == 0.5
    assert result["pv_schedule"][0]["discount_factor"] == pytest.approx(0.909091)
    assert result["terminal"]["metric_source"] == "last forecast cash flow"
    assert result["terminal"]["discount_period"] == 0.5
    assert result["terminal_value"] == 1000.0
    assert result["enterprise_value"] == 1000.0
    assert result["per_share_value"] is None


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (
            {
                "cash_flows": [100],
                "discount_rate": -1,
                "terminal": {"method": "exit_multiple", "multiple": 5},
            },
            "discount_rate must be greater than -1",
        ),
        (
            {
                "cash_flows": [100],
                "discount_rate": 0.08,
                "terminal": {"method": "perpetuity_growth", "growth_rate": 0.08},
            },
            "must be less than discount_rate",
        ),
        (
            {
                "cash_flows": [100],
                "discount_rate": 0.08,
                "terminal": {"method": "exit_multiple", "multiple": 0},
            },
            "multiple must be greater than zero",
        ),
        (
            {
                "cash_flows": [100],
                "discount_rate": 0.08,
                "terminal": {"method": "exit_multiple", "multiple": 5},
                "shares_outstanding": 0,
            },
            "shares_outstanding must be greater than zero",
        ),
        (
            {
                "cash_flows": [math.nan],
                "discount_rate": 0.08,
                "terminal": {"method": "exit_multiple", "multiple": 5},
            },
            r"cash_flows\[0\] must be finite",
        ),
    ],
)
def test_dcf_rejects_invalid_inputs(arguments: dict, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        discounted_cash_flow(arguments)
