import math

import pytest

from alpha_orchestration.calculations.market import market_statistics
from alpha_orchestration.calculations.ranking import rank_entities


def test_market_statistics_return_golden_case() -> None:
    result = market_statistics(
        {
            "returns": [0.1, -0.05, 0.2],
            "periods_per_year": 3,
            "annual_risk_free_rate": 0.0,
            "precision": 6,
            "context": {"ticker": "EXAMPLE"},
        }
    )

    metrics = result["metrics"]
    assert metrics["total_return"]["value"] == 0.254
    assert metrics["annualized_return"]["value"] == 0.254
    assert metrics["annualized_volatility"]["value"] == 0.217945
    assert metrics["sharpe_ratio"]["value"] == 1.147079
    assert metrics["sortino_ratio"]["value"] == 5.0
    assert metrics["max_drawdown"]["value"] == -0.05
    assert metrics["best_period_return"]["value"] == 0.2
    assert metrics["worst_period_return"]["value"] == -0.05
    assert metrics["positive_period_ratio"]["value"] == 0.666667
    assert metrics["beta"]["value"] is None
    assert metrics["beta"]["unavailable_reason"] == "benchmark_returns was not provided"
    assert result["context"] == {"ticker": "EXAMPLE"}


def test_market_statistics_prices_and_aligned_benchmark() -> None:
    result = market_statistics(
        {
            "prices": [100.0, 110.0, 104.5, 125.4],
            "benchmark_returns": [0.05, -0.02, 0.1],
            "periods_per_year": 3,
            "annual_risk_free_rate": 0.0,
        }
    )

    metrics = result["metrics"]
    assert metrics["total_return"]["value"] == 0.254
    assert metrics["beta"]["value"] == pytest.approx(2.087156, abs=1e-6)
    assert metrics["alpha"]["value"] == pytest.approx(-0.02133, abs=1e-6)
    assert metrics["correlation"]["value"] == pytest.approx(0.999819, abs=1e-6)
    assert result["input_kind"] == "prices"
    assert result["return_observations"] == 3


@pytest.mark.parametrize(
    "arguments, message",
    [
        ({"periods_per_year": 12, "annual_risk_free_rate": 0.0}, "exactly one"),
        (
            {
                "prices": [1, 2],
                "returns": [1],
                "periods_per_year": 12,
                "annual_risk_free_rate": 0.0,
            },
            "exactly one",
        ),
        (
            {"returns": [math.nan], "periods_per_year": 12, "annual_risk_free_rate": 0.0},
            "finite",
        ),
        (
            {
                "returns": [0.1, 0.2],
                "benchmark_returns": [0.1],
                "periods_per_year": 12,
                "annual_risk_free_rate": 0.0,
            },
            "same number",
        ),
    ],
)
def test_market_statistics_rejects_invalid_inputs(arguments: dict, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        market_statistics(arguments)


def test_market_statistics_reports_mathematically_unavailable_metrics() -> None:
    result = market_statistics(
        {
            "returns": [0.01, 0.01],
            "benchmark_returns": [0.02, 0.02],
            "periods_per_year": 12,
            "annual_risk_free_rate": 0.0,
        }
    )

    metrics = result["metrics"]
    assert metrics["sharpe_ratio"]["value"] is None
    assert metrics["sharpe_ratio"]["unavailable_reason"] == "sample volatility is zero"
    assert metrics["sortino_ratio"]["value"] is None
    assert "no returns below" in metrics["sortino_ratio"]["unavailable_reason"]
    assert metrics["beta"]["value"] is None
    assert metrics["alpha"]["value"] is None
    assert metrics["correlation"]["value"] is None
    assert "variance is zero" in metrics["beta"]["unavailable_reason"]


def test_ranking_uses_average_ranks_for_ties_and_stable_id_sort() -> None:
    result = rank_entities(
        {
            "rows": [
                {"id": "B", "metrics": {"growth": 10}},
                {"id": "A", "metrics": {"growth": 10}},
                {"id": "C", "metrics": {"growth": 5}},
            ],
            "criteria": [{"metric": "growth", "direction": "higher", "weight": 2}],
            "missing_policy": "exclude",
            "context": {"ticker": "EXAMPLE", "as_of": "2026-08-11"},
        }
    )

    assert [row["id"] for row in result["ranked"]] == ["A", "B", "C"]
    assert [row["metric_ranks"]["growth"] for row in result["ranked"]] == [1.5, 1.5, 3.0]
    assert [row["metric_scores"]["growth"] for row in result["ranked"]] == [75.0, 75.0, 0.0]
    assert result["normalized_weights"] == {"growth": 1.0}
    assert result["context"] == {"ticker": "EXAMPLE", "as_of": "2026-08-11"}


def test_ranking_respects_higher_and_lower_directions() -> None:
    result = rank_entities(
        {
            "rows": [
                {"id": "A", "metrics": {"growth": 5, "pe": 10}},
                {"id": "B", "metrics": {"growth": 10, "pe": 20}},
            ],
            "criteria": [
                {"metric": "growth", "direction": "higher", "weight": 1},
                {"metric": "pe", "direction": "lower", "weight": 1},
            ],
            "missing_policy": "exclude",
        }
    )

    assert [row["id"] for row in result["ranked"]] == ["A", "B"]
    assert [row["composite_score"] for row in result["ranked"]] == [50.0, 50.0]
    assert result["normalized_weights"] == {"growth": 0.5, "pe": 0.5}


def test_ranking_missing_policies_are_explicit() -> None:
    base = {
        "rows": [
            {"id": "A", "metrics": {"growth": 10, "quality": 5}},
            {"id": "B", "metrics": {"growth": 20}},
        ],
        "criteria": [
            {"metric": "growth", "direction": "higher", "weight": 1},
            {"metric": "quality", "direction": "higher", "weight": 1},
        ],
    }

    excluded = rank_entities({**base, "missing_policy": "exclude"})
    assert [row["id"] for row in excluded["ranked"]] == ["A"]
    assert excluded["excluded"] == [
        {
            "id": "B",
            "reason": "missing one or more ranking metrics",
            "missing_metrics": ["quality"],
        }
    ]

    worst = rank_entities({**base, "missing_policy": "worst"})
    neutral = rank_entities({**base, "missing_policy": "neutral", "top_n": 1})
    worst_b = next(row for row in worst["ranked"] if row["id"] == "B")
    assert worst_b["metric_scores"]["quality"] == 0.0
    assert worst_b["metric_ranks"]["quality"] is None
    assert neutral["ranked"][0]["id"] == "B"
    assert neutral["ranked"][0]["metric_scores"]["quality"] == 50.0
    assert neutral["returned_count"] == 1


def test_ranking_rejects_non_finite_metrics_and_zero_weights() -> None:
    with pytest.raises(ValueError, match="finite"):
        rank_entities(
            {
                "rows": [{"id": "A", "metrics": {"growth": math.inf}}],
                "criteria": [{"metric": "growth", "direction": "higher", "weight": 1}],
                "missing_policy": "worst",
            }
        )
    with pytest.raises(ValueError, match="sum to more than zero"):
        rank_entities(
            {
                "rows": [{"id": "A", "metrics": {"growth": 1}}],
                "criteria": [{"metric": "growth", "direction": "higher", "weight": 0}],
                "missing_policy": "worst",
            }
        )
    with pytest.raises(ValueError, match="context must be an object"):
        rank_entities(
            {
                "rows": [{"id": "A", "metrics": {"growth": 1}}],
                "criteria": [{"metric": "growth", "direction": "higher", "weight": 1}],
                "missing_policy": "worst",
                "context": [],
            }
        )
