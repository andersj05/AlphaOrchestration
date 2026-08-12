import json
from datetime import UTC, datetime

import pytest

from alpha_orchestration.data.observations import UnitKind
from alpha_orchestration.data.yfinance import MarketSnapshot
from alpha_orchestration.data.yfinance_mapping import map_yfinance_history, map_yfinance_snapshot

RETRIEVED_AT = datetime(2025, 1, 3, 20, tzinfo=UTC)


def test_snapshot_maps_price_and_market_cap_with_exact_evidence() -> None:
    snapshot = MarketSnapshot(
        ticker="abc",
        currency="usd",
        last_price=12.5,
        market_cap=1_250_000,
        exchange="NMS",
    )

    batch = map_yfinance_snapshot(
        snapshot,
        observed_at=datetime(2025, 1, 3, 16, tzinfo=UTC),
        retrieved_at=RETRIEVED_AT,
    )

    assert [(item.name, item.value) for item in batch.observations] == [
        ("market_cap", 1_250_000),
        ("share_price", 12.5),
    ]
    price = next(item for item in batch.observations if item.name == "share_price")
    assert price.entity_id == "ticker:ABC"
    assert price.unit.kind is UnitKind.CURRENCY_PER_SHARE
    assert price.unit.symbol == "USD"
    assert price.metadata["exchange"] == "NMS"
    evidence = batch.evidence[0]
    assert evidence.provider.value == "yfinance"
    assert evidence.source_locator["reported_value"] in {12.5, 1_250_000}
    assert not batch.issues


def test_snapshot_without_currency_fails_closed_instead_of_guessing() -> None:
    snapshot = MarketSnapshot("ABC", None, 10.0, 1000.0, "NMS")

    batch = map_yfinance_snapshot(
        snapshot,
        observed_at=datetime(2025, 1, 3, tzinfo=UTC),
        retrieved_at=RETRIEVED_AT,
    )

    assert not batch.observations
    assert [issue.code for issue in batch.issues] == ["missing_currency"]


def test_history_maps_ohlcv_and_only_nonzero_corporate_actions() -> None:
    rows = [
        {
            "Date": "2025-01-02",
            "Open": 10.0,
            "High": 11.0,
            "Low": 9.5,
            "Close": 10.5,
            "Adj Close": 10.25,
            "Volume": 100,
            "Dividends": 0.0,
            "Stock Splits": 0.0,
        },
        {
            "Datetime": "2025-01-03T16:00:00-05:00",
            "Close": 11.0,
            "Volume": 0,
            "Dividends": 0.25,
            "Stock Splits": 2.0,
            "Capital Gains": 0.1,
        },
    ]

    batch = map_yfinance_history(
        "abc",
        rows,
        currency="usd",
        auto_adjust=False,
        retrieved_at=RETRIEVED_AT,
    )

    first_day = [item.name for item in batch.observations if item.period.end.isoformat() == "2025-01-02"]
    assert first_day == [
        "adjusted_close_price",
        "close_price",
        "high_price",
        "low_price",
        "open_price",
        "volume",
    ]
    assert {item.name for item in batch.observations if item.period.end.isoformat() == "2025-01-03"} == {
        "capital_gains_per_share",
        "close_price",
        "dividends_per_share",
        "stock_split_ratio",
        "volume",
    }
    assert all(item.metadata["auto_adjust"] is False for item in batch.observations)
    assert all(item.evidence_ids[0].startswith("yfinance:price_history:") for item in batch.observations)
    assert not batch.issues


def test_adjusted_history_metadata_and_naive_dates_are_explicit() -> None:
    batch = map_yfinance_history(
        "ABC",
        [{"Date": datetime(2025, 1, 2), "Close": 10.0}],
        currency="USD",
        auto_adjust=True,
        retrieved_at=RETRIEVED_AT,
        interval="1d",
    )

    observation = batch.observations[0]
    assert observation.metadata["adjusted"] is True
    assert observation.metadata["interval"] == "1d"
    assert batch.evidence[0].observed_at.isoformat() == "2025-01-02T00:00:00+00:00"


def test_history_rejects_bad_cells_and_deduplicates_same_timestamp() -> None:
    rows = [
        {"Date": "bad-date", "Close": 10.0},
        {"Date": "2025-01-02", "Close": float("nan")},
        {"Date": "2025-01-03", "Close": 10.0},
        {"Date": "2025-01-03", "Close": 11.0},
    ]

    batch = map_yfinance_history(
        "ABC",
        rows,
        currency="USD",
        auto_adjust=False,
        retrieved_at=RETRIEVED_AT,
    )

    assert [(item.name, item.value) for item in batch.observations] == [("close_price", 11.0)]
    assert [issue.code for issue in batch.issues] == [
        "invalid_timestamp",
        "invalid_value",
        "duplicate_history_value",
    ]


def test_yfinance_mapping_is_deterministic_and_strict_json() -> None:
    kwargs = {
        "ticker": "ABC",
        "rows": [{"Date": "2025-01-02", "Close": 10.0, "Volume": 5}],
        "currency": "USD",
        "auto_adjust": False,
        "retrieved_at": RETRIEVED_AT,
    }

    first = map_yfinance_history(**kwargs)
    second = map_yfinance_history(**kwargs)

    assert first == second
    json.dumps(first.to_dict(), allow_nan=False)


def test_history_requires_currency_and_boolean_adjustment_policy() -> None:
    with pytest.raises(ValueError, match="currency"):
        map_yfinance_history(
            "ABC",
            [],
            currency="unknown currency",
            auto_adjust=False,
            retrieved_at=RETRIEVED_AT,
        )
    with pytest.raises(ValueError, match="auto_adjust"):
        map_yfinance_history(
            "ABC",
            [],
            currency="USD",
            auto_adjust=1,
            retrieved_at=RETRIEVED_AT,
        )
