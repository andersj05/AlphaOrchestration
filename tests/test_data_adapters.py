import asyncio

import pytest

from alpha_orchestration.data import yfinance as yfinance_adapter
from alpha_orchestration.data.sec import SecDataClient, normalize_cik
from alpha_orchestration.data.yfinance import YFinanceClient


def test_cik_normalization() -> None:
    assert normalize_cik(320193) == "0000320193"
    assert normalize_cik("CIK0000320193") == "0000320193"


def test_recent_filings_projects_columnar_sec_payload() -> None:
    submissions = {
        "filings": {
            "recent": {
                "form": ["10-K", "4", "8-K"],
                "accessionNumber": ["a", "b", "c"],
                "filingDate": ["2025-01-01", "2025-01-02", "2025-01-03"],
            }
        }
    }

    rows = SecDataClient.recent_filings(submissions)

    assert [row["form"] for row in rows] == ["10-K", "8-K"]
    assert rows[1]["accessionNumber"] == "c"


@pytest.mark.parametrize(
    "payload",
    [
        {"lastPrice": 226.40, "marketCap": 3_400_000_000_000, "currency": "USD", "exchange": "NMS"},
        {"last_price": 226.40, "market_cap": 3_400_000_000_000, "currency": "USD", "exchange": "NMS"},
    ],
    ids=("yfinance-1.6-camel-case", "legacy-snake-case"),
)
def test_yfinance_snapshot_accepts_fast_info_key_variants(monkeypatch, payload) -> None:
    seen: list[str] = []

    class FakeTicker:
        def __init__(self, symbol: str) -> None:
            seen.append(symbol)

        def get_fast_info(self):
            return payload

    class FakeYFinance:
        Ticker = FakeTicker

    monkeypatch.setattr(yfinance_adapter, "_load_yfinance", lambda: FakeYFinance)

    snapshot = asyncio.run(YFinanceClient().snapshot(" aapl "))

    assert seen == ["AAPL"]
    assert snapshot.ticker == "AAPL"
    assert snapshot.last_price == 226.40
    assert snapshot.market_cap == 3_400_000_000_000
    assert snapshot.currency == "USD"
    assert snapshot.exchange == "NMS"
