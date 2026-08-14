from __future__ import annotations

import asyncio

import pytest

from alpha_orchestration.data import yfinance as yfinance_adapter
from alpha_orchestration.data.universe import EquityScreenRequest
from alpha_orchestration.data.yfinance import YFinanceClient


class FakeQuery:
    def __init__(self, operator: str, operand: list) -> None:
        self.operator = operator
        self.operand = operand


def _request() -> EquityScreenRequest:
    return EquityScreenRequest(
        offset=250,
        size=200,
        exchange_codes=("nms", "NMS", "nyq"),
        minimum_market_cap=300_000_000,
        minimum_share_price=1,
        minimum_average_daily_volume_3m=200_000,
    )


def test_equity_screen_request_normalizes_identity_and_serializes_query() -> None:
    request = _request()

    assert request.exchange_codes == ("NMS", "NYQ")
    assert request.to_parameters() == {
        "offset": 250,
        "size": 200,
        "exchange_codes": ["NMS", "NYQ"],
        "minimum_market_cap": 300_000_000.0,
        "minimum_share_price": 1.0,
        "minimum_average_daily_volume_3m": 200_000.0,
        "region": "us",
        "quote_type": "EQUITY",
        "sort_field": "intradaymarketcap",
        "sort_ascending": False,
    }


def test_yfinance_equity_screen_builds_the_bounded_ranked_query(monkeypatch) -> None:
    calls: list[tuple[FakeQuery, dict]] = []
    response = {"start": 250, "count": 1, "total": 301, "quotes": [{"symbol": "AAPL"}]}

    class FakeYFinance:
        EquityQuery = FakeQuery

        @staticmethod
        def screen(query, **kwargs):
            calls.append((query, kwargs))
            return response

    monkeypatch.setattr(yfinance_adapter, "_load_yfinance", lambda: FakeYFinance)

    payload = asyncio.run(YFinanceClient().screen_equities(_request()))

    assert payload == response
    query, kwargs = calls[0]
    assert kwargs == {
        "offset": 250,
        "size": 200,
        "sortField": "intradaymarketcap",
        "sortAsc": False,
    }
    assert query.operator == "and"
    assert [(item.operator, item.operand) for item in query.operand] == [
        ("eq", ["region", "us"]),
        ("is-in", ["exchange", "NMS", "NYQ"]),
        ("gte", ["intradaymarketcap", 300_000_000.0]),
        ("gte", ["intradayprice", 1.0]),
        ("gte", ["avgdailyvol3m", 200_000.0]),
    ]


def test_yfinance_equity_screen_rejects_non_object_payload(monkeypatch) -> None:
    class FakeYFinance:
        EquityQuery = FakeQuery

        @staticmethod
        def screen(query, **kwargs):
            del query, kwargs
            return []

    monkeypatch.setattr(yfinance_adapter, "_load_yfinance", lambda: FakeYFinance)

    with pytest.raises(ValueError, match="non-object"):
        asyncio.run(YFinanceClient().screen_equities(_request()))


@pytest.mark.parametrize(
    "changes, message",
    [
        ({"offset": -1}, "offset"),
        ({"size": 251}, "size"),
        ({"exchange_codes": ()}, "exchange_codes"),
        ({"minimum_market_cap": float("nan")}, "minimum_market_cap"),
    ],
)
def test_equity_screen_request_rejects_invalid_bounds(changes, message) -> None:
    values = {
        "offset": 0,
        "size": 250,
        "exchange_codes": ("NMS", "NYQ"),
        "minimum_market_cap": 300_000_000,
        "minimum_share_price": 1,
        "minimum_average_daily_volume_3m": 200_000,
    }
    values.update(changes)

    with pytest.raises(ValueError, match=message):
        EquityScreenRequest(**values)
