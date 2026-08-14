"""Lazy yfinance adapter returning plain Python records.

yfinance is optional because its data terms and dependency footprint are
different from the SEC's public JSON APIs.  Install the ``data`` extra to use
this module.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from alpha_orchestration.data.observations import ObservationBatch


class YFinanceUnavailable(RuntimeError):
    pass


def _load_yfinance():
    try:
        import yfinance as yf
    except ImportError as exc:
        raise YFinanceUnavailable('yfinance is optional; install with: pip install -e ".[data]"') from exc
    return yf


@dataclass(frozen=True, slots=True)
class MarketSnapshot:
    ticker: str
    currency: str | None
    last_price: float | None
    market_cap: float | None
    exchange: str | None


class YFinanceClient:
    @staticmethod
    def _symbol(ticker: str) -> str:
        symbol = ticker.strip().upper()
        if not symbol or len(symbol) > 20:
            raise ValueError(f"invalid ticker: {ticker!r}")
        return symbol

    async def snapshot(self, ticker: str) -> MarketSnapshot:
        symbol = self._symbol(ticker)
        yf = _load_yfinance()

        def fetch() -> MarketSnapshot:
            info = dict(yf.Ticker(symbol).get_fast_info())
            return MarketSnapshot(
                ticker=symbol,
                currency=_optional_text(info.get("currency")),
                last_price=_optional_float(_first_present(info, "lastPrice", "last_price")),
                market_cap=_optional_float(_first_present(info, "marketCap", "market_cap")),
                exchange=_optional_text(info.get("exchange")),
            )

        return await asyncio.to_thread(fetch)

    async def history(
        self,
        ticker: str,
        *,
        start: date,
        end: date,
        auto_adjust: bool = True,
    ) -> tuple[dict[str, Any], ...]:
        symbol = self._symbol(ticker)
        yf = _load_yfinance()

        def fetch() -> tuple[dict[str, Any], ...]:
            frame = yf.download(
                symbol,
                start=start.isoformat(),
                end=end.isoformat(),
                auto_adjust=auto_adjust,
                actions=True,
                progress=False,
                threads=False,
                timeout=15,
                multi_level_index=False,
            )
            frame = frame.reset_index()
            rows: list[dict[str, Any]] = []
            for record in frame.to_dict(orient="records"):
                rows.append({key: _plain_value(value) for key, value in record.items()})
            return tuple(rows)

        return await asyncio.to_thread(fetch)


def _first_present(values: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in values:
            return values[key]
    return None


def _optional_text(value: Any) -> str | None:
    return None if value is None else str(value)


def _optional_float(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def _plain_value(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    return value


def map_yfinance_snapshot(
    snapshot: MarketSnapshot,
    *,
    observed_at: datetime,
    retrieved_at: datetime,
) -> ObservationBatch:
    """Normalize a fast-info market snapshot through the provider mapper."""

    from alpha_orchestration.data.yfinance_mapping import map_yfinance_snapshot as mapper

    return mapper(snapshot, observed_at=observed_at, retrieved_at=retrieved_at)


def map_yfinance_history(
    ticker: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    currency: str,
    auto_adjust: bool,
    retrieved_at: datetime,
    interval: str = "1d",
) -> ObservationBatch:
    """Normalize plain yfinance history rows through the provider mapper."""

    from alpha_orchestration.data.yfinance_mapping import map_yfinance_history as mapper

    return mapper(
        ticker,
        rows,
        currency=currency,
        auto_adjust=auto_adjust,
        retrieved_at=retrieved_at,
        interval=interval,
    )
