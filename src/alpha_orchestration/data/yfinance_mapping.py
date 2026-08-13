"""Map yfinance projections into canonical financial observations."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from typing import Any

from alpha_orchestration.data.observations import (
    DataProvider,
    EvidenceRecord,
    FinancialObservation,
    FinancialPeriod,
    FinancialUnit,
    NormalizationIssue,
    ObservationBatch,
    PeriodKind,
    UnitKind,
    bounded_issues,
    canonical_content_hash,
    evidence_id_for,
    observation_id_for,
)
from alpha_orchestration.data.yfinance import MarketSnapshot
from alpha_orchestration.domain import JsonValue


@dataclass(frozen=True, slots=True)
class _HistoryField:
    name: str
    unit_kind: UnitKind
    event_only: bool = False


_HISTORY_FIELDS: dict[str, _HistoryField] = {
    "open": _HistoryField("open_price", UnitKind.CURRENCY_PER_SHARE),
    "high": _HistoryField("high_price", UnitKind.CURRENCY_PER_SHARE),
    "low": _HistoryField("low_price", UnitKind.CURRENCY_PER_SHARE),
    "close": _HistoryField("close_price", UnitKind.CURRENCY_PER_SHARE),
    "adjclose": _HistoryField("adjusted_close_price", UnitKind.CURRENCY_PER_SHARE),
    "volume": _HistoryField("volume", UnitKind.SHARES),
    "dividends": _HistoryField("dividends_per_share", UnitKind.CURRENCY_PER_SHARE, event_only=True),
    "stocksplits": _HistoryField("stock_split_ratio", UnitKind.RATIO, event_only=True),
    "capitalgains": _HistoryField("capital_gains_per_share", UnitKind.CURRENCY_PER_SHARE, event_only=True),
}


def map_yfinance_snapshot(
    snapshot: MarketSnapshot,
    *,
    observed_at: datetime,
    retrieved_at: datetime,
) -> ObservationBatch:
    """Normalize a yfinance fast-info projection at an explicit as-of time."""

    if not isinstance(snapshot, MarketSnapshot):
        raise ValueError("snapshot must be a MarketSnapshot")
    symbol = _symbol(snapshot.ticker)
    issues: list[NormalizationIssue] = []
    currency = _currency(snapshot.currency)
    if currency is None:
        issues.append(
            NormalizationIssue(
                "missing_currency",
                "snapshot.currency",
                "currency is required to normalize price and market-cap values",
            )
        )
        return ObservationBatch(issues=bounded_issues(issues))

    records: list[tuple[FinancialObservation, EvidenceRecord]] = []
    for provider_field, canonical_name, unit_kind in (
        ("last_price", "share_price", UnitKind.CURRENCY_PER_SHARE),
        ("market_cap", "market_cap", UnitKind.CURRENCY),
    ):
        raw_value = getattr(snapshot, provider_field)
        path = f"snapshot.{provider_field}"
        if raw_value is None:
            issues.append(NormalizationIssue("missing_value", path, f"{provider_field} is unavailable"))
            continue
        try:
            value = _market_number(raw_value, provider_field)
        except ValueError as exc:
            issues.append(NormalizationIssue("invalid_value", path, str(exc)))
            continue
        records.append(
            _record(
                symbol=symbol,
                canonical_name=canonical_name,
                provider_field=provider_field,
                value=value,
                currency=currency,
                unit_kind=unit_kind,
                observed_at=_market_datetime(observed_at),
                retrieved_at=retrieved_at,
                source_kind="market_snapshot",
                metadata={
                    "exchange": snapshot.exchange,
                    "currency": currency,
                },
                locator_metadata={"exchange": snapshot.exchange},
            )
        )

    records.sort(key=lambda item: item[0].name)
    return ObservationBatch(
        observations=tuple(item[0] for item in records),
        evidence=tuple(item[1] for item in records),
        issues=bounded_issues(issues),
    )


def map_yfinance_history(
    ticker: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    currency: str,
    auto_adjust: bool,
    retrieved_at: datetime,
    interval: str = "1d",
) -> ObservationBatch:
    """Normalize yfinance OHLCV/action rows without pandas-specific types."""

    symbol = _symbol(ticker)
    normalized_currency = _currency(currency)
    if normalized_currency is None:
        raise ValueError("currency must contain a 3-12 character alphabetic currency code")
    if not isinstance(auto_adjust, bool):
        raise ValueError("auto_adjust must be a boolean")
    if isinstance(rows, (str, bytes)) or not isinstance(rows, Sequence):
        raise ValueError("rows must be a sequence of objects")
    normalized_interval = interval.strip()
    if not normalized_interval or len(normalized_interval) > 20:
        raise ValueError("interval must contain between 1 and 20 characters")

    issues: list[NormalizationIssue] = []
    selected: dict[tuple[str, datetime], tuple[FinancialObservation, EvidenceRecord]] = {}
    for row_index, raw_row in enumerate(rows):
        row_path = f"history[{row_index}]"
        if not isinstance(raw_row, Mapping):
            issues.append(NormalizationIssue("invalid_row", row_path, "history row must be an object"))
            continue
        keyed = {_normalized_key(key): value for key, value in raw_row.items() if isinstance(key, str)}
        raw_timestamp = keyed.get("datetime", keyed.get("date"))
        try:
            observed_at = _market_datetime(raw_timestamp)
        except ValueError as exc:
            issues.append(NormalizationIssue("invalid_timestamp", row_path, str(exc)))
            continue

        for provider_key, field in _HISTORY_FIELDS.items():
            if provider_key not in keyed:
                continue
            raw_value = keyed[provider_key]
            if raw_value is None:
                continue
            path = f"{row_path}.{provider_key}"
            try:
                value = _market_number(raw_value, provider_key)
            except ValueError as exc:
                issues.append(NormalizationIssue("invalid_value", path, str(exc)))
                continue
            if field.event_only and value == 0:
                continue
            try:
                _validate_market_field(value, provider_key)
            except ValueError as exc:
                issues.append(NormalizationIssue("invalid_value", path, str(exc)))
                continue
            record = _record(
                symbol=symbol,
                canonical_name=field.name,
                provider_field=provider_key,
                value=value,
                currency=normalized_currency,
                unit_kind=field.unit_kind,
                observed_at=observed_at,
                retrieved_at=retrieved_at,
                source_kind="price_history",
                metadata={
                    "currency": normalized_currency,
                    "interval": normalized_interval,
                    "auto_adjust": auto_adjust,
                    "adjusted": auto_adjust and provider_key in {"open", "high", "low", "close"},
                },
                locator_metadata={
                    "interval": normalized_interval,
                    "auto_adjust": auto_adjust,
                },
            )
            key = (field.name, observed_at)
            if key in selected:
                issues.append(
                    NormalizationIssue(
                        "duplicate_history_value",
                        path,
                        f"later {field.name} value replaced an earlier value at {observed_at.isoformat()}",
                    )
                )
            selected[key] = record

    records = sorted(selected.values(), key=lambda item: (item[0].period.end, item[0].name, item[0].observation_id))
    return ObservationBatch(
        observations=tuple(item[0] for item in records),
        evidence=tuple(item[1] for item in records),
        issues=bounded_issues(issues),
    )


def _record(
    *,
    symbol: str,
    canonical_name: str,
    provider_field: str,
    value: int | float,
    currency: str,
    unit_kind: UnitKind,
    observed_at: datetime,
    retrieved_at: datetime,
    source_kind: str,
    metadata: dict[str, JsonValue],
    locator_metadata: dict[str, JsonValue],
) -> tuple[FinancialObservation, EvidenceRecord]:
    period = FinancialPeriod(PeriodKind.INSTANT, end=observed_at.date())
    if unit_kind is UnitKind.CURRENCY or unit_kind is UnitKind.CURRENCY_PER_SHARE:
        unit = FinancialUnit(unit_kind, currency)
    elif unit_kind is UnitKind.SHARES:
        unit = FinancialUnit(unit_kind, "shares")
    else:
        unit = FinancialUnit(unit_kind, "pure")
    locator: dict[str, JsonValue] = {
        "ticker": symbol,
        "provider_field": provider_field,
        "observed_at": observed_at.isoformat(),
        "reported_value": value,
        **locator_metadata,
    }
    content_hash = canonical_content_hash({**locator, "currency": currency})
    locator["content_fingerprint"] = content_hash[:16]
    evidence_id = evidence_id_for(DataProvider.YFINANCE, source_kind, locator)
    evidence = EvidenceRecord(
        evidence_id=evidence_id,
        provider=DataProvider.YFINANCE,
        source_kind=source_kind,
        source_locator=locator,
        source_url=f"https://finance.yahoo.com/quote/{symbol}",
        observed_at=observed_at,
        retrieved_at=retrieved_at,
        content_hash=content_hash,
    )
    entity_id = f"ticker:{symbol}"
    observation = FinancialObservation(
        observation_id=observation_id_for(
            DataProvider.YFINANCE,
            entity_id=entity_id,
            name=canonical_name,
            period=period,
            evidence_id=evidence_id,
        ),
        entity_id=entity_id,
        ticker=symbol,
        name=canonical_name,
        value=value,
        unit=unit,
        period=period,
        evidence_ids=(evidence_id,),
        metadata={"provider_field": provider_field, **metadata},
    )
    return observation, evidence


def _market_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, time(), tzinfo=UTC)
    elif isinstance(value, str):
        raw = value.strip()
        if not raw:
            raise ValueError("date or datetime value must not be empty")
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            try:
                parsed = datetime.combine(date.fromisoformat(raw), time(), tzinfo=UTC)
            except ValueError as exc:
                raise ValueError("date or datetime value must use ISO format") from exc
    else:
        raise ValueError("date or datetime value must use ISO format")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _market_number(value: object, field: str) -> int | float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be a finite JSON number")
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{field} must be a finite JSON number")
    return value


def _validate_market_field(value: int | float, field: str) -> None:
    if field in {"open", "high", "low", "close", "adjclose"} and value <= 0:
        raise ValueError(f"{field} must be positive")
    if field in {"volume", "dividends", "capitalgains"} and value < 0:
        raise ValueError(f"{field} must not be negative")
    if field == "stocksplits" and value <= 0:
        raise ValueError("stocksplits must be positive when present")


def _normalized_key(value: str) -> str:
    return "".join(character for character in value.lower() if character.isalnum())


def _symbol(value: str) -> str:
    symbol = value.strip().upper()
    if not symbol or len(symbol) > 20:
        raise ValueError(f"invalid ticker: {value!r}")
    return symbol


def _currency(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().upper()
    if len(normalized) != 3 or not normalized.isalpha():
        return None
    return normalized
