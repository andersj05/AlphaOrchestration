"""Deterministic, source-bound discovery of a broad US public-equity universe.

The universe is intentionally defined by observable eligibility rules rather than by
an opaque index or an arbitrary ticker slice.  Yahoo's equity screener supplies a
market-cap ordered, liquidity-qualified candidate population.  Every selected symbol
must then join to the SEC's official ticker/CIK/exchange association file.

Discovery produces research inputs, not recommendations.  It does not claim that the
SEC association files are complete or that provider market data is independently
verified.
"""

from __future__ import annotations

import asyncio
import math
import re
from collections import Counter
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from alpha_orchestration.data.cache import ContentAddressedJsonCache, ProviderRequest
from alpha_orchestration.data.observations import (
    canonical_content_hash,
)
from alpha_orchestration.data.sec import normalize_cik
from alpha_orchestration.domain import JsonValue

SEC_EXCHANGE_MAP_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
YFINANCE_SCREENER_URL = "https://query2.finance.yahoo.com/v1/finance/screener"

_TICKER = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,19}$")


class SecUniverseProvider(Protocol):
    async def company_tickers_exchange(self) -> dict[str, Any]: ...


class MarketUniverseProvider(Protocol):
    async def screen_equities(self, request: EquityScreenRequest) -> dict[str, Any]: ...


class UniverseDiscoveryError(RuntimeError):
    """Raised when provider data cannot establish a trustworthy universe."""


class UniverseCoverageError(UniverseDiscoveryError):
    """Raised when discovery returns fewer issuers than the minimum viable cohort."""

    def __init__(self, manifest: UniverseManifest) -> None:
        self.manifest = manifest
        super().__init__(
            f"automatic universe produced {len(manifest.members)} eligible issuers; "
            f"minimum is {manifest.policy.minimum_size}"
        )


@dataclass(frozen=True, slots=True)
class UniversePolicy:
    """Controller-owned, reproducible definition of the automatic research universe."""

    profile_id: str = "US_LARGE_LIQUID_V1"
    target_size: int = 300
    minimum_size: int = 200
    page_size: int = 250
    max_screened: int = 1_000
    minimum_market_cap: float = 300_000_000.0
    minimum_share_price: float = 1.0
    minimum_average_daily_volume_3m: float = 200_000.0
    sec_exchanges: tuple[str, ...] = ("Nasdaq", "NYSE")
    market_exchange_codes: tuple[str, ...] = ("NMS", "NGM", "NCM", "NYQ")

    def __post_init__(self) -> None:
        if not isinstance(self.profile_id, str) or not re.fullmatch(r"[A-Z0-9_]{3,64}", self.profile_id):
            raise ValueError("profile_id must be a versioned uppercase identifier")
        if not 100 <= self.minimum_size <= self.target_size <= 1_000:
            raise ValueError("universe sizes must satisfy 100 <= minimum <= target <= 1000")
        if not 1 <= self.page_size <= 250:
            raise ValueError("universe page_size must be between 1 and 250")
        if not self.target_size <= self.max_screened <= 5_000:
            raise ValueError("max_screened must be between target_size and 5000")
        for name in (
            "minimum_market_cap",
            "minimum_share_price",
            "minimum_average_daily_volume_3m",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be a finite positive number")
            object.__setattr__(self, name, float(value))
        object.__setattr__(self, "sec_exchanges", _normalized_unique(self.sec_exchanges, "sec_exchanges"))
        object.__setattr__(
            self,
            "market_exchange_codes",
            tuple(
                dict.fromkeys(
                    value.upper() for value in _normalized_unique(self.market_exchange_codes, "market_exchange_codes")
                )
            ),
        )

    @property
    def policy_hash(self) -> str:
        return canonical_content_hash(self.to_dict())

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "profile_id": self.profile_id,
            "target_size": self.target_size,
            "minimum_size": self.minimum_size,
            "page_size": self.page_size,
            "max_screened": self.max_screened,
            "minimum_market_cap": self.minimum_market_cap,
            "minimum_share_price": self.minimum_share_price,
            "minimum_average_daily_volume_3m": self.minimum_average_daily_volume_3m,
            "sec_exchanges": list(self.sec_exchanges),
            "market_exchange_codes": list(self.market_exchange_codes),
            "ranking": "market_cap_desc_then_ticker_asc",
            "security_type": "equity",
            "region": "us",
        }


@dataclass(frozen=True, slots=True)
class EquityScreenRequest:
    """Provider-neutral identity for one bounded page of the equity screen."""

    offset: int
    size: int
    exchange_codes: tuple[str, ...]
    minimum_market_cap: float
    minimum_share_price: float
    minimum_average_daily_volume_3m: float

    def __post_init__(self) -> None:
        if not isinstance(self.offset, int) or isinstance(self.offset, bool) or not 0 <= self.offset <= 5_000:
            raise ValueError("screen offset must be an integer between 0 and 5000")
        if not isinstance(self.size, int) or isinstance(self.size, bool) or not 1 <= self.size <= 250:
            raise ValueError("screen size must be an integer between 1 and 250")
        codes = tuple(
            dict.fromkeys(value.upper() for value in _normalized_unique(self.exchange_codes, "exchange_codes"))
        )
        object.__setattr__(self, "exchange_codes", codes)
        for name in (
            "minimum_market_cap",
            "minimum_share_price",
            "minimum_average_daily_volume_3m",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be a finite positive number")
            object.__setattr__(self, name, float(value))

    def to_parameters(self) -> dict[str, JsonValue]:
        return {
            "offset": self.offset,
            "size": self.size,
            "exchange_codes": list(self.exchange_codes),
            "minimum_market_cap": self.minimum_market_cap,
            "minimum_share_price": self.minimum_share_price,
            "minimum_average_daily_volume_3m": self.minimum_average_daily_volume_3m,
            "region": "us",
            "quote_type": "EQUITY",
            "sort_field": "intradaymarketcap",
            "sort_ascending": False,
        }


@dataclass(frozen=True, slots=True)
class UniverseSource:
    provider: str
    source_kind: str
    source_url: str
    retrieved_at: datetime
    content_hash: str
    request_hash: str
    access_mode: str
    row_count: int

    def __post_init__(self) -> None:
        if not all(
            isinstance(value, str) and value.strip() for value in (self.provider, self.source_kind, self.source_url)
        ):
            raise ValueError("universe source identity fields must not be empty")
        for name in ("content_hash", "request_hash"):
            if not _is_digest(getattr(self, name)):
                raise ValueError(f"universe source {name} must be a SHA-256 digest")
        _aware_utc(self.retrieved_at, "source retrieved_at")
        if self.access_mode not in {"cache", "network"}:
            raise ValueError("source access_mode must be cache or network")
        if self.row_count < 0:
            raise ValueError("source row_count must not be negative")

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "provider": self.provider,
            "source_kind": self.source_kind,
            "source_url": self.source_url,
            "retrieved_at": self.retrieved_at.astimezone(UTC).isoformat(),
            "content_hash": self.content_hash,
            "request_hash": self.request_hash,
            "access_mode": self.access_mode,
            "row_count": self.row_count,
        }


@dataclass(frozen=True, slots=True)
class UniverseMember:
    rank: int
    ticker: str
    cik: str
    company: str
    sec_exchange: str
    market_exchange_code: str
    market_cap: float
    share_price: float
    average_daily_volume_3m: float
    currency: str
    sec_content_hash: str
    market_content_hash: str
    market_request_hash: str
    market_retrieved_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.rank, int) or isinstance(self.rank, bool) or self.rank < 1:
            raise ValueError("universe member rank must be a positive integer")
        if not _TICKER.fullmatch(self.ticker):
            raise ValueError("universe member ticker is invalid")
        if normalize_cik(self.cik) != self.cik:
            raise ValueError("universe member CIK must be normalized")
        if not all(
            isinstance(value, str) and value.strip()
            for value in (self.company, self.sec_exchange, self.market_exchange_code)
        ):
            raise ValueError("universe member identity fields must not be empty")
        for name in ("market_cap", "share_price", "average_daily_volume_3m"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"universe member {name} must be a finite positive number")
        if not isinstance(self.currency, str) or not re.fullmatch(r"[A-Z]{3}", self.currency):
            raise ValueError("universe member currency must be a three-letter uppercase code")
        for name in ("sec_content_hash", "market_content_hash", "market_request_hash"):
            if not _is_digest(getattr(self, name)):
                raise ValueError(f"universe member {name} must be a SHA-256 digest")
        _aware_utc(self.market_retrieved_at, "member market_retrieved_at")

    @property
    def entity_id(self) -> str:
        return f"ticker:{self.ticker}"

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "rank": self.rank,
            "entity_id": self.entity_id,
            "ticker": self.ticker,
            "cik": self.cik,
            "company": self.company,
            "sec_exchange": self.sec_exchange,
            "market_exchange_code": self.market_exchange_code,
            "market_cap": self.market_cap,
            "share_price": self.share_price,
            "average_daily_volume_3m": self.average_daily_volume_3m,
            "currency": self.currency,
            "source_content_hashes": [self.sec_content_hash, self.market_content_hash],
            "source_lineage": {
                "sec_identity": {
                    "provider": "sec",
                    "source_kind": "company_ticker_exchange_identity",
                    "source_url": SEC_EXCHANGE_MAP_URL,
                    "ticker": self.ticker,
                    "cik": self.cik,
                    "content_hash": self.sec_content_hash,
                },
                "market_screen": {
                    "provider": "yfinance",
                    "source_kind": "equity_screen_page",
                    "source_url": YFINANCE_SCREENER_URL,
                    "ticker": self.ticker,
                    "request_hash": self.market_request_hash,
                    "content_hash": self.market_content_hash,
                    "retrieved_at": self.market_retrieved_at.astimezone(UTC).isoformat(),
                    "currency": self.currency,
                },
            },
        }


@dataclass(frozen=True, slots=True)
class UniverseExclusion:
    ticker: str
    reason: str
    detail: str

    def to_dict(self) -> dict[str, JsonValue]:
        return {"ticker": self.ticker, "reason": self.reason, "detail": self.detail}


@dataclass(frozen=True, slots=True)
class UniverseManifest:
    policy: UniversePolicy
    members: tuple[UniverseMember, ...]
    exclusions: tuple[UniverseExclusion, ...]
    discovered_at: datetime
    sec_source: UniverseSource
    market_sources: tuple[UniverseSource, ...]
    provider_reported_total: int
    screened_unique_count: int
    fetched_row_count: int

    def __post_init__(self) -> None:
        _aware_utc(self.discovered_at, "manifest discovered_at")
        if min(self.provider_reported_total, self.screened_unique_count, self.fetched_row_count) < 0:
            raise ValueError("universe coverage counts must not be negative")
        if not (
            len(self.members)
            <= self.screened_unique_count
            <= self.fetched_row_count
            <= self.provider_reported_total
        ):
            raise ValueError(
                "universe coverage must satisfy selected <= valid unique <= fetched <= provider total"
            )
        if self.fetched_row_count != sum(source.row_count for source in self.market_sources):
            raise ValueError("universe fetched coverage must equal the retained market source rows")
        expected = tuple(range(1, len(self.members) + 1))
        if tuple(member.rank for member in self.members) != expected:
            raise ValueError("universe member ranks must be contiguous and one-based")
        tickers = self.tickers
        if len(set(tickers)) != len(tickers):
            raise ValueError("universe members must contain unique tickers")
        market_lineage = {
            (source.content_hash, source.request_hash, source.retrieved_at.astimezone(UTC))
            for source in self.market_sources
        }
        for member in self.members:
            if member.sec_content_hash != self.sec_source.content_hash:
                raise ValueError(
                    f"universe member SEC lineage is not present in the manifest: {member.ticker}"
                )
            member_lineage = (
                member.market_content_hash,
                member.market_request_hash,
                member.market_retrieved_at.astimezone(UTC),
            )
            if member_lineage not in market_lineage:
                raise ValueError(
                    f"universe member market lineage is not present in the manifest: {member.ticker}"
                )

    @property
    def tickers(self) -> tuple[str, ...]:
        return tuple(member.ticker for member in self.members)

    @property
    def posture(self) -> str:
        if len(self.members) >= self.policy.target_size:
            return "target_met"
        if len(self.members) >= self.policy.minimum_size:
            return "minimum_met"
        return "insufficient"

    @property
    def content_hash(self) -> str:
        identity: dict[str, JsonValue] = {
            "policy": self.policy.to_dict(),
            "members": [member.to_dict() for member in self.members],
            "exclusions": [exclusion.to_dict() for exclusion in self.exclusions],
            "sec_content_hash": self.sec_source.content_hash,
            "sec_request_hash": self.sec_source.request_hash,
            "market_content_hashes": [source.content_hash for source in self.market_sources],
            "market_request_hashes": [source.request_hash for source in self.market_sources],
            "provider_reported_total": self.provider_reported_total,
            "screened_unique_count": self.screened_unique_count,
            "fetched_row_count": self.fetched_row_count,
        }
        return canonical_content_hash(identity)

    @property
    def manifest_id(self) -> str:
        return f"universe-{self.content_hash[:16]}"

    def require_minimum(self) -> UniverseManifest:
        if self.posture == "insufficient":
            raise UniverseCoverageError(self)
        return self

    def snapshot(self, *, include_members: bool = True) -> dict[str, JsonValue]:
        exclusion_counts = Counter(exclusion.reason for exclusion in self.exclusions)
        snapshot: dict[str, JsonValue] = {
            "manifest_id": self.manifest_id,
            "content_hash": self.content_hash,
            "discovered_at": self.discovered_at.astimezone(UTC).isoformat(),
            "posture": self.posture,
            "research_candidate_status": "screen inputs; not investment recommendations",
            "policy": self.policy.to_dict(),
            "policy_hash": self.policy.policy_hash,
            "selected_count": len(self.members),
            "screened_unique_count": self.screened_unique_count,
            "provider_reported_total": self.provider_reported_total,
            "fetched_row_count": self.fetched_row_count,
            "provider_rows_not_inspected": max(0, self.provider_reported_total - self.fetched_row_count),
            "target_coverage": len(self.members) / self.policy.target_size,
            "exclusion_counts": dict(sorted(exclusion_counts.items())),
            "exclusion_samples": [item.to_dict() for item in self.exclusions[:25]],
            "sec_source": self.sec_source.to_dict(),
            "market_sources": [source.to_dict() for source in self.market_sources],
        }
        if include_members:
            snapshot["members"] = [member.to_dict() for member in self.members]
        return snapshot


@dataclass(frozen=True, slots=True)
class _SecIdentity:
    ticker: str
    cik: str
    company: str
    exchange: str


@dataclass(frozen=True, slots=True)
class _ScreenRow:
    ticker: str
    exchange_code: str
    market_cap: float
    share_price: float
    average_daily_volume_3m: float
    currency: str
    source_content_hash: str
    source_request_hash: str
    source_retrieved_at: datetime


class AutomaticUniverseDiscovery:
    """Build a broad automatic cohort from cached or live SEC/yfinance inputs."""

    def __init__(
        self,
        sec: SecUniverseProvider,
        market: MarketUniverseProvider,
        cache: ContentAddressedJsonCache,
        *,
        cache_max_age: timedelta = timedelta(hours=6),
        sec_map_max_age: timedelta = timedelta(days=7),
        provider_timeout_seconds: float = 30.0,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if cache_max_age < timedelta(0) or sec_map_max_age < timedelta(0):
            raise ValueError("universe cache ages must not be negative")
        if not 0 < provider_timeout_seconds <= 120:
            raise ValueError("provider_timeout_seconds must be in (0, 120]")
        self.sec = sec
        self.market = market
        self.cache = cache
        self.cache_max_age = cache_max_age
        self.sec_map_max_age = sec_map_max_age
        self.provider_timeout_seconds = provider_timeout_seconds
        self._now = now or (lambda: datetime.now(UTC))

    async def discover(self, policy: UniversePolicy | None = None) -> UniverseManifest:
        selected_policy = policy or UniversePolicy()
        sec_payload, sec_source = await self._sec_exchange_map()
        sec_identities = _parse_sec_exchange_map(sec_payload)

        rows: list[_ScreenRow] = []
        exclusions: list[UniverseExclusion] = []
        market_sources: list[UniverseSource] = []
        provider_total: int | None = None
        offset = 0
        while offset < selected_policy.max_screened:
            size = min(selected_policy.page_size, selected_policy.max_screened - offset)
            request = EquityScreenRequest(
                offset=offset,
                size=size,
                exchange_codes=selected_policy.market_exchange_codes,
                minimum_market_cap=selected_policy.minimum_market_cap,
                minimum_share_price=selected_policy.minimum_share_price,
                minimum_average_daily_volume_3m=selected_policy.minimum_average_daily_volume_3m,
            )
            payload, source = await self._market_screen_page(request)
            page_rows, page_exclusions, page_total = _parse_market_page(
                payload, source.content_hash, source.request_hash, source.retrieved_at, selected_policy
            )
            rows.extend(page_rows)
            exclusions.extend(page_exclusions)
            market_sources.append(source)
            if provider_total is not None and page_total != provider_total:
                raise UniverseDiscoveryError("market equity screen reported conflicting totals across pages")
            provider_total = page_total

            eligible, _ = _eligible_rows(rows, sec_identities, selected_policy)
            if len(eligible) >= selected_policy.target_size:
                break
            if not page_rows or offset + size >= page_total:
                break
            offset += size

        eligible, join_exclusions = _eligible_rows(rows, sec_identities, selected_policy)
        exclusions.extend(join_exclusions)
        ordered = sorted(eligible, key=lambda item: (-item[1].market_cap, item[0].ticker))
        selected = ordered[: selected_policy.target_size]
        for identity, _ in ordered[selected_policy.target_size :]:
            exclusions.append(
                UniverseExclusion(
                    identity.ticker,
                    "below_target_cutoff",
                    "qualified but ranked below the controller-owned analysis budget",
                )
            )

        members = tuple(
            UniverseMember(
                rank=index,
                ticker=identity.ticker,
                cik=identity.cik,
                company=identity.company,
                sec_exchange=identity.exchange,
                market_exchange_code=row.exchange_code,
                market_cap=row.market_cap,
                share_price=row.share_price,
                average_daily_volume_3m=row.average_daily_volume_3m,
                currency=row.currency,
                sec_content_hash=sec_source.content_hash,
                market_content_hash=row.source_content_hash,
                market_request_hash=row.source_request_hash,
                market_retrieved_at=row.source_retrieved_at,
            )
            for index, (identity, row) in enumerate(selected, start=1)
        )
        manifest = UniverseManifest(
            policy=selected_policy,
            members=members,
            exclusions=tuple(sorted(exclusions, key=lambda item: (item.reason, item.ticker, item.detail))),
            discovered_at=self._checked_now(),
            sec_source=sec_source,
            market_sources=tuple(market_sources),
            provider_reported_total=0 if provider_total is None else provider_total,
            screened_unique_count=len({row.ticker for row in rows}),
            fetched_row_count=sum(source.row_count for source in market_sources),
        )
        return manifest.require_minimum()

    async def _sec_exchange_map(self) -> tuple[dict[str, Any], UniverseSource]:
        request = ProviderRequest("sec", "company_tickers_exchange", "official-us-listed-exchange-map", {})
        return await self._cached_payload(
            request,
            max_age=self.sec_map_max_age,
            source_kind="company_ticker_exchange_identity",
            source_url=SEC_EXCHANGE_MAP_URL,
            call=self.sec.company_tickers_exchange,
        )

    async def _market_screen_page(self, request: EquityScreenRequest) -> tuple[dict[str, Any], UniverseSource]:
        cache_request = ProviderRequest(
            "yfinance",
            "equity_screen",
            "liquid-us-major-exchange-equities",
            request.to_parameters(),
        )
        return await self._cached_payload(
            cache_request,
            max_age=self.cache_max_age,
            source_kind="equity_screen_page",
            source_url=YFINANCE_SCREENER_URL,
            call=lambda: self.market.screen_equities(request),
        )

    async def _cached_payload(
        self,
        request: ProviderRequest,
        *,
        max_age: timedelta,
        source_kind: str,
        source_url: str,
        call: Callable[[], Awaitable[dict[str, Any]]],
    ) -> tuple[dict[str, Any], UniverseSource]:
        record = self.cache.get(request, max_age=max_age, now=self._checked_now())
        if record is not None and record.fresh:
            payload = _object_payload(record.payload)
            source = UniverseSource(
                request.provider,
                source_kind,
                source_url,
                record.fetched_at,
                record.content_hash,
                request.request_hash,
                "cache",
                _payload_row_count(payload),
            )
            return payload, source
        payload = _object_payload(await asyncio.wait_for(call(), timeout=self.provider_timeout_seconds))
        fetched_at = self._checked_now()
        stored = self.cache.put(request, payload, fetched_at=fetched_at)
        source = UniverseSource(
            request.provider,
            source_kind,
            source_url,
            stored.fetched_at,
            stored.content_hash,
            request.request_hash,
            "network",
            _payload_row_count(payload),
        )
        return payload, source

    def _checked_now(self) -> datetime:
        return _aware_utc(self._now(), "universe discovery clock")


def _parse_sec_exchange_map(payload: Mapping[str, Any]) -> dict[str, _SecIdentity]:
    fields = payload.get("fields")
    data = payload.get("data")
    if not isinstance(fields, list) or not all(isinstance(field, str) for field in fields):
        raise UniverseDiscoveryError("SEC exchange map fields are missing or invalid")
    if not isinstance(data, list):
        raise UniverseDiscoveryError("SEC exchange map data is missing or invalid")
    required = ("cik", "name", "ticker", "exchange")
    if any(field not in fields for field in required):
        raise UniverseDiscoveryError("SEC exchange map does not contain the required identity fields")
    indexes = {field: fields.index(field) for field in required}
    identities: dict[str, _SecIdentity] = {}
    for raw in data:
        if not isinstance(raw, list) or len(raw) < len(fields):
            continue
        ticker = str(raw[indexes["ticker"]]).strip().upper()
        company = str(raw[indexes["name"]]).strip()
        exchange = str(raw[indexes["exchange"]]).strip()
        try:
            cik = normalize_cik(raw[indexes["cik"]])
        except ValueError:
            continue
        if not _TICKER.fullmatch(ticker) or not company or not exchange:
            continue
        identity = _SecIdentity(ticker, cik, company, exchange)
        existing = identities.get(ticker)
        if existing is not None and existing != identity:
            raise UniverseDiscoveryError(f"SEC exchange map contains conflicting identity rows for {ticker}")
        identities[ticker] = identity
    if not identities:
        raise UniverseDiscoveryError("SEC exchange map did not contain any valid identity rows")
    return identities


def _parse_market_page(
    payload: Mapping[str, Any],
    source_content_hash: str,
    source_request_hash: str,
    source_retrieved_at: datetime,
    policy: UniversePolicy,
) -> tuple[list[_ScreenRow], list[UniverseExclusion], int]:
    quotes = payload.get("quotes")
    if not isinstance(quotes, list):
        raise UniverseDiscoveryError("market equity screen did not contain a quotes list")
    total = _nonnegative_int(payload.get("total"), "market screen total")
    rows: list[_ScreenRow] = []
    exclusions: list[UniverseExclusion] = []
    for raw in quotes:
        if not isinstance(raw, Mapping):
            exclusions.append(UniverseExclusion("UNKNOWN", "invalid_market_row", "quote row is not an object"))
            continue
        ticker = str(_first_present(raw, "symbol", "ticker") or "").strip().upper()
        safe_ticker = ticker if _TICKER.fullmatch(ticker) else "UNKNOWN"
        quote_type = str(_first_present(raw, "quoteType", "quote_type") or "").strip().upper()
        exchange = str(_first_present(raw, "exchange", "exchangeCode") or "").strip().upper()
        market_cap = _finite_number(_first_present(raw, "marketCap", "intradaymarketcap"))
        price = _finite_number(_first_present(raw, "regularMarketPrice", "intradayprice", "price"))
        volume = _finite_number(_first_present(raw, "averageDailyVolume3Month", "avgdailyvol3m"))
        currency_value = _first_present(raw, "currency", "financialCurrency")
        currency = None if currency_value is None else str(currency_value).strip().upper() or None
        reason = None
        detail = None
        if safe_ticker == "UNKNOWN":
            reason, detail = "invalid_market_row", "missing or invalid ticker"
        elif quote_type != "EQUITY":
            reason, detail = "non_equity", f"provider quote type was {quote_type}"
        elif exchange not in policy.market_exchange_codes:
            reason, detail = "market_exchange_outside_policy", f"provider exchange was {exchange or 'missing'}"
        elif market_cap is None or price is None or volume is None or currency is None:
            reason, detail = (
                "incomplete_screen_metrics",
                "market cap, price, three-month volume, or currency was missing",
            )
        elif market_cap < policy.minimum_market_cap or price < policy.minimum_share_price:
            reason, detail = "failed_market_threshold", "market cap or share price was below policy"
        elif volume < policy.minimum_average_daily_volume_3m:
            reason, detail = "failed_liquidity_threshold", "three-month average volume was below policy"
        if reason is not None:
            exclusions.append(UniverseExclusion(safe_ticker, reason, detail or reason))
            continue
        assert market_cap is not None and price is not None and volume is not None and currency is not None
        rows.append(
            _ScreenRow(
                ticker,
                exchange,
                market_cap,
                price,
                volume,
                currency,
                source_content_hash,
                source_request_hash,
                source_retrieved_at,
            )
        )
    return rows, exclusions, total


def _eligible_rows(
    rows: Sequence[_ScreenRow],
    identities: Mapping[str, _SecIdentity],
    policy: UniversePolicy,
) -> tuple[list[tuple[_SecIdentity, _ScreenRow]], list[UniverseExclusion]]:
    best_by_ticker: dict[str, _ScreenRow] = {}
    exclusions: list[UniverseExclusion] = []
    for row in rows:
        existing = best_by_ticker.get(row.ticker)
        if existing is not None:
            exclusions.append(
                UniverseExclusion(row.ticker, "duplicate_market_row", "duplicate screener symbol was de-duplicated")
            )
            if (row.market_cap, row.source_content_hash) <= (existing.market_cap, existing.source_content_hash):
                continue
        best_by_ticker[row.ticker] = row

    allowed_sec = {exchange.casefold() for exchange in policy.sec_exchanges}
    eligible: list[tuple[_SecIdentity, _ScreenRow]] = []
    for ticker, row in sorted(best_by_ticker.items()):
        identity = identities.get(ticker)
        if identity is None:
            exclusions.append(
                UniverseExclusion(
                    ticker,
                    "missing_sec_identity",
                    "ticker was absent from the official SEC exchange map",
                )
            )
        elif identity.exchange.casefold() not in allowed_sec:
            exclusions.append(
                UniverseExclusion(
                    ticker,
                    "sec_exchange_outside_policy",
                    f"official SEC exchange was {identity.exchange}",
                )
            )
        else:
            eligible.append((identity, row))

    deduplicated: list[tuple[_SecIdentity, _ScreenRow]] = []
    kept_by_cik: dict[str, str] = {}
    for identity, row in sorted(eligible, key=lambda item: (-item[1].market_cap, item[0].ticker)):
        kept_ticker = kept_by_cik.get(identity.cik)
        if kept_ticker is not None:
            exclusions.append(
                UniverseExclusion(
                    identity.ticker,
                    "duplicate_issuer",
                    f"same verified SEC CIK as higher-ranked share class {kept_ticker}",
                )
            )
            continue
        kept_by_cik[identity.cik] = identity.ticker
        deduplicated.append((identity, row))
    return deduplicated, exclusions


def _payload_row_count(payload: Mapping[str, Any]) -> int:
    data = payload.get("data")
    quotes = payload.get("quotes")
    if isinstance(data, list):
        return len(data)
    if isinstance(quotes, list):
        return len(quotes)
    return 0


def _first_present(values: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in values:
            return values[key]
    return None


def _finite_number(value: object) -> float | None:
    if isinstance(value, Mapping):
        value = value.get("raw")
    if isinstance(value, bool):
        return None
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _nonnegative_int(value: object, label: str) -> int:
    if isinstance(value, bool):
        raise UniverseDiscoveryError(f"{label} must be a non-negative integer")
    try:
        number = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise UniverseDiscoveryError(f"{label} must be a non-negative integer") from exc
    if number < 0:
        raise UniverseDiscoveryError(f"{label} must be a non-negative integer")
    return number


def _normalized_unique(values: Sequence[str], label: str) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise ValueError(f"{label} must be a sequence of strings")
    normalized: list[str] = []
    for raw in values:
        if not isinstance(raw, str) or not raw.strip():
            raise ValueError(f"{label} must contain non-empty strings")
        value = raw.strip()
        if value not in normalized:
            normalized.append(value)
    if not normalized:
        raise ValueError(f"{label} must not be empty")
    return tuple(normalized)


def _object_payload(payload: object) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise UniverseDiscoveryError("universe provider payload must be a JSON object")
    return dict(payload)


def _aware_utc(value: datetime, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware")
    return value.astimezone(UTC)


def _is_digest(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(character in "0123456789abcdef" for character in value)
