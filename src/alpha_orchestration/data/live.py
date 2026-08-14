"""Bounded live SEC/yfinance collection with controller-owned issuer identity."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from alpha_orchestration.data.cache import ContentAddressedJsonCache, ProviderRequest
from alpha_orchestration.data.ledger import EvidencePacket, ObservationLedger
from alpha_orchestration.data.observations import (
    DataProvider,
    EvidenceRecord,
    FinancialObservation,
    ObservationBatch,
    PeriodKind,
    UnitKind,
    canonical_content_hash,
    evidence_id_for,
    observation_id_for,
)
from alpha_orchestration.data.sec import map_sec_company_facts, normalize_cik
from alpha_orchestration.data.yfinance import MarketSnapshot, map_yfinance_snapshot
from alpha_orchestration.domain import JsonValue

_TICKER = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,19}$")
MAX_LIVE_TICKERS = 8


class SecLiveProvider(Protocol):
    async def company_tickers(self) -> dict[str, Any]: ...

    async def company_facts(self, cik: str | int) -> dict[str, Any]: ...


class MarketLiveProvider(Protocol):
    async def snapshot(self, ticker: str) -> MarketSnapshot: ...


class LiveCollectionError(RuntimeError):
    """Raised when collection cannot produce any trusted issuer packet."""


@dataclass(frozen=True, slots=True)
class ResolvedIssuer:
    ticker: str
    cik: str
    company: str

    @property
    def entity_id(self) -> str:
        return f"ticker:{self.ticker}"


@dataclass(frozen=True, slots=True)
class CollectionFailure:
    ticker: str
    provider: str
    phase: str
    error: str
    retryable: bool
    occurred_at: datetime

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "ticker": self.ticker,
            "provider": self.provider,
            "phase": self.phase,
            "error": self.error,
            "retryable": self.retryable,
            "occurred_at": self.occurred_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class LiveIssuerEvidence:
    issuer: ResolvedIssuer
    packet: EvidencePacket
    identity_evidence: EvidenceRecord
    observation_ids: Mapping[str, str]
    observations_by_name: Mapping[str, FinancialObservation]
    provider_status: Mapping[str, Mapping[str, JsonValue]]
    normalization_issues: tuple[str, ...]

    @property
    def task_id(self) -> str:
        return f"analyze-{self.issuer.ticker.lower().replace('.', '-')}"

    @property
    def source_ids(self) -> tuple[str, ...]:
        return (self.identity_evidence.evidence_id, *self.packet.source_ids)


@dataclass(frozen=True, slots=True)
class LiveCollection:
    requested_tickers: tuple[str, ...]
    issuers: tuple[LiveIssuerEvidence, ...]
    failures: tuple[CollectionFailure, ...]
    mapping_retrieved_at: datetime
    mapping_content_hash: str
    configured_provider_slots: int
    observed_peak_provider_requests: int

    def snapshot(self) -> dict[str, JsonValue]:
        requested = len(self.requested_tickers)
        ready = len(self.issuers)
        provider_successes = {"sec": 0, "yfinance": 0}
        provider_failures = {"sec": 0, "yfinance": 0}
        issuer_rows: list[JsonValue] = []
        for evidence in self.issuers:
            for provider in provider_successes:
                status = evidence.provider_status.get(provider, {}).get("status")
                if status == "ok":
                    provider_successes[provider] += 1
            issuer_rows.append(
                {
                    "ticker": evidence.issuer.ticker,
                    "cik": evidence.issuer.cik,
                    "company": evidence.issuer.company,
                    "status": "ready",
                    "providers": {key: dict(value) for key, value in evidence.provider_status.items()},
                    "observation_count": len(evidence.packet.observations),
                    "evidence_count": len(evidence.source_ids),
                    "normalization_issue_count": len(evidence.normalization_issues),
                }
            )
        ready_tickers = {issuer.issuer.ticker for issuer in self.issuers}
        for ticker in self.requested_tickers:
            if ticker not in ready_tickers:
                issuer_rows.append({"ticker": ticker, "status": "failed"})
        for failure in self.failures:
            if failure.provider in provider_failures:
                provider_failures[failure.provider] += 1
        return {
            "requested_tickers": list(self.requested_tickers),
            "requested_count": requested,
            "ready_count": ready,
            "failed_count": requested - ready,
            "partial": ready != requested or bool(self.failures),
            "issuers": issuer_rows,
            "failures": [failure.to_dict() for failure in self.failures],
            "provider_successes": provider_successes,
            "provider_failures": provider_failures,
            "mapping": {
                "provider": "sec",
                "source_url": "https://www.sec.gov/files/company_tickers.json",
                "retrieved_at": self.mapping_retrieved_at.isoformat(),
                "content_hash": self.mapping_content_hash,
            },
            "configured_provider_slots": self.configured_provider_slots,
            "observed_peak_provider_requests": self.observed_peak_provider_requests,
        }


def normalize_live_tickers(tickers: Sequence[str]) -> tuple[str, ...]:
    if isinstance(tickers, (str, bytes)) or not isinstance(tickers, Sequence):
        raise ValueError("tickers must be a sequence of strings")
    normalized: list[str] = []
    for raw in tickers:
        if not isinstance(raw, str):
            raise ValueError("tickers must contain only strings")
        ticker = raw.strip().upper()
        if not _TICKER.fullmatch(ticker):
            raise ValueError(f"invalid ticker: {raw!r}")
        if ticker not in normalized:
            normalized.append(ticker)
    if not 1 <= len(normalized) <= MAX_LIVE_TICKERS:
        raise ValueError(f"live ticker universe must contain between 1 and {MAX_LIVE_TICKERS} unique tickers")
    return tuple(normalized)


class LiveDataCollector:
    """Resolve identities then collect a bounded packet for each requested issuer."""

    def __init__(
        self,
        sec: SecLiveProvider,
        market: MarketLiveProvider,
        cache: ContentAddressedJsonCache,
        *,
        cache_max_age: timedelta = timedelta(hours=6),
        ticker_map_max_age: timedelta = timedelta(days=7),
        provider_slots: int = 4,
        provider_timeout_seconds: float = 30.0,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if not 1 <= provider_slots <= 8:
            raise ValueError("provider_slots must be between 1 and 8")
        if not 0 < provider_timeout_seconds <= 120:
            raise ValueError("provider_timeout_seconds must be in (0, 120]")
        if cache_max_age < timedelta(0) or ticker_map_max_age < timedelta(0):
            raise ValueError("cache ages must not be negative")
        self.sec = sec
        self.market = market
        self.cache = cache
        self.cache_max_age = cache_max_age
        self.ticker_map_max_age = ticker_map_max_age
        self.provider_slots = provider_slots
        self.provider_timeout_seconds = provider_timeout_seconds
        self._now = now or (lambda: datetime.now(UTC))
        self._semaphore = asyncio.Semaphore(provider_slots)
        self._active_provider_requests = 0
        self._peak_provider_requests = 0

    async def collect(self, tickers: Sequence[str]) -> LiveCollection:
        requested = normalize_live_tickers(tickers)
        mapping_payload, mapping_at, mapping_hash, _ = await self._company_tickers()
        mapping = _parse_company_tickers(mapping_payload)
        failures: list[CollectionFailure] = []
        resolved: list[ResolvedIssuer] = []
        for ticker in requested:
            issuer = mapping.get(ticker)
            if issuer is None:
                failures.append(
                    self._failure(ticker, "sec", "ticker_resolution", "ticker is absent from the official SEC map")
                )
            else:
                resolved.append(issuer)

        results = await asyncio.gather(
            *(self._collect_issuer(issuer, mapping_at, mapping_hash) for issuer in resolved),
            return_exceptions=True,
        )
        issuer_evidence: list[LiveIssuerEvidence] = []
        for issuer, result in zip(resolved, results, strict=True):
            if isinstance(result, Exception):
                failures.append(self._failure(issuer.ticker, "sec", "issuer_collection", _safe_error(result)))
            else:
                evidence, issuer_failures = result
                failures.extend(issuer_failures)
                if evidence is not None:
                    issuer_evidence.append(evidence)
        return LiveCollection(
            requested_tickers=requested,
            issuers=tuple(issuer_evidence),
            failures=tuple(failures),
            mapping_retrieved_at=mapping_at,
            mapping_content_hash=mapping_hash,
            configured_provider_slots=self.provider_slots,
            observed_peak_provider_requests=self._peak_provider_requests,
        )

    async def _collect_issuer(
        self,
        issuer: ResolvedIssuer,
        mapping_at: datetime,
        mapping_hash: str,
    ) -> tuple[LiveIssuerEvidence | None, tuple[CollectionFailure, ...]]:
        sec_result, market_result = await asyncio.gather(
            self._company_facts(issuer),
            self._market_snapshot(issuer),
            return_exceptions=True,
        )
        failures: list[CollectionFailure] = []
        statuses: dict[str, Mapping[str, JsonValue]] = {}
        if isinstance(sec_result, Exception):
            failures.append(self._failure(issuer.ticker, "sec", "company_facts", _safe_error(sec_result)))
            statuses["sec"] = {"status": "failed", "error": _safe_error(sec_result)}
            return None, tuple(failures)

        sec_payload, sec_at, sec_source = sec_result
        try:
            sec_batch = _canonicalize_sec_batch(
                map_sec_company_facts(sec_payload, retrieved_at=sec_at, ticker=issuer.ticker),
                issuer,
            )
        except Exception as exc:
            failures.append(self._failure(issuer.ticker, "sec", "normalization", _safe_error(exc)))
            return None, tuple(failures)
        statuses["sec"] = {
            "status": "ok",
            "source": sec_source,
            "retrieved_at": sec_at.isoformat(),
        }

        batches = [sec_batch]
        if isinstance(market_result, Exception):
            failures.append(self._failure(issuer.ticker, "yfinance", "snapshot", _safe_error(market_result)))
            statuses["yfinance"] = {"status": "failed", "error": _safe_error(market_result)}
        else:
            snapshot, market_at, market_source = market_result
            try:
                batches.append(map_yfinance_snapshot(snapshot, observed_at=market_at, retrieved_at=market_at))
                statuses["yfinance"] = {
                    "status": "ok",
                    "source": market_source,
                    "retrieved_at": market_at.isoformat(),
                }
            except Exception as exc:
                failures.append(self._failure(issuer.ticker, "yfinance", "normalization", _safe_error(exc)))
                statuses["yfinance"] = {"status": "failed", "error": _safe_error(exc)}

        ledger = ObservationLedger(batches)
        selected = _select_analysis_observations(ledger, issuer)
        if "revenue" not in selected:
            failures.append(
                self._failure(issuer.ticker, "sec", "evidence_selection", "no comparable annual revenue fact")
            )
            return None, tuple(failures)
        packet = ledger.evidence_packet([observation.observation_id for observation in selected.values()])
        issues = tuple(f"{issue.code}: {issue.message}" for batch in batches for issue in batch.issues)
        identity = _identity_evidence(issuer, mapping_at, mapping_hash)
        return (
            LiveIssuerEvidence(
                issuer=issuer,
                packet=packet,
                identity_evidence=identity,
                observation_ids={name: observation.observation_id for name, observation in selected.items()},
                observations_by_name=dict(selected),
                provider_status=statuses,
                normalization_issues=issues,
            ),
            tuple(failures),
        )

    async def _company_tickers(self) -> tuple[dict[str, Any], datetime, str, str]:
        request = ProviderRequest("sec", "company_tickers", "official-us-listed-map", {})
        record = self.cache.get(request, max_age=self.ticker_map_max_age, now=self._checked_now())
        if record is not None and record.fresh:
            return _object_payload(record.payload), record.fetched_at, record.content_hash, "cache"
        payload = await self._provider_call(self.sec.company_tickers)
        fetched_at = self._checked_now()
        stored = self.cache.put(request, _object_payload(payload), fetched_at=fetched_at)
        return _object_payload(stored.payload), stored.fetched_at, stored.content_hash, "network"

    async def _company_facts(self, issuer: ResolvedIssuer) -> tuple[dict[str, Any], datetime, str]:
        request = ProviderRequest("sec", "company_facts", issuer.cik, {})
        record = self.cache.get(request, max_age=self.cache_max_age, now=self._checked_now())
        if record is not None and record.fresh:
            return _object_payload(record.payload), record.fetched_at, "cache"
        payload = await self._provider_call(lambda: self.sec.company_facts(issuer.cik))
        fetched_at = self._checked_now()
        stored = self.cache.put(request, _object_payload(payload), fetched_at=fetched_at)
        return _object_payload(stored.payload), stored.fetched_at, "network"

    async def _market_snapshot(self, issuer: ResolvedIssuer) -> tuple[MarketSnapshot, datetime, str]:
        request = ProviderRequest("yfinance", "snapshot", issuer.ticker, {})
        record = self.cache.get(request, max_age=self.cache_max_age, now=self._checked_now())
        if record is not None and record.fresh:
            return _snapshot_from_payload(record.payload), record.fetched_at, "cache"
        snapshot = await self._provider_call(lambda: self.market.snapshot(issuer.ticker))
        fetched_at = self._checked_now()
        payload: dict[str, JsonValue] = {
            "ticker": snapshot.ticker,
            "currency": snapshot.currency,
            "last_price": snapshot.last_price,
            "market_cap": snapshot.market_cap,
            "exchange": snapshot.exchange,
        }
        stored = self.cache.put(request, payload, fetched_at=fetched_at)
        return _snapshot_from_payload(stored.payload), stored.fetched_at, "network"

    async def _provider_call(self, call: Callable[[], Awaitable[Any]]) -> Any:
        async with self._semaphore:
            self._active_provider_requests += 1
            self._peak_provider_requests = max(self._peak_provider_requests, self._active_provider_requests)
            try:
                return await asyncio.wait_for(call(), timeout=self.provider_timeout_seconds)
            finally:
                self._active_provider_requests -= 1

    def _failure(self, ticker: str, provider: str, phase: str, error: str) -> CollectionFailure:
        retryable = "TimeoutError" in error or "timed out" in error.lower()
        return CollectionFailure(ticker, provider, phase, error[:500], retryable, self._checked_now())

    def _checked_now(self) -> datetime:
        value = self._now()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("live collector clock must return a timezone-aware datetime")
        return value.astimezone(UTC)


def _parse_company_tickers(payload: Mapping[str, Any]) -> dict[str, ResolvedIssuer]:
    resolved: dict[str, ResolvedIssuer] = {}
    for key in sorted(payload, key=str):
        raw = payload[key]
        if not isinstance(raw, Mapping):
            continue
        ticker = str(raw.get("ticker", "")).strip().upper()
        company = str(raw.get("title", "")).strip()
        try:
            cik = normalize_cik(raw.get("cik_str", ""))
        except ValueError:
            continue
        if not _TICKER.fullmatch(ticker) or not company:
            continue
        issuer = ResolvedIssuer(ticker, cik, company)
        existing = resolved.get(ticker)
        if existing is not None and existing != issuer:
            raise ValueError(f"official SEC map contains conflicting entries for {ticker}")
        resolved[ticker] = issuer
    if not resolved:
        raise ValueError("official SEC ticker map did not contain any valid entries")
    return resolved


def _canonicalize_sec_batch(batch: ObservationBatch, issuer: ResolvedIssuer) -> ObservationBatch:
    observations = []
    for observation in batch.observations:
        metadata = dict(observation.metadata or {})
        metadata.update(
            {
                "resolved_cik": issuer.cik,
                "identity_source": "sec_company_tickers_json",
            }
        )
        observations.append(
            replace(
                observation,
                observation_id=observation_id_for(
                    DataProvider.SEC,
                    entity_id=issuer.entity_id,
                    name=observation.name,
                    period=observation.period,
                    evidence_id=observation.evidence_ids[0],
                ),
                entity_id=issuer.entity_id,
                ticker=issuer.ticker,
                metadata=metadata,
            )
        )
    return ObservationBatch(tuple(observations), batch.evidence, batch.issues)


def _select_analysis_observations(
    ledger: ObservationLedger,
    issuer: ResolvedIssuer,
) -> dict[str, FinancialObservation]:
    annual_revenue = [
        observation
        for observation in ledger.select(entity_id=issuer.entity_id, name="revenue")
        if observation.period.kind is PeriodKind.DURATION
        and observation.period.fiscal_period == "FY"
        and observation.unit.kind is UnitKind.CURRENCY
    ]
    groups: dict[tuple[str, str], list[FinancialObservation]] = {}
    for observation in annual_revenue:
        groups.setdefault((observation.unit.symbol, observation.unit.scale), []).append(observation)
    comparable = sorted(
        groups.values(),
        key=lambda values: (max(item.period.end for item in values), len(values)),
        reverse=True,
    )
    if not comparable:
        return {}
    revenues = sorted(comparable[0], key=lambda item: (item.period.end, item.observation_id), reverse=True)
    current = revenues[0]
    selected: dict[str, FinancialObservation] = {"revenue": current}
    prior = next((item for item in revenues[1:] if item.period.end != current.period.end), None)
    if prior is not None:
        selected["prior_revenue"] = prior
    for name in ("net_income", "operating_cash_flow", "capital_expenditures"):
        matches = [
            item
            for item in ledger.select(entity_id=issuer.entity_id, name=name)
            if item.period == current.period and item.unit == current.unit
        ]
        if matches:
            selected[name] = matches[-1]
    for name in ("market_cap", "share_price"):
        matches = [
            item
            for item in ledger.select(entity_id=issuer.entity_id, name=name)
            if item.unit.symbol == current.unit.symbol and item.unit.scale == current.unit.scale
        ]
        if matches:
            selected[name] = matches[-1]
    return selected


def _identity_evidence(issuer: ResolvedIssuer, retrieved_at: datetime, mapping_hash: str) -> EvidenceRecord:
    locator: dict[str, JsonValue] = {
        "ticker": issuer.ticker,
        "cik": issuer.cik,
        "company": issuer.company,
        "mapping_content_hash": mapping_hash,
    }
    return EvidenceRecord(
        evidence_id=evidence_id_for(DataProvider.SEC, "company_ticker_identity", locator),
        provider=DataProvider.SEC,
        source_kind="company_ticker_identity",
        source_locator=locator,
        source_url="https://www.sec.gov/files/company_tickers.json",
        observed_at=retrieved_at,
        retrieved_at=retrieved_at,
        content_hash=canonical_content_hash(locator),
    )


def _snapshot_from_payload(payload: JsonValue) -> MarketSnapshot:
    data = _object_payload(payload)
    return MarketSnapshot(
        ticker=str(data["ticker"]),
        currency=None if data.get("currency") is None else str(data["currency"]),
        last_price=None if data.get("last_price") is None else float(data["last_price"]),
        market_cap=None if data.get("market_cap") is None else float(data["market_cap"]),
        exchange=None if data.get("exchange") is None else str(data["exchange"]),
    )


def _object_payload(payload: object) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("provider payload must be a JSON object")
    return dict(payload)


def _safe_error(error: Exception) -> str:
    message = str(error).strip()
    return f"{type(error).__name__}: {message}" if message else type(error).__name__
