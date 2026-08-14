"""Deterministic providers and SEC-shaped inputs for the scale harness."""

from __future__ import annotations

import asyncio
import math
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from alpha_orchestration.automatic_projection import LOGICAL_AGENT_LANES
from alpha_orchestration.automatic_runtime import AutomaticLiveRuntime
from alpha_orchestration.controller import RunController
from alpha_orchestration.data.cache import ContentAddressedJsonCache
from alpha_orchestration.data.live import LiveDataCollector, LiveIssuerEvidence
from alpha_orchestration.data.observations import canonical_content_hash
from alpha_orchestration.data.universe import (
    SEC_EXCHANGE_MAP_URL,
    YFINANCE_SCREENER_URL,
    UniverseManifest,
    UniverseMember,
    UniversePolicy,
    UniverseSource,
)
from alpha_orchestration.data.yfinance import MarketSnapshot
from alpha_orchestration.domain import RunEvent, RunSpec, RunState
from alpha_orchestration.journal import JsonlJournal, load_events, replay
from alpha_orchestration.live_runtime import IssuerAnalysis, _analyze_one

FIXTURE_NOW = datetime(2026, 8, 13, 12, tzinfo=UTC)
PRIMARY_UNIVERSE_SIZE = 300
PRIMARY_CANDIDATE_LIMIT = 25


@dataclass(slots=True)
class FixtureDiscovery:
    manifest: UniverseManifest
    calls: int = 0

    async def discover(self, policy: UniversePolicy | None = None) -> UniverseManifest:
        self.calls += 1
        if policy != self.manifest.policy:
            raise AssertionError("runtime altered the fixture universe policy")
        await asyncio.sleep(0)
        return self.manifest


class ForbiddenMarketProvider:
    """Any call proves manifest market data was not reused."""

    def __init__(self) -> None:
        self.snapshot_calls: list[str] = []
        self.screen_calls = 0

    async def snapshot(self, ticker: str) -> MarketSnapshot:
        self.snapshot_calls.append(ticker)
        raise AssertionError("automatic collection must not fetch per-ticker quotes")

    async def screen_equities(self, request: object) -> dict[str, Any]:
        del request
        self.screen_calls += 1
        raise AssertionError("fixture discovery must not call a market screener")


class BarrierSecProvider:
    """Serve facts and hold the first wave until eight requests overlap."""

    def __init__(
        self,
        manifest: UniverseManifest,
        *,
        fail_tickers: Sequence[str] = (),
        barrier_size: int = LOGICAL_AGENT_LANES,
    ) -> None:
        if not 1 <= barrier_size <= LOGICAL_AGENT_LANES:
            raise ValueError("barrier_size must be between 1 and 8")
        self._ticker = {member.cik: member.ticker for member in manifest.members}
        self._company = {member.cik: member.company for member in manifest.members}
        self._rank = {member.cik: member.rank for member in manifest.members}
        self._fail = frozenset(fail_tickers)
        if self._fail - set(manifest.tickers):
            raise ValueError("failure tickers must belong to the fixture manifest")
        self._barrier_size = barrier_size
        self._release = asyncio.Event()
        self._lock = asyncio.Lock()
        self._active = 0
        self.peak_active_calls = 0
        self.fact_calls: list[str] = []
        self.mapping_calls = 0

    async def company_tickers(self) -> dict[str, Any]:
        self.mapping_calls += 1
        raise AssertionError("manifest collection must not reload the ticker map")

    async def company_facts(self, cik: str | int) -> dict[str, Any]:
        normalized = str(cik).removeprefix("CIK").zfill(10)
        async with self._lock:
            if normalized not in self._ticker:
                raise AssertionError(f"unexpected fixture CIK: {normalized}")
            self.fact_calls.append(normalized)
            position = len(self.fact_calls)
            self._active += 1
            self.peak_active_calls = max(self.peak_active_calls, self._active)
            if position == self._barrier_size:
                self._release.set()
        try:
            if position <= self._barrier_size:
                await asyncio.wait_for(self._release.wait(), timeout=5)
            await asyncio.sleep(0)
            ticker = self._ticker[normalized]
            if ticker in self._fail:
                raise RuntimeError("isolated fixture SEC failure")
            return _company_facts(
                int(normalized),
                ticker,
                self._company[normalized],
                self._rank[normalized],
            )
        finally:
            async with self._lock:
                self._active -= 1


class BarrierAnalyzer:
    """Run trusted arithmetic behind a real eight-thread barrier."""

    def __init__(self, *, fail_tickers: Sequence[str] = ()) -> None:
        self._barrier = threading.Barrier(LOGICAL_AGENT_LANES)
        self._fail = frozenset(fail_tickers)
        self._lock = threading.Lock()
        self._active = 0
        self.calls = 0
        self.peak_active_calls = 0
        self.results: dict[str, IssuerAnalysis] = {}

    def __call__(self, evidence: LiveIssuerEvidence) -> IssuerAnalysis:
        with self._lock:
            position = self.calls
            self.calls += 1
            self._active += 1
            self.peak_active_calls = max(self.peak_active_calls, self._active)
        try:
            if position < LOGICAL_AGENT_LANES:
                try:
                    self._barrier.wait(timeout=5)
                except threading.BrokenBarrierError as exc:
                    raise RuntimeError("eight-way analysis barrier was not reached") from exc
            ticker = evidence.issuer.ticker
            if ticker in self._fail:
                raise RuntimeError("isolated fixture analysis failure")
            result = _analyze_one(evidence)
            with self._lock:
                self.results[ticker] = result
            return result
        finally:
            with self._lock:
                self._active -= 1


@dataclass(frozen=True, slots=True)
class AutomaticHarnessRun:
    manifest: UniverseManifest
    state: RunState
    restored: RunState
    events: tuple[RunEvent, ...]
    discovery: FixtureDiscovery
    sec: BarrierSecProvider
    market: ForbiddenMarketProvider
    analyzer: BarrierAnalyzer


def build_fixture_policy(target_size: int = PRIMARY_UNIVERSE_SIZE) -> UniversePolicy:
    if not 100 <= target_size <= 500:
        raise ValueError("fixture target_size must be between 100 and 500")
    return UniversePolicy(
        profile_id=f"OFFLINE_SCALE_{target_size}_V1",
        target_size=target_size,
        minimum_size=min(200, target_size),
        page_size=250,
        max_screened=max(250, target_size),
        minimum_market_cap=300_000_000,
        minimum_share_price=1,
        minimum_average_daily_volume_3m=200_000,
    )


def build_fixture_manifest(
    policy: UniversePolicy | None = None,
    *,
    currency_overrides: Mapping[int, str] | None = None,
) -> UniverseManifest:
    policy = policy or build_fixture_policy()
    overrides = dict(currency_overrides or {})
    if set(overrides) - set(range(1, policy.target_size + 1)):
        raise ValueError("currency override rank is outside the universe")
    sec_hash = canonical_content_hash(
        {"fixture": "automatic-scale-v1", "source": "sec", "rows": policy.target_size}
    )
    sec_source = UniverseSource(
        "sec",
        "company_ticker_exchange_identity",
        SEC_EXCHANGE_MAP_URL,
        FIXTURE_NOW,
        sec_hash,
        canonical_content_hash({"fixture": "automatic-scale-v1", "request": "sec"}),
        "cache",
        policy.target_size,
    )
    market_sources: list[UniverseSource] = []
    bindings: list[tuple[str, str]] = []
    for offset in range(0, policy.target_size, policy.page_size):
        count = min(policy.page_size, policy.target_size - offset)
        request_hash = canonical_content_hash(
            {"fixture": "automatic-scale-v1", "request": "market", "offset": offset}
        )
        content_hash = canonical_content_hash(
            {"fixture": "automatic-scale-v1", "source": "market", "offset": offset, "rows": count}
        )
        market_sources.append(
            UniverseSource(
                "yfinance",
                "equity_screen_page",
                YFINANCE_SCREENER_URL,
                FIXTURE_NOW,
                content_hash,
                request_hash,
                "cache",
                count,
            )
        )
        bindings.extend((content_hash, request_hash) for _ in range(count))
    members = tuple(
        UniverseMember(
            rank=rank,
            ticker=_ticker(rank),
            cik=str(rank).zfill(10),
            company=f"Offline Scale Company {rank:03d}",
            sec_exchange="Nasdaq" if rank % 2 else "NYSE",
            market_exchange_code="NMS" if rank % 2 else "NYQ",
            market_cap=50_000_000_000.0 - rank * 50_000_000.0,
            share_price=20.0 + rank / 10,
            average_daily_volume_3m=500_000.0 + rank * 1_000.0,
            currency=overrides.get(rank, "USD"),
            sec_content_hash=sec_hash,
            market_content_hash=bindings[rank - 1][0],
            market_request_hash=bindings[rank - 1][1],
            market_retrieved_at=FIXTURE_NOW,
        )
        for rank in range(1, policy.target_size + 1)
    )
    return UniverseManifest(
        policy,
        members,
        (),
        FIXTURE_NOW,
        sec_source,
        tuple(market_sources),
        policy.target_size + 125,
        policy.target_size,
        policy.target_size,
    )


async def execute_fixture(
    journal_path: Path,
    *,
    cache_root: Path,
    target_size: int = PRIMARY_UNIVERSE_SIZE,
    fact_failures: Sequence[str] = (),
    analysis_failures: Sequence[str] = (),
    currency_overrides: Mapping[int, str] | None = None,
) -> AutomaticHarnessRun:
    policy = build_fixture_policy(target_size)
    manifest = build_fixture_manifest(policy, currency_overrides=currency_overrides)
    discovery = FixtureDiscovery(manifest)
    sec = BarrierSecProvider(manifest, fail_tickers=fact_failures)
    market = ForbiddenMarketProvider()
    collector = LiveDataCollector(
        sec,
        market,
        ContentAddressedJsonCache(cache_root),
        provider_slots=LOGICAL_AGENT_LANES,
        provider_timeout_seconds=10,
        now=lambda: FIXTURE_NOW,
    )
    analyzer = BarrierAnalyzer(fail_tickers=analysis_failures)
    runtime = AutomaticLiveRuntime(
        discovery,
        collector,
        policy=policy,
        collection_batch_size=25,
        candidate_limit=min(PRIMARY_CANDIDATE_LIMIT, target_size),
        minimum_screened_ratio=0.70,
        minimum_screened_count=min(100, max(1, target_size - 10)),
        analysis_function=analyzer,
    )
    spec = RunSpec(
        sector="Offline automatic-universe fixture",
        universe_size=target_size,
        agent_budget=LOGICAL_AGENT_LANES,
        active_slots=LOGICAL_AGENT_LANES,
        mode="automatic_live",
        run_id=f"run-automatic-harness-{target_size}",
    )
    journal_path.parent.mkdir(parents=True, exist_ok=True)
    state = await RunController(spec, runtime, JsonlJournal(journal_path)).run()
    return AutomaticHarnessRun(
        manifest,
        state,
        replay(journal_path),
        load_events(journal_path),
        discovery,
        sec,
        market,
        analyzer,
    )


def _ticker(rank: int) -> str:
    return f"U{rank:04d}"


def _fact(
    value: int,
    *,
    start: str,
    end: str,
    fiscal_year: int,
    accession: str,
) -> dict[str, object]:
    return {
        "val": value,
        "start": start,
        "end": end,
        "filed": f"{fiscal_year + 1}-02-01",
        "form": "10-K",
        "accn": accession,
        "fy": fiscal_year,
        "fp": "FY",
        "frame": f"CY{fiscal_year}",
    }


def _company_facts(cik: int, ticker: str, company: str, rank: int) -> dict[str, Any]:
    prior = 100_000_000 + rank * 1_000_000
    current = math.floor(prior * (105 + (rank * 17) % 36) / 100)
    base = _fact(
        current,
        start="2024-01-01",
        end="2024-12-31",
        fiscal_year=2024,
        accession=f"{cik:010d}-25-000001",
    )

    def metric(value: int, suffix: int) -> dict[str, object]:
        return {**base, "val": value, "accn": f"{cik:010d}-25-{suffix:06d}"}

    return {
        "cik": cik,
        "entityName": company,
        "tickers": [ticker],
        "facts": {
            "us-gaap": {
                "RevenueFromContractWithCustomerExcludingAssessedTax": {
                    "units": {
                        "USD": [
                            _fact(
                                prior,
                                start="2023-01-01",
                                end="2023-12-31",
                                fiscal_year=2023,
                                accession=f"{cik:010d}-24-000001",
                            ),
                            base,
                        ]
                    }
                },
                "NetIncomeLoss": {
                    "units": {"USD": [metric(math.floor(current * (7 + rank * 13 % 22) / 100), 2)]}
                },
                "NetCashProvidedByUsedInOperatingActivities": {
                    "units": {"USD": [metric(math.floor(current * (10 + rank * 11 % 24) / 100), 3)]}
                },
                "PaymentsToAcquirePropertyPlantAndEquipment": {
                    "units": {"USD": [metric(-math.floor(current * (2 + rank * 5 % 6) / 100), 4)]}
                },
            }
        },
    }
