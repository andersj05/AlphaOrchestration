from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from alpha_orchestration.data.cache import ContentAddressedJsonCache
from alpha_orchestration.data.universe import (
    AutomaticUniverseDiscovery,
    UniverseCoverageError,
    UniverseDiscoveryError,
    UniversePolicy,
)

NOW = datetime(2026, 8, 13, 16, tzinfo=UTC)


def _ticker(index: int) -> str:
    return f"T{index:03d}"


def _sec_payload(count: int, *, duplicate_share_class: bool = False, missing: tuple[int, ...] = ()) -> dict:
    rows = []
    for index in range(count):
        if index in missing:
            continue
        cik = 1 if duplicate_share_class and index in {0, 1} else index + 1
        rows.append(
            [
                cik,
                f"Company {index:03d}",
                _ticker(index),
                "Nasdaq" if index % 2 == 0 else "NYSE",
            ]
        )
    return {"fields": ["cik", "name", "ticker", "exchange"], "data": rows}


def _quotes(count: int) -> list[dict]:
    return [
        {
            "symbol": _ticker(index),
            "quoteType": "EQUITY",
            "exchange": "NMS" if index % 2 == 0 else "NYQ",
            "marketCap": 5_000_000_000_000 - index * 1_000_000,
            "regularMarketPrice": 100 + index,
            "averageDailyVolume3Month": 1_000_000 + index,
            "currency": "USD",
        }
        for index in range(count)
    ]


class FakeSec:
    def __init__(self, payload: dict) -> None:
        self.payload = payload
        self.calls = 0

    async def company_tickers_exchange(self) -> dict:
        self.calls += 1
        await asyncio.sleep(0)
        return self.payload


class FakeMarket:
    def __init__(self, quotes: list[dict]) -> None:
        self.quotes = quotes
        self.requests = []

    async def screen_equities(self, request) -> dict:
        self.requests.append(request)
        await asyncio.sleep(0)
        page = self.quotes[request.offset : request.offset + request.size]
        return {
            "start": request.offset,
            "count": len(page),
            "total": len(self.quotes),
            "quotes": page,
        }


def _discovery(tmp_path, sec: FakeSec, market: FakeMarket) -> AutomaticUniverseDiscovery:
    return AutomaticUniverseDiscovery(
        sec,
        market,
        ContentAddressedJsonCache(tmp_path / "cache"),
        now=lambda: NOW,
    )


def test_default_universe_policy_is_versioned_broad_and_explicit() -> None:
    policy = UniversePolicy()

    assert policy.profile_id == "US_LARGE_LIQUID_V1"
    assert (policy.target_size, policy.minimum_size, policy.max_screened) == (300, 200, 1_000)
    assert policy.sec_exchanges == ("Nasdaq", "NYSE")
    assert policy.market_exchange_codes == ("NMS", "NGM", "NCM", "NYQ")
    assert policy.to_dict() == {
        "profile_id": "US_LARGE_LIQUID_V1",
        "target_size": 300,
        "minimum_size": 200,
        "page_size": 250,
        "max_screened": 1_000,
        "minimum_market_cap": 300_000_000.0,
        "minimum_share_price": 1.0,
        "minimum_average_daily_volume_3m": 200_000.0,
        "sec_exchanges": ["Nasdaq", "NYSE"],
        "market_exchange_codes": ["NMS", "NGM", "NCM", "NYQ"],
        "ranking": "market_cap_desc_then_ticker_asc",
        "security_type": "equity",
        "region": "us",
    }


def test_discovery_selects_300_deduplicated_sec_issuers_and_reuses_cache(tmp_path) -> None:
    sec = FakeSec(_sec_payload(320, duplicate_share_class=True, missing=(5,)))
    market = FakeMarket(_quotes(320))
    discovery = _discovery(tmp_path, sec, market)

    first = asyncio.run(discovery.discover())

    assert len(first.members) == 300
    assert first.posture == "target_met"
    assert first.tickers[0] == "T000"
    assert first.tickers[-1] == "T301"
    assert "T001" not in first.tickers
    assert "T005" not in first.tickers
    assert len({member.cik for member in first.members}) == 300
    assert [request.offset for request in market.requests] == [0, 250]
    assert first.provider_reported_total == 320
    assert first.screened_unique_count == 320
    assert first.sec_source.access_mode == "network"
    assert [source.access_mode for source in first.market_sources] == ["network", "network"]

    snapshot = first.snapshot()
    assert snapshot["policy"]["profile_id"] == "US_LARGE_LIQUID_V1"
    assert snapshot["selected_count"] == 300
    assert snapshot["provider_rows_not_inspected"] == 0
    assert snapshot["exclusion_counts"] == {
        "below_target_cutoff": 18,
        "duplicate_issuer": 1,
        "missing_sec_identity": 1,
    }
    assert snapshot["research_candidate_status"] == "screen inputs; not investment recommendations"
    assert len(snapshot["members"]) == 300
    assert all(len(member["source_content_hashes"]) == 2 for member in snapshot["members"])

    second = asyncio.run(discovery.discover())

    assert second.tickers == first.tickers
    assert second.content_hash == first.content_hash
    assert sec.calls == 1
    assert len(market.requests) == 2
    assert second.sec_source.access_mode == "cache"
    assert [source.access_mode for source in second.market_sources] == ["cache", "cache"]


def test_discovery_fails_closed_below_the_minimum_cohort(tmp_path) -> None:
    sec = FakeSec(_sec_payload(99))
    market = FakeMarket(_quotes(99))
    policy = UniversePolicy(target_size=200, minimum_size=100, max_screened=200)

    with pytest.raises(UniverseCoverageError) as caught:
        asyncio.run(_discovery(tmp_path, sec, market).discover(policy))

    assert caught.value.manifest.posture == "insufficient"
    assert len(caught.value.manifest.members) == 99
    assert caught.value.manifest.snapshot(include_members=False)["selected_count"] == 99


def test_discovery_reports_provider_rows_that_were_not_inspected(tmp_path) -> None:
    quotes = _quotes(700)
    sec = FakeSec(_sec_payload(700))
    market = FakeMarket(quotes)

    manifest = asyncio.run(_discovery(tmp_path, sec, market).discover())

    assert len(market.requests) == 2
    assert manifest.snapshot(include_members=False)["provider_rows_not_inspected"] == 200


def test_discovery_revalidates_market_rows_instead_of_trusting_the_query(tmp_path) -> None:
    quotes = _quotes(310)
    quotes[0]["quoteType"] = "ETF"
    quotes[1]["exchange"] = "PNK"
    quotes[2].pop("marketCap")
    quotes[3]["averageDailyVolume3Month"] = 1
    sec = FakeSec(_sec_payload(310))
    market = FakeMarket(quotes)

    manifest = asyncio.run(_discovery(tmp_path, sec, market).discover())
    counts = manifest.snapshot(include_members=False)["exclusion_counts"]

    assert len(manifest.members) == 300
    assert counts["non_equity"] == 1
    assert counts["market_exchange_outside_policy"] == 1
    assert counts["incomplete_screen_metrics"] == 1
    assert counts["failed_liquidity_threshold"] == 1


def test_discovery_rejects_conflicting_sec_identity_rows(tmp_path) -> None:
    sec = FakeSec(
        {
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": [
                [1, "Alpha", "ALP", "Nasdaq"],
                [2, "Conflicting Alpha", "ALP", "NYSE"],
            ],
        }
    )
    market = FakeMarket(_quotes(300))

    with pytest.raises(UniverseDiscoveryError, match="conflicting identity rows"):
        asyncio.run(_discovery(tmp_path, sec, market).discover())
