from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from alpha_orchestration.data.cache import ContentAddressedJsonCache
from alpha_orchestration.data.observations import UnitKind
from alpha_orchestration.data.universe import (
    AutomaticUniverseDiscovery,
    UniverseDiscoveryError,
)
from alpha_orchestration.data.universe_mapping import manifest_market_batch

NOW = datetime(2026, 8, 13, 16, tzinfo=UTC)


def _ticker(index: int) -> str:
    return f"A{index:03d}"


def _sec_payload(count: int) -> dict:
    return {
        "fields": ["cik", "name", "ticker", "exchange"],
        "data": [[index + 1, f"Adversarial Company {index:03d}", _ticker(index), "Nasdaq"] for index in range(count)],
    }


def _quotes(count: int) -> list[dict]:
    return [
        {
            "symbol": _ticker(index),
            "quoteType": "EQUITY",
            "exchange": "NMS",
            "marketCap": 1_000_000_000_000 - index * 1_000_000,
            "regularMarketPrice": 50.0 + index,
            "averageDailyVolume3Month": 500_000.0 + index,
            "currency": "USD",
        }
        for index in range(count)
    ]


class FakeSec:
    def __init__(self, count: int) -> None:
        self.payload = _sec_payload(count)

    async def company_tickers_exchange(self) -> dict:
        return self.payload


class FakeMarket:
    def __init__(self, quotes: list[dict], *, conflicting_total: bool = False) -> None:
        self.quotes = quotes
        self.calls = 0
        self.conflicting_total = conflicting_total

    async def screen_equities(self, request) -> dict:
        self.calls += 1
        total = len(self.quotes) - (1 if self.conflicting_total and self.calls > 1 else 0)
        return {
            "total": total,
            "quotes": self.quotes[request.offset : request.offset + request.size],
        }


def _discover(tmp_path, quotes: list[dict], *, conflicting_total: bool = False):
    return asyncio.run(
        AutomaticUniverseDiscovery(
            FakeSec(len(quotes)),
            FakeMarket(quotes, conflicting_total=conflicting_total),
            ContentAddressedJsonCache(tmp_path / "cache"),
            now=lambda: NOW,
        ).discover()
    )


def test_missing_quote_type_and_currency_fail_closed_without_corrupting_raw_coverage(tmp_path) -> None:
    quotes = _quotes(305)
    quotes[0].pop("quoteType")
    quotes[1].pop("currency")

    manifest = _discover(tmp_path, quotes)
    snapshot = manifest.snapshot(include_members=False)

    assert snapshot["fetched_row_count"] == 305
    assert snapshot["screened_unique_count"] == 303
    assert snapshot["provider_rows_not_inspected"] == 0
    assert snapshot["exclusion_counts"]["non_equity"] == 1
    assert snapshot["exclusion_counts"]["incomplete_screen_metrics"] == 1


def test_conflicting_provider_totals_across_pages_fail_closed(tmp_path) -> None:
    with pytest.raises(UniverseDiscoveryError, match="conflicting totals"):
        _discover(tmp_path, _quotes(400), conflicting_total=True)


def test_manifest_identity_binds_exclusions_and_source_request_hashes(tmp_path) -> None:
    manifest = _discover(tmp_path, _quotes(305))
    changed_exclusion = replace(
        manifest,
        exclusions=(replace(manifest.exclusions[0], detail="tampered exclusion"), *manifest.exclusions[1:]),
    )

    assert changed_exclusion.content_hash != manifest.content_hash
    with pytest.raises(ValueError, match="market lineage"):
        replace(
            manifest,
            market_sources=(
                replace(manifest.market_sources[0], request_hash="a" * 64),
                *manifest.market_sources[1:],
            ),
        )


def test_manifest_market_batch_preserves_currency_page_lineage_and_time_basis(tmp_path) -> None:
    member = _discover(tmp_path, _quotes(305)).members[0]

    batch = manifest_market_batch(member)

    assert {item.name for item in batch.observations} == {
        "average_daily_volume_3m",
        "market_cap",
        "share_price",
    }
    market_cap = next(item for item in batch.observations if item.name == "market_cap")
    share_price = next(item for item in batch.observations if item.name == "share_price")
    assert (market_cap.unit.kind, market_cap.unit.symbol) == (UnitKind.CURRENCY, "USD")
    assert (share_price.unit.kind, share_price.unit.symbol) == (UnitKind.CURRENCY_PER_SHARE, "USD")
    assert all(item.metadata["observed_at_basis"] == "provider_page_retrieval_time" for item in batch.observations)
    assert all(item.period.end == NOW.date() for item in batch.observations)
    assert all(evidence.content_hash == member.market_content_hash for evidence in batch.evidence)
    assert all(evidence.source_locator["request_hash"] == member.market_request_hash for evidence in batch.evidence)


def test_universe_member_rejects_missing_currency_or_invalid_provenance(tmp_path) -> None:
    member = _discover(tmp_path, _quotes(305)).members[0]

    with pytest.raises(ValueError, match="currency"):
        replace(member, currency="")
    with pytest.raises(ValueError, match="market_request_hash"):
        replace(member, market_request_hash="not-a-digest")
