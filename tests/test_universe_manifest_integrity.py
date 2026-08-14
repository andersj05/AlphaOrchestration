from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from alpha_orchestration.data.universe import (
    UniverseManifest,
    UniverseMember,
    UniversePolicy,
    UniverseSource,
)

NOW = datetime(2026, 8, 13, 16, tzinfo=UTC)


def _manifest() -> UniverseManifest:
    sec_source = UniverseSource(
        provider="sec",
        source_kind="company_ticker_exchange_identity",
        source_url="https://www.sec.gov/files/company_tickers_exchange.json",
        retrieved_at=NOW,
        content_hash="a" * 64,
        request_hash="b" * 64,
        access_mode="network",
        row_count=1,
    )
    market_source = UniverseSource(
        provider="yfinance",
        source_kind="equity_screen_page",
        source_url="https://query2.finance.yahoo.com/v1/finance/screener",
        retrieved_at=NOW,
        content_hash="c" * 64,
        request_hash="d" * 64,
        access_mode="network",
        row_count=1,
    )
    member = UniverseMember(
        rank=1,
        ticker="ALP",
        cik="0000000001",
        company="Alpha Logic",
        sec_exchange="Nasdaq",
        market_exchange_code="NMS",
        market_cap=1_000_000_000.0,
        share_price=50.0,
        average_daily_volume_3m=500_000.0,
        currency="USD",
        sec_content_hash=sec_source.content_hash,
        market_content_hash=market_source.content_hash,
        market_request_hash=market_source.request_hash,
        market_retrieved_at=market_source.retrieved_at,
    )
    return UniverseManifest(
        policy=UniversePolicy(target_size=100, minimum_size=100, max_screened=100),
        members=(member,),
        exclusions=(),
        discovered_at=NOW,
        sec_source=sec_source,
        market_sources=(market_source,),
        provider_reported_total=1,
        screened_unique_count=1,
        fetched_row_count=1,
    )


def test_manifest_rejects_member_lineage_not_present_in_its_sources() -> None:
    manifest = _manifest()
    member = manifest.members[0]

    with pytest.raises(ValueError, match="SEC lineage"):
        replace(
            manifest,
            members=(replace(member, sec_content_hash="e" * 64),),
        )
    with pytest.raises(ValueError, match="market lineage"):
        replace(
            manifest,
            members=(replace(member, market_request_hash="e" * 64),),
        )


def test_manifest_rejects_irreconcilable_coverage_counts() -> None:
    manifest = _manifest()

    with pytest.raises(ValueError, match="selected <= valid unique <= fetched <= provider total"):
        replace(manifest, fetched_row_count=0)
    with pytest.raises(ValueError, match="retained market source rows"):
        replace(
            manifest,
            market_sources=(replace(manifest.market_sources[0], row_count=0),),
        )
