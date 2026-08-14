from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from alpha_orchestration.data.cache import (
    CacheIntegrityError,
    ContentAddressedJsonCache,
    ProviderRequest,
)
from alpha_orchestration.data.live import normalize_live_tickers

NOW = datetime(2026, 8, 13, 12, tzinfo=UTC)


def test_live_ticker_normalization_is_bounded_and_stable() -> None:
    assert normalize_live_tickers((" aapl ", "MSFT", "AAPL", "BRK.B")) == (
        "AAPL",
        "MSFT",
        "BRK.B",
    )

    with pytest.raises(ValueError, match="between 1 and 8"):
        normalize_live_tickers(())
    with pytest.raises(ValueError, match="invalid ticker"):
        normalize_live_tickers(("AAPL/../../secret",))
    with pytest.raises(ValueError, match="between 1 and 8"):
        normalize_live_tickers(tuple(f"T{index}" for index in range(9)))


def test_content_addressed_cache_reports_freshness(tmp_path) -> None:
    cache = ContentAddressedJsonCache(tmp_path / "cache")
    request = ProviderRequest("sec", "company_facts", "0000320193", {})
    stored = cache.put(request, {"entityName": "Apple Inc."}, fetched_at=NOW)

    assert stored.fresh is True
    assert cache.get(request, max_age=timedelta(hours=1), now=NOW) == stored

    stale = cache.get(request, max_age=timedelta(hours=1), now=NOW + timedelta(hours=2))
    assert stale is not None
    assert stale.fresh is False


def test_content_addressed_cache_fails_closed_on_blob_tampering(tmp_path) -> None:
    cache = ContentAddressedJsonCache(tmp_path / "cache")
    request = ProviderRequest("sec", "company_facts", "0000320193", {})
    stored = cache.put(request, {"entityName": "Apple Inc."}, fetched_at=NOW)
    blob = cache.root / "blobs" / f"{stored.content_hash}.json"
    blob.write_text(json.dumps({"entityName": "Tampered Corp."}), encoding="utf-8")

    with pytest.raises(CacheIntegrityError, match="hash mismatch"):
        cache.get(request, max_age=timedelta(days=1), now=NOW)


def test_provider_request_identity_excludes_credentials() -> None:
    request = ProviderRequest(
        "yfinance",
        "snapshot",
        "AAPL",
        {"fields": ["last_price", "market_cap"]},
    )

    assert request.to_dict() == {
        "provider": "yfinance",
        "operation": "snapshot",
        "identity": "AAPL",
        "parameters": {"fields": ["last_price", "market_cap"]},
    }
    assert len(request.request_hash) == 64
