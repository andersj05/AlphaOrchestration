from __future__ import annotations

import asyncio

import pytest

import alpha_orchestration.automatic_harness as automatic_harness


def test_automatic_harness_executes_and_replays_300_issuers(tmp_path, monkeypatch) -> None:
    run = asyncio.run(
        automatic_harness.execute_fixture(
            tmp_path / "events.jsonl",
            cache_root=tmp_path / "cache",
        )
    )
    summary = automatic_harness.validate_primary_run(run)

    assert summary["ok"] is True
    assert summary["status"] == "complete"
    assert (summary["selected"], summary["eligible"], summary["screened"]) == (300, 300, 300)
    assert summary["universe_rows"] == 300
    assert summary["registered_lanes"] == 8
    assert summary["configured_active_slots"] == 8
    assert summary["observed_peak_analysis_tasks"] == 8
    assert summary["observed_peak_provider_requests"] == 8
    assert summary["observed_peak_journal_screen_tasks"] == 8
    assert summary["market_snapshot_calls"] == 0
    assert summary["candidates"] == 25
    assert summary["source_currency_binding_verified"] is True
    assert summary["terminal_order_verified"] is True
    assert summary["replay_equivalent"] is True
    assert len(automatic_harness.EXPECTED_PRIMARY_ARTIFACT_HASH) == 64
    assert summary["artifact_hash"] == automatic_harness.EXPECTED_PRIMARY_ARTIFACT_HASH

    monkeypatch.setattr(
        automatic_harness,
        "EXPECTED_PRIMARY_ARTIFACT_HASH",
        "0" * 64,
    )
    with pytest.raises(RuntimeError, match="automatic results artifact changed"):
        automatic_harness.validate_primary_run(run)
