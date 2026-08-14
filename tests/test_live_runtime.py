import asyncio
from datetime import UTC, datetime

from alpha_orchestration.controller import RunController
from alpha_orchestration.data.cache import ContentAddressedJsonCache
from alpha_orchestration.data.live import LiveDataCollector
from alpha_orchestration.data.yfinance import MarketSnapshot
from alpha_orchestration.domain import CandidateSourceMode, EventKind, RunSpec, RunStatus
from alpha_orchestration.journal import JsonlJournal, replay
from alpha_orchestration.live_runtime import LiveRuntime, live_environment_readiness

NOW = datetime(2026, 8, 13, 12, tzinfo=UTC)


def _fact(
    value: int,
    *,
    start: str,
    end: str,
    fiscal_year: int,
    accession: str,
) -> dict:
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


def _company_facts(cik: int, ticker: str, company: str, multiplier: int) -> dict:
    prior = _fact(
        100_000_000 * multiplier,
        start="2023-01-01",
        end="2023-12-31",
        fiscal_year=2023,
        accession=f"{cik:010d}-24-000001",
    )
    current = _fact(
        130_000_000 * multiplier,
        start="2024-01-01",
        end="2024-12-31",
        fiscal_year=2024,
        accession=f"{cik:010d}-25-000001",
    )
    return {
        "cik": cik,
        "entityName": company,
        "tickers": [ticker],
        "facts": {
            "us-gaap": {
                "RevenueFromContractWithCustomerExcludingAssessedTax": {"units": {"USD": [prior, current]}},
                "NetIncomeLoss": {
                    "units": {
                        "USD": [
                            {
                                **current,
                                "val": 19_500_000 * multiplier,
                                "accn": f"{cik:010d}-25-000002",
                            }
                        ]
                    }
                },
                "NetCashProvidedByUsedInOperatingActivities": {
                    "units": {
                        "USD": [
                            {
                                **current,
                                "val": 24_000_000 * multiplier,
                                "accn": f"{cik:010d}-25-000003",
                            }
                        ]
                    }
                },
                "PaymentsToAcquirePropertyPlantAndEquipment": {
                    "units": {
                        "USD": [
                            {
                                **current,
                                "val": -4_000_000 * multiplier,
                                "accn": f"{cik:010d}-25-000004",
                            }
                        ]
                    }
                },
            }
        },
    }


class FakeSec:
    def __init__(self) -> None:
        self.mapping_calls = 0
        self.fact_calls: list[str] = []

    async def company_tickers(self) -> dict:
        self.mapping_calls += 1
        await asyncio.sleep(0)
        return {
            "0": {"ticker": "ALP", "title": "Alpha Logic", "cik_str": 1},
            "1": {"ticker": "BET", "title": "Beta Systems", "cik_str": 2},
        }

    async def company_facts(self, cik: str | int) -> dict:
        normalized = str(cik).zfill(10)
        self.fact_calls.append(normalized)
        await asyncio.sleep(0.01)
        if normalized == "0000000001":
            return _company_facts(1, "ALP", "Alpha Logic", 1)
        if normalized == "0000000002":
            return _company_facts(2, "BET", "Beta Systems", 2)
        raise AssertionError(f"unexpected CIK {cik}")


class FakeMarket:
    def __init__(self, *, fail: tuple[str, ...] = ()) -> None:
        self.fail = set(fail)
        self.calls: list[str] = []

    async def snapshot(self, ticker: str) -> MarketSnapshot:
        self.calls.append(ticker)
        await asyncio.sleep(0.01)
        if ticker in self.fail:
            raise TimeoutError("bounded fake market timeout")
        scale = 1 if ticker == "ALP" else 2
        return MarketSnapshot(
            ticker=ticker,
            currency="USD",
            last_price=25.0 * scale,
            market_cap=650_000_000.0 * scale,
            exchange="NMS",
        )


def _runtime(tmp_path, *, market_fail: tuple[str, ...] = ()) -> LiveRuntime:
    collector = LiveDataCollector(
        FakeSec(),
        FakeMarket(fail=market_fail),
        ContentAddressedJsonCache(tmp_path / "cache"),
        provider_slots=4,
        now=lambda: NOW,
    )
    return LiveRuntime(("ALP", "BET"), collector)


def test_live_runtime_collects_ranks_projects_and_replays(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    spec = RunSpec(
        sector="Operator watchlist",
        universe_size=2,
        agent_budget=2,
        active_slots=2,
        mode="live",
        run_id="run-live-test",
    )
    state = asyncio.run(RunController(spec, _runtime(tmp_path), JsonlJournal(path)).run())

    assert state.status is RunStatus.COMPLETE
    assert len(state.candidates) == 2
    assert all(candidate.source_mode is CandidateSourceMode.LIVE for candidate in state.candidates.values())
    assert all(candidate.financials for candidate in state.candidates.values())
    assert all(candidate.evidence_ids for candidate in state.candidates.values())
    assert replay(path) == state

    completed = next(event for event in state.recent_events if event.kind is EventKind.WORKFLOW_COMPLETED)
    assert completed.payload["analysis_mode"] == "RULE-BASED"
    assert completed.payload["observed_peak_active_tasks"] == 2
    snapshot = completed.payload["live_collection"]
    assert snapshot["requested_count"] == 2
    assert snapshot["ready_count"] == 2
    assert snapshot["partial"] is False
    assert snapshot["provider_failures"] == {"sec": 0, "yfinance": 0}


def test_live_runtime_preserves_partial_market_failure(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    spec = RunSpec(
        sector="Operator watchlist",
        universe_size=2,
        agent_budget=2,
        active_slots=2,
        mode="live",
        run_id="run-live-partial",
    )
    state = asyncio.run(
        RunController(
            spec,
            _runtime(tmp_path, market_fail=("BET",)),
            JsonlJournal(path),
        ).run()
    )

    assert state.status is RunStatus.COMPLETE
    beta = state.candidates["ticker:BET"]
    assert beta.evidence_gaps
    assert any("yfinance" in gap for gap in beta.evidence_gaps)
    completed = next(event for event in state.recent_events if event.kind is EventKind.WORKFLOW_COMPLETED)
    snapshot = completed.payload["live_collection"]
    assert snapshot["partial"] is True
    assert snapshot["provider_failures"] == {"sec": 0, "yfinance": 1}
    assert replay(path) == state


def test_live_readiness_reads_only_named_identity_from_env_file(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("ALPHA_SEC_USER_AGENT", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text(
        "UNRELATED_SECRET=do-not-read\nALPHA_SEC_USER_AGENT='Alpha research test contact@example.com'\n",
        encoding="utf-8",
    )

    readiness = live_environment_readiness(env_file=env_file)

    assert readiness["sec_identity_configured"] is True
    assert set(readiness) == {"sec_identity_configured", "yfinance_installed"}


def test_live_collector_bounds_provider_timeout_and_marks_it_retryable(tmp_path) -> None:
    class HangingMarket:
        def __init__(self) -> None:
            self.cancelled = False

        async def snapshot(self, ticker: str) -> MarketSnapshot:
            del ticker
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise
            raise AssertionError("unreachable")

    market = HangingMarket()
    collector = LiveDataCollector(
        FakeSec(),
        market,
        ContentAddressedJsonCache(tmp_path / "cache"),
        provider_slots=2,
        provider_timeout_seconds=0.05,
        now=lambda: NOW,
    )

    collection = asyncio.run(collector.collect(("ALP",)))

    failure = next(item for item in collection.failures if item.provider == "yfinance")
    assert (failure.phase, failure.error, failure.retryable) == ("snapshot", "TimeoutError", True)
