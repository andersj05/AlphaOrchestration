import json
from datetime import UTC, date, datetime

import pytest

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


def _batch() -> ObservationBatch:
    period = FinancialPeriod(
        PeriodKind.DURATION,
        start=date(2024, 1, 1),
        end=date(2024, 12, 31),
        fiscal_year=2024,
        fiscal_period="fy",
    )
    locator = {"accession": "0000000000-25-000001", "concept": "Revenue", "value": 125}
    content_hash = canonical_content_hash(locator)
    evidence_id = evidence_id_for(DataProvider.SEC, "company_fact", locator)
    evidence = EvidenceRecord(
        evidence_id=evidence_id,
        provider=DataProvider.SEC,
        source_kind="company_fact",
        source_locator=locator,
        source_url="https://www.sec.gov/Archives/edgar/data/1/example/",
        observed_at=datetime(2025, 2, 1, tzinfo=UTC),
        retrieved_at=datetime(2025, 2, 2, tzinfo=UTC),
        content_hash=content_hash,
    )
    observation = FinancialObservation(
        observation_id=observation_id_for(
            DataProvider.SEC,
            entity_id="cik:0000000001",
            name="revenue",
            period=period,
            evidence_id=evidence_id,
        ),
        entity_id="cik:0000000001",
        ticker="abc",
        name="revenue",
        value=125,
        unit=FinancialUnit(UnitKind.CURRENCY, "usd"),
        period=period,
        evidence_ids=(evidence_id,),
        metadata={"form": "10-K"},
    )
    return ObservationBatch(observations=(observation,), evidence=(evidence,))


def test_observation_batch_round_trips_as_strict_json() -> None:
    batch = _batch()

    encoded = json.dumps(batch.to_dict(), allow_nan=False, sort_keys=True)
    restored = ObservationBatch.from_dict(json.loads(encoded))

    assert restored == batch
    assert restored.observations[0].ticker == "ABC"
    assert restored.observations[0].unit.symbol == "USD"
    assert restored.observations[0].period.fiscal_period == "FY"


def test_stable_ids_change_when_provider_locator_changes() -> None:
    first = {"ticker": "ABC", "field": "close", "date": "2025-01-01"}
    second = {**first, "date": "2025-01-02"}

    assert evidence_id_for(DataProvider.YFINANCE, "price_history", first) == evidence_id_for(
        DataProvider.YFINANCE, "price_history", dict(reversed(list(first.items())))
    )
    assert evidence_id_for(DataProvider.YFINANCE, "price_history", first) != evidence_id_for(
        DataProvider.YFINANCE, "price_history", second
    )


def test_period_and_numeric_invariants_fail_closed() -> None:
    with pytest.raises(ValueError, match="duration periods require"):
        FinancialPeriod(PeriodKind.DURATION, end=date(2025, 1, 1))
    with pytest.raises(ValueError, match="finite"):
        FinancialObservation(
            observation_id="obs:bad",
            entity_id="ticker:BAD",
            name="share_price",
            value=float("nan"),
            unit=FinancialUnit(UnitKind.CURRENCY_PER_SHARE, "USD"),
            period=FinancialPeriod(PeriodKind.INSTANT, end=date(2025, 1, 1)),
            evidence_ids=("source:bad",),
        )


def test_batch_rejects_unresolved_evidence_ids() -> None:
    observation = _batch().observations[0]

    with pytest.raises(ValueError, match="absent from the batch"):
        ObservationBatch(observations=(observation,))


def test_provider_issue_noise_is_bounded_and_reports_truncation() -> None:
    issues = [NormalizationIssue("bad_value", f"facts[{index}]", "bad") for index in range(5)]

    result = bounded_issues(issues, maximum=3)

    assert len(result) == 3
    assert result[-1].code == "issues_truncated"
    assert result[-1].message == "3 additional normalization issue(s) omitted"
