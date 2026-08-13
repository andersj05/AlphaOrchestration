import json
from dataclasses import replace
from datetime import UTC, date, datetime

import pytest

from alpha_orchestration.data import (
    DataProvider,
    EvidencePacket,
    EvidencePacketLimitError,
    EvidenceRecord,
    FinancialObservation,
    FinancialPeriod,
    FinancialUnit,
    LedgerCollisionError,
    MarketSnapshot,
    NormalizationIssue,
    ObservationBatch,
    ObservationLedger,
    PeriodKind,
    UnitKind,
    map_sec_company_facts,
    map_yfinance_history,
    map_yfinance_snapshot,
)
from alpha_orchestration.data.observations import (
    canonical_content_hash,
    evidence_id_for,
    observation_id_for,
)
from alpha_orchestration.data.sec import map_sec_company_facts as sec_adapter_mapper
from alpha_orchestration.data.yfinance import (
    map_yfinance_history as yfinance_history_adapter_mapper,
)
from alpha_orchestration.data.yfinance import (
    map_yfinance_snapshot as yfinance_snapshot_adapter_mapper,
)

RETRIEVED_AT = datetime(2025, 3, 3, 20, tzinfo=UTC)


def _records(
    *,
    name: str,
    value: int | float,
    end: date,
    start: date | None = None,
    currency: str = "USD",
    suffix: str,
) -> tuple[FinancialObservation, EvidenceRecord]:
    period = FinancialPeriod(
        PeriodKind.INSTANT if start is None else PeriodKind.DURATION,
        start=start,
        end=end,
        fiscal_year=end.year,
        fiscal_period=None if start is None else "FY",
    )
    locator = {
        "ticker": "ABC",
        "field": name,
        "period_end": end.isoformat(),
        "suffix": suffix,
        "value": value,
    }
    content_hash = canonical_content_hash(locator)
    evidence_id = evidence_id_for(DataProvider.SEC, "fixture_fact", locator)
    evidence = EvidenceRecord(
        evidence_id=evidence_id,
        provider=DataProvider.SEC,
        source_kind="fixture_fact",
        source_locator=locator,
        source_url=f"https://example.test/{suffix}",
        observed_at=datetime.combine(end, datetime.min.time(), tzinfo=UTC),
        retrieved_at=RETRIEVED_AT,
        content_hash=content_hash,
    )
    observation = FinancialObservation(
        observation_id=observation_id_for(
            DataProvider.SEC,
            entity_id="ticker:ABC",
            name=name,
            period=period,
            evidence_id=evidence_id,
        ),
        entity_id="ticker:ABC",
        ticker="ABC",
        name=name,
        value=value,
        unit=FinancialUnit(UnitKind.CURRENCY, currency),
        period=period,
        evidence_ids=(evidence_id,),
        metadata={"suffix": suffix},
    )
    return observation, evidence


def _batch(*pairs: tuple[FinancialObservation, EvidenceRecord]) -> ObservationBatch:
    return ObservationBatch(
        observations=tuple(pair[0] for pair in pairs),
        evidence=tuple(pair[1] for pair in pairs),
    )


def test_batch_resolves_evidence_in_requested_order_and_fails_closed() -> None:
    first = _records(name="revenue", value=100, end=date(2024, 12, 31), suffix="first")
    second = _records(name="net_income", value=10, end=date(2024, 12, 31), suffix="second")
    batch = _batch(first, second)

    resolved = batch.resolve_evidence([second[1].evidence_id, first[1].evidence_id])

    assert resolved == (second[1], first[1])
    with pytest.raises(ValueError, match="unknown evidence IDs"):
        batch.resolve_evidence(["sec:unknown"])
    with pytest.raises(ValueError, match="must not contain duplicates"):
        batch.resolve_evidence([first[1].evidence_id, first[1].evidence_id])
    with pytest.raises(ValueError, match="sequence"):
        batch.resolve_evidence(first[1].evidence_id)


def test_ledger_ingest_is_idempotent_and_selection_preserves_periods_and_units() -> None:
    prior = _records(
        name="revenue",
        value=100,
        start=date(2023, 1, 1),
        end=date(2023, 12, 31),
        suffix="prior",
    )
    current_usd = _records(
        name="revenue",
        value=125,
        start=date(2024, 1, 1),
        end=date(2024, 12, 31),
        suffix="current-usd",
    )
    current_eur = _records(
        name="revenue",
        value=115,
        start=date(2024, 1, 1),
        end=date(2024, 12, 31),
        currency="EUR",
        suffix="current-eur",
    )
    ledger = ObservationLedger()

    ledger.ingest(_batch(current_eur, prior, current_usd))
    ledger.ingest(_batch(current_eur, prior, current_usd))

    assert len(ledger.observations) == 3
    assert len(ledger.evidence) == 3
    series = ledger.select(entity_id="ticker:ABC", name="revenue")
    assert [(record.period.end.year, record.unit.symbol, record.value) for record in series] == [
        (2023, "USD", 100),
        (2024, "EUR", 115),
        (2024, "USD", 125),
    ]
    exact = ledger.select(
        entity_id="ticker:ABC",
        name="revenue",
        period=current_usd[0].period,
    )
    assert [(record.unit.symbol, record.value) for record in exact] == [
        ("EUR", 115),
        ("USD", 125),
    ]
    assert not ledger.select(entity_id="ticker:ABC", name="not_present")


def test_ledger_rejects_unequal_id_collisions_without_partial_ingest() -> None:
    original = _records(name="revenue", value=100, end=date(2024, 12, 31), suffix="original")
    additional = _records(name="net_income", value=10, end=date(2024, 12, 31), suffix="additional")
    ledger = ObservationLedger((_batch(original),))
    conflicting_evidence = replace(original[1], source_url="https://example.test/conflict")

    with pytest.raises(LedgerCollisionError, match="evidence ID collision"):
        ledger.ingest(
            ObservationBatch(
                observations=(additional[0],),
                evidence=(additional[1], conflicting_evidence),
            )
        )

    assert ledger.observation_ids == (original[0].observation_id,)
    assert ledger.evidence_ids == (original[1].evidence_id,)

    conflicting_observation = replace(original[0], value=999)
    with pytest.raises(LedgerCollisionError, match="observation ID collision"):
        ledger.ingest(_batch((conflicting_observation, original[1])))
    assert ledger.get_observation(original[0].observation_id).value == 100


def test_evidence_packet_is_exact_bounded_and_deterministic() -> None:
    revenue = _records(name="revenue", value=100, end=date(2024, 12, 31), suffix="revenue")
    income = _records(name="net_income", value=10, end=date(2024, 12, 31), suffix="income")
    ledger = ObservationLedger((_batch(revenue, income),))

    first = ledger.evidence_packet([income[0].observation_id, revenue[0].observation_id])
    second = ledger.evidence_packet([revenue[0].observation_id, income[0].observation_id])

    assert first == second
    assert first.source_ids == tuple(sorted((revenue[1].evidence_id, income[1].evidence_id)))
    assert {record.unit.symbol for record in first.observations} == {"USD"}
    assert {record.period.end for record in first.observations} == {date(2024, 12, 31)}
    assert first.encoded_size_bytes == len(
        json.dumps(
            first.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode()
    )
    restored = EvidencePacket.from_dict(json.loads(json.dumps(first.to_dict(), allow_nan=False)))
    assert restored == first

    with pytest.raises(EvidencePacketLimitError, match="max_observations"):
        ledger.evidence_packet(
            [revenue[0].observation_id, income[0].observation_id],
            max_observations=1,
        )
    with pytest.raises(EvidencePacketLimitError, match="max_evidence"):
        ledger.evidence_packet(
            [revenue[0].observation_id, income[0].observation_id],
            max_evidence=1,
        )
    with pytest.raises(EvidencePacketLimitError, match="max_bytes"):
        ledger.evidence_packet([revenue[0].observation_id], max_bytes=100)
    with pytest.raises(ValueError, match="unknown observation IDs"):
        ledger.evidence_packet(["obs:unknown"])


def test_ledger_strict_json_round_trip_is_independent_of_ingest_order() -> None:
    revenue = _records(name="revenue", value=100, end=date(2024, 12, 31), suffix="revenue")
    income = _records(name="net_income", value=10, end=date(2024, 12, 31), suffix="income")
    issue = NormalizationIssue("fixture_warning", "facts.example", "fixture")
    first = ObservationLedger(
        (
            ObservationBatch(
                observations=(revenue[0],),
                evidence=(revenue[1],),
                issues=(issue,),
            ),
            _batch(income),
        )
    )
    second = ObservationLedger((_batch(income), _batch(revenue)))
    second.ingest(ObservationBatch(issues=(issue,)))

    assert first.to_dict() == second.to_dict()
    encoded = json.dumps(first.to_dict(), sort_keys=True, allow_nan=False)
    restored = ObservationLedger.from_dict(json.loads(encoded))
    assert restored.to_dict() == first.to_dict()
    assert restored.resolve_evidence(first.evidence_ids) == first.evidence


def test_adapter_and_package_reexports_map_without_network_access() -> None:
    sec_payload = {"cik": 1, "facts": {}}
    snapshot = MarketSnapshot("ABC", "USD", 10.0, 1_000.0, "NMS")
    history_rows = [{"Date": "2025-03-03", "Close": 10.0}]

    package_sec = map_sec_company_facts(sec_payload, retrieved_at=RETRIEVED_AT)
    adapter_sec = sec_adapter_mapper(sec_payload, retrieved_at=RETRIEVED_AT)
    package_snapshot = map_yfinance_snapshot(
        snapshot,
        observed_at=RETRIEVED_AT,
        retrieved_at=RETRIEVED_AT,
    )
    adapter_snapshot = yfinance_snapshot_adapter_mapper(
        snapshot,
        observed_at=RETRIEVED_AT,
        retrieved_at=RETRIEVED_AT,
    )
    package_history = map_yfinance_history(
        "ABC",
        history_rows,
        currency="USD",
        auto_adjust=False,
        retrieved_at=RETRIEVED_AT,
    )
    adapter_history = yfinance_history_adapter_mapper(
        "ABC",
        history_rows,
        currency="USD",
        auto_adjust=False,
        retrieved_at=RETRIEVED_AT,
    )

    assert package_sec == adapter_sec == ObservationBatch()
    assert package_snapshot == adapter_snapshot
    assert package_history == adapter_history
