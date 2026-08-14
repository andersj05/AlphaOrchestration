"""Source-bound projection of universe screener values into observations."""

from __future__ import annotations

from alpha_orchestration.data.observations import (
    DataProvider,
    EvidenceRecord,
    FinancialObservation,
    FinancialPeriod,
    FinancialUnit,
    ObservationBatch,
    PeriodKind,
    UnitKind,
    evidence_id_for,
    observation_id_for,
)
from alpha_orchestration.data.universe import YFINANCE_SCREENER_URL, UniverseMember
from alpha_orchestration.domain import JsonValue


def manifest_market_batch(member: UniverseMember) -> ObservationBatch:
    """Project one selected screen row without weakening its currency or page lineage.

    Yahoo's screen result does not expose a quote timestamp.  The page retrieval time
    is therefore used for both ``observed_at`` and ``retrieved_at``, and that limitation
    is explicit in observation metadata.
    """

    if not isinstance(member, UniverseMember):
        raise ValueError("member must be a UniverseMember")
    observed_at = member.market_retrieved_at
    period = FinancialPeriod(PeriodKind.INSTANT, end=observed_at.date())
    records: list[tuple[FinancialObservation, EvidenceRecord]] = []
    for provider_field, canonical_name, value, unit_kind in (
        ("regularMarketPrice", "share_price", member.share_price, UnitKind.CURRENCY_PER_SHARE),
        ("marketCap", "market_cap", member.market_cap, UnitKind.CURRENCY),
        (
            "averageDailyVolume3Month",
            "average_daily_volume_3m",
            member.average_daily_volume_3m,
            UnitKind.SHARES,
        ),
    ):
        locator: dict[str, JsonValue] = {
            "ticker": member.ticker,
            "provider_field": provider_field,
            "request_hash": member.market_request_hash,
            "page_content_hash": member.market_content_hash,
            "reported_value": value,
            "currency": member.currency,
            "exchange": member.market_exchange_code,
            "retrieved_at": observed_at.isoformat(),
        }
        evidence_id = evidence_id_for(DataProvider.YFINANCE, "equity_screen_page", locator)
        evidence = EvidenceRecord(
            evidence_id=evidence_id,
            provider=DataProvider.YFINANCE,
            source_kind="equity_screen_page",
            source_locator=locator,
            source_url=YFINANCE_SCREENER_URL,
            observed_at=observed_at,
            retrieved_at=observed_at,
            content_hash=member.market_content_hash,
        )
        unit = (
            FinancialUnit(unit_kind, "shares")
            if unit_kind is UnitKind.SHARES
            else FinancialUnit(unit_kind, member.currency)
        )
        entity_id = member.entity_id
        observation = FinancialObservation(
            observation_id=observation_id_for(
                DataProvider.YFINANCE,
                entity_id=entity_id,
                name=canonical_name,
                period=period,
                evidence_id=evidence_id,
            ),
            entity_id=entity_id,
            ticker=member.ticker,
            name=canonical_name,
            value=value,
            unit=unit,
            period=period,
            evidence_ids=(evidence_id,),
            metadata={
                "provider_field": provider_field,
                "exchange": member.market_exchange_code,
                "currency": member.currency,
                "request_hash": member.market_request_hash,
                "page_content_hash": member.market_content_hash,
                "observed_at_basis": "provider_page_retrieval_time",
            },
        )
        records.append((observation, evidence))
    records.sort(key=lambda item: item[0].name)
    return ObservationBatch(
        observations=tuple(item[0] for item in records),
        evidence=tuple(item[1] for item in records),
    )
