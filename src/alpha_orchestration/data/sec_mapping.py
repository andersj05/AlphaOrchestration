"""Map SEC company-facts payloads into canonical financial observations."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from typing import Any

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
from alpha_orchestration.data.sec import normalize_cik
from alpha_orchestration.domain import JsonValue


@dataclass(frozen=True, slots=True)
class _ConceptRule:
    name: str
    unit_kind: UnitKind
    priority: int = 0
    absolute_value: bool = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    observation: FinancialObservation
    evidence: EvidenceRecord
    concept_priority: int


_CONCEPT_RULES: dict[tuple[str, str], _ConceptRule] = {
    ("us-gaap", "RevenueFromContractWithCustomerExcludingAssessedTax"): _ConceptRule(
        "revenue", UnitKind.CURRENCY
    ),
    ("us-gaap", "SalesRevenueNet"): _ConceptRule("revenue", UnitKind.CURRENCY, priority=1),
    ("us-gaap", "Revenues"): _ConceptRule("revenue", UnitKind.CURRENCY, priority=2),
    ("us-gaap", "CostOfRevenue"): _ConceptRule("cost_of_revenue", UnitKind.CURRENCY),
    ("us-gaap", "CostOfGoodsAndServiceExcludingDepreciationDepletionAndAmortization"): _ConceptRule(
        "cost_of_revenue", UnitKind.CURRENCY, priority=1
    ),
    ("us-gaap", "CostOfGoodsSold"): _ConceptRule("cost_of_revenue", UnitKind.CURRENCY, priority=2),
    ("us-gaap", "GrossProfit"): _ConceptRule("gross_profit", UnitKind.CURRENCY),
    ("us-gaap", "OperatingIncomeLoss"): _ConceptRule("operating_income", UnitKind.CURRENCY),
    ("us-gaap", "NetIncomeLoss"): _ConceptRule("net_income", UnitKind.CURRENCY),
    ("us-gaap", "NetCashProvidedByUsedInOperatingActivities"): _ConceptRule(
        "operating_cash_flow", UnitKind.CURRENCY
    ),
    ("us-gaap", "PaymentsToAcquirePropertyPlantAndEquipment"): _ConceptRule(
        "capital_expenditures", UnitKind.CURRENCY, absolute_value=True
    ),
    ("us-gaap", "AssetsCurrent"): _ConceptRule("current_assets", UnitKind.CURRENCY),
    ("us-gaap", "LiabilitiesCurrent"): _ConceptRule("current_liabilities", UnitKind.CURRENCY),
    ("us-gaap", "CashAndCashEquivalentsAtCarryingValue"): _ConceptRule(
        "cash_and_equivalents", UnitKind.CURRENCY
    ),
    ("us-gaap", "DebtAndFinanceLeaseObligations"): _ConceptRule("total_debt", UnitKind.CURRENCY),
    ("us-gaap", "LongTermDebtAndFinanceLeaseObligations"): _ConceptRule(
        "total_debt", UnitKind.CURRENCY, priority=1
    ),
    ("us-gaap", "LongTermDebt"): _ConceptRule("total_debt", UnitKind.CURRENCY, priority=2),
    ("us-gaap", "LongTermDebtAndFinanceLeaseObligationsCurrent"): _ConceptRule(
        "current_debt", UnitKind.CURRENCY
    ),
    ("us-gaap", "LongTermDebtCurrent"): _ConceptRule("current_debt", UnitKind.CURRENCY, priority=1),
    ("us-gaap", "ShortTermBorrowings"): _ConceptRule("short_term_debt", UnitKind.CURRENCY),
    ("us-gaap", "LongTermDebtAndFinanceLeaseObligationsNoncurrent"): _ConceptRule(
        "long_term_debt", UnitKind.CURRENCY
    ),
    ("us-gaap", "LongTermDebtNoncurrent"): _ConceptRule(
        "long_term_debt", UnitKind.CURRENCY, priority=1
    ),
    ("us-gaap", "StockholdersEquity"): _ConceptRule("shareholders_equity", UnitKind.CURRENCY),
    ("us-gaap", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"): _ConceptRule(
        "shareholders_equity", UnitKind.CURRENCY, priority=1
    ),
    ("us-gaap", "Assets"): _ConceptRule("total_assets", UnitKind.CURRENCY),
    ("us-gaap", "AccountsReceivableNetCurrent"): _ConceptRule("accounts_receivable", UnitKind.CURRENCY),
    ("us-gaap", "InventoryNet"): _ConceptRule("inventory", UnitKind.CURRENCY),
    ("us-gaap", "AccountsPayableCurrent"): _ConceptRule("accounts_payable", UnitKind.CURRENCY),
    ("us-gaap", "InterestExpenseNonOperating"): _ConceptRule("interest_expense", UnitKind.CURRENCY),
    ("us-gaap", "IncomeTaxExpenseBenefit"): _ConceptRule("income_tax_expense", UnitKind.CURRENCY),
    (
        "us-gaap",
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
    ): _ConceptRule("income_before_tax", UnitKind.CURRENCY),
    (
        "us-gaap",
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments",
    ): _ConceptRule("income_before_tax", UnitKind.CURRENCY, priority=1),
    ("us-gaap", "DepreciationDepletionAndAmortization"): _ConceptRule(
        "depreciation_and_amortization", UnitKind.CURRENCY
    ),
    ("us-gaap", "DepreciationDepletionAndAmortizationPropertyPlantAndEquipment"): _ConceptRule(
        "depreciation_and_amortization", UnitKind.CURRENCY, priority=1
    ),
    ("us-gaap", "WeightedAverageNumberOfDilutedSharesOutstanding"): _ConceptRule(
        "diluted_shares_outstanding", UnitKind.SHARES
    ),
    ("us-gaap", "EarningsPerShareDiluted"): _ConceptRule(
        "earnings_per_share", UnitKind.CURRENCY_PER_SHARE
    ),
    ("us-gaap", "PaymentsOfDividends"): _ConceptRule(
        "dividends_paid", UnitKind.CURRENCY, absolute_value=True
    ),
    ("us-gaap", "PaymentsOfDividendsCommonStock"): _ConceptRule(
        "dividends_paid", UnitKind.CURRENCY, priority=1, absolute_value=True
    ),
    ("us-gaap", "CommonStockDividendsPerShareDeclared"): _ConceptRule(
        "dividends_per_share", UnitKind.CURRENCY_PER_SHARE, absolute_value=True
    ),
    ("us-gaap", "EffectiveIncomeTaxRateContinuingOperations"): _ConceptRule(
        "effective_tax_rate", UnitKind.RATIO
    ),
    ("us-gaap", "PreferredStocksIncludingAdditionalPaidInCapital"): _ConceptRule(
        "preferred_stock", UnitKind.CURRENCY
    ),
    ("us-gaap", "MinorityInterest"): _ConceptRule("minority_interest", UnitKind.CURRENCY),
    ("dei", "EntityCommonStockSharesOutstanding"): _ConceptRule("shares_outstanding", UnitKind.SHARES),
}


def map_sec_company_facts(
    payload: Mapping[str, Any],
    *,
    retrieved_at: datetime,
    forms: tuple[str, ...] = ("10-K", "10-Q", "10-K/A", "10-Q/A"),
    ticker: str | None = None,
) -> ObservationBatch:
    """Normalize supported values from one SEC company-facts response.

    Comparative values are commonly repeated in later filings.  For an equal
    canonical name, unit, and exact period, the latest filed value wins.  An
    ordered concept preference resolves aliases reported in the same filing.
    Quarter, year-to-date, and annual durations remain separate because their
    start dates differ.
    """

    if not isinstance(payload, Mapping):
        raise ValueError("SEC company-facts payload must be an object")
    if not forms or any(not isinstance(form, str) or not form.strip() for form in forms):
        raise ValueError("forms must contain at least one non-empty form name")
    try:
        cik = normalize_cik(payload["cik"])
    except KeyError as exc:
        raise ValueError("SEC company-facts payload is missing cik") from exc
    entity_id = f"cik:{cik}"
    allowed_forms = frozenset(form.strip().upper() for form in forms)
    normalized_ticker = _ticker(payload, ticker)
    entity_name = _optional_text(payload.get("entityName"))
    issues: list[NormalizationIssue] = []
    facts = payload.get("facts")
    if not isinstance(facts, Mapping):
        issues.append(NormalizationIssue("invalid_facts", "facts", "facts must be an object"))
        return ObservationBatch(issues=bounded_issues(issues))

    selected: dict[tuple[object, ...], _Candidate] = {}
    for namespace, namespace_facts in facts.items():
        if not isinstance(namespace, str) or not isinstance(namespace_facts, Mapping):
            continue
        for concept, concept_payload in namespace_facts.items():
            if not isinstance(concept, str):
                continue
            rule = _CONCEPT_RULES.get((namespace, concept))
            if rule is None:
                continue
            base_path = f"facts.{namespace}.{concept}"
            if not isinstance(concept_payload, Mapping):
                issues.append(
                    NormalizationIssue("invalid_concept", base_path, "known concept payload must be an object")
                )
                continue
            units = concept_payload.get("units")
            if not isinstance(units, Mapping):
                issues.append(NormalizationIssue("invalid_units", f"{base_path}.units", "units must be an object"))
                continue
            label = _optional_text(concept_payload.get("label"))
            for provider_unit, raw_facts in units.items():
                unit_path = f"{base_path}.units.{provider_unit}"
                if not isinstance(provider_unit, str) or not isinstance(raw_facts, list):
                    issues.append(
                        NormalizationIssue("invalid_unit_facts", unit_path, "unit facts must be an array")
                    )
                    continue
                unit = _unit(provider_unit)
                if unit is None or unit.kind is not rule.unit_kind:
                    issues.append(
                        NormalizationIssue(
                            "unsupported_unit",
                            unit_path,
                            f"{provider_unit!r} is not valid for canonical metric {rule.name}",
                        )
                    )
                    continue
                for index, raw_fact in enumerate(raw_facts):
                    candidate = _candidate(
                        raw_fact,
                        path=f"{unit_path}[{index}]",
                        cik=cik,
                        entity_id=entity_id,
                        entity_name=entity_name,
                        ticker=normalized_ticker,
                        namespace=namespace,
                        concept=concept,
                        label=label,
                        provider_unit=provider_unit,
                        unit=unit,
                        rule=rule,
                        retrieved_at=retrieved_at,
                        allowed_forms=allowed_forms,
                        issues=issues,
                    )
                    if candidate is None:
                        continue
                    key = _candidate_key(candidate.observation)
                    existing = selected.get(key)
                    if existing is None or _candidate_rank(candidate) > _candidate_rank(existing):
                        selected[key] = candidate

    candidates = sorted(selected.values(), key=_output_sort_key)
    return ObservationBatch(
        observations=tuple(candidate.observation for candidate in candidates),
        evidence=tuple(candidate.evidence for candidate in candidates),
        issues=bounded_issues(issues),
    )


def _candidate(
    raw_fact: object,
    *,
    path: str,
    cik: str,
    entity_id: str,
    entity_name: str | None,
    ticker: str | None,
    namespace: str,
    concept: str,
    label: str | None,
    provider_unit: str,
    unit: FinancialUnit,
    rule: _ConceptRule,
    retrieved_at: datetime,
    allowed_forms: frozenset[str],
    issues: list[NormalizationIssue],
) -> _Candidate | None:
    if not isinstance(raw_fact, Mapping):
        issues.append(NormalizationIssue("invalid_fact", path, "fact must be an object"))
        return None
    raw_form = raw_fact.get("form")
    if not isinstance(raw_form, str) or not raw_form.strip():
        issues.append(NormalizationIssue("missing_form", path, "fact is missing its filing form"))
        return None
    form = raw_form.strip().upper()
    if form not in allowed_forms:
        return None
    accession = _optional_text(raw_fact.get("accn"))
    if accession is None:
        issues.append(NormalizationIssue("missing_accession", path, "fact is missing its accession number"))
        return None
    try:
        filing_date = _iso_date(raw_fact.get("filed"), "filed")
        period = _period(raw_fact)
        reported_value = _number(raw_fact.get("val"))
    except ValueError as exc:
        issues.append(NormalizationIssue("invalid_fact", path, str(exc)))
        return None

    normalized_value = abs(reported_value) if rule.absolute_value else reported_value
    frame = _optional_text(raw_fact.get("frame"))
    locator: dict[str, JsonValue] = {
        "cik": cik,
        "taxonomy": namespace,
        "concept": concept,
        "provider_unit": provider_unit,
        "accession_number": accession,
        "form": form,
        "filing_date": filing_date.isoformat(),
        "start": None if period.start is None else period.start.isoformat(),
        "end": period.end.isoformat(),
        "frame": frame,
        "reported_value": reported_value,
    }
    content: dict[str, JsonValue] = {
        **locator,
        "fiscal_year": period.fiscal_year,
        "fiscal_period": period.fiscal_period,
        "decimals": _json_scalar(raw_fact.get("decimals")),
    }
    content_hash = canonical_content_hash(content)
    locator["content_fingerprint"] = content_hash[:16]
    evidence_id = evidence_id_for(DataProvider.SEC, "company_fact", locator)
    evidence = EvidenceRecord(
        evidence_id=evidence_id,
        provider=DataProvider.SEC,
        source_kind="company_fact",
        source_locator=locator,
        source_url=(
            f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/"
        ),
        observed_at=datetime.combine(filing_date, time(), tzinfo=UTC),
        retrieved_at=retrieved_at,
        content_hash=content_hash,
    )
    observation = FinancialObservation(
        observation_id=observation_id_for(
            DataProvider.SEC,
            entity_id=entity_id,
            name=rule.name,
            period=period,
            evidence_id=evidence_id,
        ),
        entity_id=entity_id,
        ticker=ticker,
        name=rule.name,
        value=normalized_value,
        unit=unit,
        period=period,
        evidence_ids=(evidence_id,),
        metadata={
            "taxonomy": namespace,
            "concept": concept,
            "label": label,
            "form": form,
            "accession_number": accession,
            "filing_date": filing_date.isoformat(),
            "frame": frame,
            "entity_name": entity_name,
            "provider_unit": provider_unit,
            "reported_value": reported_value,
            "sign_policy": "absolute" if rule.absolute_value else "preserve",
        },
    )
    return _Candidate(observation, evidence, rule.priority)


def _period(raw_fact: Mapping[str, Any]) -> FinancialPeriod:
    end = _iso_date(raw_fact.get("end"), "end")
    raw_start = raw_fact.get("start")
    start = None if raw_start is None else _iso_date(raw_start, "start")
    raw_year = raw_fact.get("fy")
    fiscal_year: int | None
    if raw_year is None:
        fiscal_year = None
    elif isinstance(raw_year, bool):
        raise ValueError("fy must be a four-digit year")
    else:
        try:
            fiscal_year = int(raw_year)
        except (TypeError, ValueError) as exc:
            raise ValueError("fy must be a four-digit year") from exc
    return FinancialPeriod(
        kind=PeriodKind.INSTANT if start is None else PeriodKind.DURATION,
        start=start,
        end=end,
        fiscal_year=fiscal_year,
        fiscal_period=_optional_text(raw_fact.get("fp")),
    )


def _unit(provider_unit: str) -> FinancialUnit | None:
    normalized = provider_unit.strip()
    lowered = normalized.lower()
    if lowered == "shares":
        return FinancialUnit(UnitKind.SHARES, "shares")
    if lowered == "pure":
        return FinancialUnit(UnitKind.RATIO, "pure")
    if lowered.endswith("/shares"):
        currency = normalized[: -len("/shares")]
        if len(currency) == 3 and currency.isalpha():
            return FinancialUnit(UnitKind.CURRENCY_PER_SHARE, currency)
        return None
    if len(normalized) == 3 and normalized.isalpha():
        return FinancialUnit(UnitKind.CURRENCY, normalized)
    return None


def _iso_date(value: object, name: str) -> date:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be an ISO date")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO date") from exc


def _number(value: object) -> int | float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("val must be a finite JSON number")
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("val must be a finite JSON number")
    return value


def _ticker(payload: Mapping[str, Any], ticker: str | None) -> str | None:
    candidate = ticker
    if candidate is None:
        raw_tickers = payload.get("tickers")
        if isinstance(raw_tickers, list) and raw_tickers and isinstance(raw_tickers[0], str):
            candidate = raw_tickers[0]
    if candidate is None:
        return None
    normalized = candidate.strip().upper()
    if not normalized or len(normalized) > 32:
        raise ValueError("ticker must contain between 1 and 32 characters")
    return normalized


def _optional_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _json_scalar(value: object) -> JsonValue:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return None


def _candidate_key(observation: FinancialObservation) -> tuple[object, ...]:
    return (
        observation.entity_id,
        observation.name,
        observation.unit.kind.value,
        observation.unit.symbol,
        observation.unit.scale,
        observation.period.kind.value,
        observation.period.start,
        observation.period.end,
    )


def _candidate_rank(candidate: _Candidate) -> tuple[datetime, int, str]:
    return (candidate.evidence.observed_at, -candidate.concept_priority, candidate.evidence.evidence_id)


def _output_sort_key(candidate: _Candidate) -> tuple[object, ...]:
    observation = candidate.observation
    return (
        observation.name,
        observation.period.end,
        date.min if observation.period.start is None else observation.period.start,
        observation.unit.symbol,
        observation.observation_id,
    )
