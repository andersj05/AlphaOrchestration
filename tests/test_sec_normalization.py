import json
from datetime import UTC, datetime

import pytest

from alpha_orchestration.data.observations import PeriodKind, UnitKind
from alpha_orchestration.data.sec_mapping import map_sec_company_facts

RETRIEVED_AT = datetime(2025, 3, 1, 12, tzinfo=UTC)


def _fact(
    value: int | float,
    *,
    start: str | None = "2024-01-01",
    end: str = "2024-12-31",
    filed: str = "2025-01-31",
    form: str = "10-K",
    accession: str = "0000000001-25-000001",
    fiscal_year: int = 2024,
    fiscal_period: str = "FY",
) -> dict:
    result = {
        "val": value,
        "end": end,
        "filed": filed,
        "form": form,
        "accn": accession,
        "fy": fiscal_year,
        "fp": fiscal_period,
        "frame": f"CY{fiscal_year}",
    }
    if start is not None:
        result["start"] = start
    return result


def test_sec_mapper_normalizes_periods_signs_units_and_latest_restatement() -> None:
    annual_original = _fact(100)
    annual_amendment = _fact(
        105,
        filed="2025-02-15",
        form="10-K/A",
        accession="0000000001-25-000002",
    )
    q1 = _fact(
        30,
        start="2025-01-01",
        end="2025-03-31",
        filed="2025-05-01",
        form="10-Q",
        accession="0000000001-25-000003",
        fiscal_year=2025,
        fiscal_period="Q1",
    )
    payload = {
        "cik": 1,
        "entityName": "Example Corp",
        "tickers": ["exm"],
        "facts": {
            "us-gaap": {
                "RevenueFromContractWithCustomerExcludingAssessedTax": {
                    "label": "Revenue",
                    "units": {"USD": [annual_original, annual_amendment, q1]},
                },
                "SalesRevenueNet": {
                    "label": "Legacy revenue",
                    "units": {
                        "USD": [
                            _fact(
                                104,
                                filed="2025-02-15",
                                form="10-K/A",
                                accession="0000000001-25-000002",
                            )
                        ]
                    },
                },
                "PaymentsToAcquirePropertyPlantAndEquipment": {
                    "label": "Capital expenditures",
                    "units": {"USD": [_fact(-12)]},
                },
                "Assets": {
                    "label": "Assets",
                    "units": {"USD": [_fact(400, start=None)]},
                },
            },
            "example-extension": {
                "UnrecognizedCompanyMetric": {"units": {"USD": [_fact(999)]}},
            },
        },
    }

    batch = map_sec_company_facts(payload, retrieved_at=RETRIEVED_AT)

    revenues = [item for item in batch.observations if item.name == "revenue"]
    assert [(item.value, item.period.fiscal_period) for item in revenues] == [(105, "FY"), (30, "Q1")]
    assert revenues[0].ticker == "EXM"
    assert revenues[0].unit.kind is UnitKind.CURRENCY
    assert revenues[0].unit.symbol == "USD"
    assert revenues[0].metadata["concept"] == "RevenueFromContractWithCustomerExcludingAssessedTax"
    assert revenues[0].metadata["form"] == "10-K/A"
    capex = next(item for item in batch.observations if item.name == "capital_expenditures")
    assert capex.value == 12
    assert capex.metadata["reported_value"] == -12
    assert capex.metadata["sign_policy"] == "absolute"
    assets = next(item for item in batch.observations if item.name == "total_assets")
    assert assets.period.kind is PeriodKind.INSTANT
    assert assets.period.start is None
    assert not batch.issues
    assert len(batch.evidence) == len(batch.observations) == 4
    assert all(item.evidence_ids[0].startswith("sec:company_fact:") for item in batch.observations)


def test_sec_mapper_preserves_distinct_duration_contexts_and_currencies() -> None:
    payload = {
        "cik": "CIK0000000001",
        "facts": {
            "us-gaap": {
                "RevenueFromContractWithCustomerExcludingAssessedTax": {
                    "units": {
                        "USD": [
                            _fact(20, start="2025-04-01", end="2025-06-30", fiscal_year=2025, fiscal_period="Q2"),
                            _fact(35, start="2025-01-01", end="2025-06-30", fiscal_year=2025, fiscal_period="Q2"),
                        ],
                        "EUR": [
                            _fact(32, start="2025-01-01", end="2025-06-30", fiscal_year=2025, fiscal_period="Q2")
                        ],
                    }
                }
            }
        },
    }

    batch = map_sec_company_facts(payload, retrieved_at=RETRIEVED_AT)

    assert len(batch.observations) == 3
    assert {(item.period.start.isoformat(), item.unit.symbol) for item in batch.observations} == {
        ("2025-01-01", "EUR"),
        ("2025-01-01", "USD"),
        ("2025-04-01", "USD"),
    }


def test_sec_mapper_is_deterministic_and_emits_strict_json() -> None:
    payload = {
        "cik": 1,
        "facts": {
            "us-gaap": {
                "NetIncomeLoss": {"units": {"USD": [_fact(10)]}},
            }
        },
    }

    first = map_sec_company_facts(payload, retrieved_at=RETRIEVED_AT)
    second = map_sec_company_facts(payload, retrieved_at=RETRIEVED_AT)

    assert first == second
    json.dumps(first.to_dict(), allow_nan=False)
    assert first.evidence[0].source_url.endswith("/000000000125000001/")


def test_sec_mapper_bounds_malformed_known_fact_issues() -> None:
    bad_facts = [
        {
            "val": True,
            "start": "2024-01-01",
            "end": "not-a-date",
            "filed": "2025-01-31",
            "form": "10-K",
            "accn": f"bad-{index}",
        }
        for index in range(120)
    ]
    payload = {
        "cik": 1,
        "facts": {
            "us-gaap": {
                "RevenueFromContractWithCustomerExcludingAssessedTax": {
                    "units": {"USD": bad_facts, "widgets": [_fact(1)]}
                }
            }
        },
    }

    batch = map_sec_company_facts(payload, retrieved_at=RETRIEVED_AT)

    assert not batch.observations
    assert len(batch.issues) == 100
    assert batch.issues[-1].code == "issues_truncated"


def test_sec_mapper_requires_entity_identity() -> None:
    with pytest.raises(ValueError, match="missing cik"):
        map_sec_company_facts({"facts": {}}, retrieved_at=RETRIEVED_AT)
