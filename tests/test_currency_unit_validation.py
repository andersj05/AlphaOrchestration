from datetime import UTC, datetime

import pytest

from alpha_orchestration.data.sec_mapping import map_sec_company_facts
from alpha_orchestration.data.yfinance import MarketSnapshot
from alpha_orchestration.data.yfinance_mapping import map_yfinance_history, map_yfinance_snapshot

RETRIEVED_AT = datetime(2025, 3, 1, tzinfo=UTC)


def test_sec_rejects_alphabetic_non_currency_per_share_prefix() -> None:
    payload = {
        "cik": 1,
        "facts": {
            "us-gaap": {
                "EarningsPerShareDiluted": {
                    "units": {
                        "widgets/shares": [
                            {
                                "val": 1.25,
                                "start": "2024-01-01",
                                "end": "2024-12-31",
                                "filed": "2025-01-31",
                                "form": "10-K",
                                "accn": "0000000001-25-000001",
                                "fy": 2024,
                                "fp": "FY",
                            }
                        ]
                    }
                }
            }
        },
    }

    batch = map_sec_company_facts(payload, retrieved_at=RETRIEVED_AT)

    assert not batch.observations
    assert [issue.code for issue in batch.issues] == ["unsupported_unit"]


def test_yfinance_rejects_alphabetic_non_currency_code() -> None:
    snapshot = MarketSnapshot("ABC", "widgets", 10.0, 1_000.0, "NMS")

    snapshot_batch = map_yfinance_snapshot(
        snapshot,
        observed_at=datetime(2025, 3, 1, tzinfo=UTC),
        retrieved_at=RETRIEVED_AT,
    )

    assert not snapshot_batch.observations
    assert [issue.code for issue in snapshot_batch.issues] == ["missing_currency"]
    with pytest.raises(ValueError, match="currency"):
        map_yfinance_history(
            "ABC",
            [],
            currency="widgets",
            auto_adjust=False,
            retrieved_at=RETRIEVED_AT,
        )
