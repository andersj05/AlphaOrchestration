from alpha_orchestration.data.sec import SecDataClient, normalize_cik


def test_cik_normalization() -> None:
    assert normalize_cik(320193) == "0000320193"
    assert normalize_cik("CIK0000320193") == "0000320193"


def test_recent_filings_projects_columnar_sec_payload() -> None:
    submissions = {
        "filings": {
            "recent": {
                "form": ["10-K", "4", "8-K"],
                "accessionNumber": ["a", "b", "c"],
                "filingDate": ["2025-01-01", "2025-01-02", "2025-01-03"],
            }
        }
    }

    rows = SecDataClient.recent_filings(submissions)

    assert [row["form"] for row in rows] == ["10-K", "8-K"]
    assert rows[1]["accessionNumber"] == "c"
