"""Small, rate-limited client for the SEC's public EDGAR JSON endpoints."""

from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Mapping
from typing import Any

import httpx


def normalize_cik(cik: str | int) -> str:
    digits = str(cik).strip().removeprefix("CIK")
    if not digits.isdigit() or len(digits) > 10:
        raise ValueError(f"invalid CIK: {cik!r}")
    return digits.zfill(10)


class SecDataClient:
    """Fetch submissions and XBRL facts without leaking HTTP types downstream."""

    BASE_URL = "https://data.sec.gov"

    def __init__(
        self,
        user_agent: str | None = None,
        *,
        requests_per_second: float = 8.0,
        timeout_seconds: float = 20.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        identity = (user_agent or os.getenv("ALPHA_SEC_USER_AGENT", "")).strip()
        if not identity:
            raise ValueError("Set ALPHA_SEC_USER_AGENT to a descriptive identity with contact information")
        if not 0 < requests_per_second <= 10:
            raise ValueError("requests_per_second must be in (0, 10]")
        self._minimum_interval = 1.0 / requests_per_second
        self._last_request_at = 0.0
        self._rate_lock = asyncio.Lock()
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            base_url=self.BASE_URL,
            timeout=timeout_seconds,
            follow_redirects=True,
            headers={
                "User-Agent": identity,
                "Accept": "application/json",
                "Accept-Encoding": "gzip, deflate",
            },
        )

    async def _get_json(self, path: str) -> dict[str, Any]:
        async with self._rate_lock:
            wait = self._minimum_interval - (time.monotonic() - self._last_request_at)
            if wait > 0:
                await asyncio.sleep(wait)
            response = await self._client.get(path)
            self._last_request_at = time.monotonic()
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError(f"SEC returned a non-object payload for {path}")
        return payload

    async def submissions(self, cik: str | int) -> dict[str, Any]:
        return await self._get_json(f"/submissions/CIK{normalize_cik(cik)}.json")

    async def company_facts(self, cik: str | int) -> dict[str, Any]:
        return await self._get_json(f"/api/xbrl/companyfacts/CIK{normalize_cik(cik)}.json")

    @staticmethod
    def recent_filings(
        submissions: Mapping[str, Any],
        *,
        forms: tuple[str, ...] = ("10-K", "10-Q", "8-K"),
    ) -> tuple[dict[str, Any], ...]:
        recent = submissions.get("filings", {})
        columns = recent.get("recent", {}) if isinstance(recent, Mapping) else {}
        if not isinstance(columns, Mapping):
            return ()
        form_values = list(columns.get("form", []))
        rows: list[dict[str, Any]] = []
        for index, form in enumerate(form_values):
            if str(form) not in forms:
                continue
            row = {
                key: values[index]
                for key, values in columns.items()
                if isinstance(values, list) and index < len(values)
            }
            rows.append(row)
        return tuple(rows)

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> SecDataClient:
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        del exc_type, exc, traceback
        await self.close()
