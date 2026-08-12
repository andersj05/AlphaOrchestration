"""Provider-neutral financial observations with durable evidence lineage.

Provider adapters create these immutable records at the system boundary.  The
records intentionally use finite ``int``/``float`` values so ``to_dict`` output
fits :class:`alpha_orchestration.domain.JsonValue` and can be journaled without
custom decimal encoders.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Any

from alpha_orchestration.domain import JsonValue

MAX_NORMALIZATION_ISSUES = 100
_SCALES = frozenset({"units", "thousands", "millions", "billions"})


class DataProvider(StrEnum):
    SEC = "sec"
    YFINANCE = "yfinance"


class PeriodKind(StrEnum):
    INSTANT = "instant"
    DURATION = "duration"


class UnitKind(StrEnum):
    CURRENCY = "currency"
    SHARES = "shares"
    CURRENCY_PER_SHARE = "currency_per_share"
    RATIO = "ratio"
    COUNT = "count"


@dataclass(frozen=True, slots=True)
class FinancialUnit:
    """A value unit kept separate from provider-specific unit labels."""

    kind: UnitKind
    symbol: str
    scale: str = "units"

    def __post_init__(self) -> None:
        symbol = self.symbol.strip()
        if not symbol or len(symbol) > 32:
            raise ValueError("unit symbol must contain between 1 and 32 characters")
        if self.scale not in _SCALES:
            raise ValueError(f"unsupported financial unit scale: {self.scale!r}")
        if self.kind in {UnitKind.CURRENCY, UnitKind.CURRENCY_PER_SHARE}:
            symbol = symbol.upper()
        object.__setattr__(self, "symbol", symbol)

    def to_dict(self) -> dict[str, JsonValue]:
        return {"kind": self.kind.value, "symbol": self.symbol, "scale": self.scale}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FinancialUnit:
        return cls(
            kind=UnitKind(str(data["kind"])),
            symbol=str(data["symbol"]),
            scale=str(data.get("scale", "units")),
        )


@dataclass(frozen=True, slots=True)
class FinancialPeriod:
    """The accounting or market period represented by an observation."""

    kind: PeriodKind
    end: date
    start: date | None = None
    fiscal_year: int | None = None
    fiscal_period: str | None = None

    def __post_init__(self) -> None:
        if self.kind is PeriodKind.INSTANT and self.start is not None:
            raise ValueError("instant periods must not have a start date")
        if self.kind is PeriodKind.DURATION and self.start is None:
            raise ValueError("duration periods require a start date")
        if self.start is not None and self.start > self.end:
            raise ValueError("period start must not be after period end")
        if self.fiscal_year is not None and not 1_000 <= self.fiscal_year <= 9_999:
            raise ValueError("fiscal_year must be a four-digit year")
        if self.fiscal_period is not None:
            fiscal_period = self.fiscal_period.strip().upper()
            if not fiscal_period or len(fiscal_period) > 16:
                raise ValueError("fiscal_period must contain between 1 and 16 characters")
            object.__setattr__(self, "fiscal_period", fiscal_period)

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "kind": self.kind.value,
            "start": None if self.start is None else self.start.isoformat(),
            "end": self.end.isoformat(),
            "fiscal_year": self.fiscal_year,
            "fiscal_period": self.fiscal_period,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FinancialPeriod:
        raw_start = data.get("start")
        return cls(
            kind=PeriodKind(str(data["kind"])),
            start=None if raw_start is None else date.fromisoformat(str(raw_start)),
            end=date.fromisoformat(str(data["end"])),
            fiscal_year=None if data.get("fiscal_year") is None else int(data["fiscal_year"]),
            fiscal_period=None if data.get("fiscal_period") is None else str(data["fiscal_period"]),
        )


@dataclass(frozen=True, slots=True)
class EvidenceRecord:
    """A durable locator for one provider value used by an observation."""

    evidence_id: str
    provider: DataProvider
    source_kind: str
    source_locator: dict[str, JsonValue]
    source_url: str
    observed_at: datetime
    retrieved_at: datetime
    content_hash: str

    def __post_init__(self) -> None:
        _validate_identifier(self.evidence_id, "evidence_id", maximum=200)
        source_kind = self.source_kind.strip()
        if not source_kind or len(source_kind) > 80:
            raise ValueError("source_kind must contain between 1 and 80 characters")
        source_url = self.source_url.strip()
        if not source_url or len(source_url) > 2_000:
            raise ValueError("source_url must contain between 1 and 2000 characters")
        locator = _json_object_copy(self.source_locator, "source_locator")
        observed_at = _utc_datetime(self.observed_at, "observed_at")
        retrieved_at = _utc_datetime(self.retrieved_at, "retrieved_at")
        digest = self.content_hash.lower()
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise ValueError("content_hash must be a 64-character hexadecimal SHA-256 digest")
        object.__setattr__(self, "source_kind", source_kind)
        object.__setattr__(self, "source_url", source_url)
        object.__setattr__(self, "source_locator", locator)
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "retrieved_at", retrieved_at)
        object.__setattr__(self, "content_hash", digest)

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "evidence_id": self.evidence_id,
            "provider": self.provider.value,
            "source_kind": self.source_kind,
            "source_locator": _json_object_copy(self.source_locator, "source_locator"),
            "source_url": self.source_url,
            "observed_at": self.observed_at.isoformat(),
            "retrieved_at": self.retrieved_at.isoformat(),
            "content_hash": self.content_hash,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> EvidenceRecord:
        return cls(
            evidence_id=str(data["evidence_id"]),
            provider=DataProvider(str(data["provider"])),
            source_kind=str(data["source_kind"]),
            source_locator=dict(data["source_locator"]),
            source_url=str(data["source_url"]),
            observed_at=_parse_datetime(data["observed_at"]),
            retrieved_at=_parse_datetime(data["retrieved_at"]),
            content_hash=str(data["content_hash"]),
        )


@dataclass(frozen=True, slots=True)
class FinancialObservation:
    """One canonical, unit- and period-scoped financial value."""

    observation_id: str
    entity_id: str
    name: str
    value: int | float
    unit: FinancialUnit
    period: FinancialPeriod
    evidence_ids: tuple[str, ...]
    ticker: str | None = None
    metadata: dict[str, JsonValue] | None = None

    def __post_init__(self) -> None:
        _validate_identifier(self.observation_id, "observation_id", maximum=200)
        _validate_identifier(self.entity_id, "entity_id", maximum=200)
        name = self.name.strip()
        if not name or len(name) > 100:
            raise ValueError("observation name must contain between 1 and 100 characters")
        if isinstance(self.value, bool) or not isinstance(self.value, (int, float)):
            raise ValueError("observation value must be an int or float")
        if isinstance(self.value, float) and not math.isfinite(self.value):
            raise ValueError("observation value must be finite")
        evidence_ids = tuple(self.evidence_ids)
        if not evidence_ids or len(evidence_ids) > 100:
            raise ValueError("observations require between 1 and 100 evidence IDs")
        if len(evidence_ids) != len(set(evidence_ids)):
            raise ValueError("observation evidence IDs must be unique")
        for evidence_id in evidence_ids:
            _validate_identifier(evidence_id, "evidence_id", maximum=200)
        ticker = None if self.ticker is None else self.ticker.strip().upper()
        if ticker is not None and (not ticker or len(ticker) > 32):
            raise ValueError("ticker must contain between 1 and 32 characters")
        metadata = {} if self.metadata is None else _json_object_copy(self.metadata, "metadata")
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "evidence_ids", evidence_ids)
        object.__setattr__(self, "ticker", ticker)
        object.__setattr__(self, "metadata", metadata)

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "observation_id": self.observation_id,
            "entity_id": self.entity_id,
            "ticker": self.ticker,
            "name": self.name,
            "value": self.value,
            "unit": self.unit.to_dict(),
            "period": self.period.to_dict(),
            "evidence_ids": list(self.evidence_ids),
            "metadata": _json_object_copy(self.metadata or {}, "metadata"),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FinancialObservation:
        raw_value = data["value"]
        if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)):
            raise ValueError("observation value must be an int or float")
        return cls(
            observation_id=str(data["observation_id"]),
            entity_id=str(data["entity_id"]),
            ticker=None if data.get("ticker") is None else str(data["ticker"]),
            name=str(data["name"]),
            value=raw_value,
            unit=FinancialUnit.from_dict(dict(data["unit"])),
            period=FinancialPeriod.from_dict(dict(data["period"])),
            evidence_ids=tuple(str(item) for item in data["evidence_ids"]),
            metadata=dict(data.get("metadata", {})),
        )


@dataclass(frozen=True, slots=True)
class NormalizationIssue:
    code: str
    provider_path: str
    message: str

    def __post_init__(self) -> None:
        for value, name, maximum in (
            (self.code, "issue code", 80),
            (self.provider_path, "provider path", 500),
            (self.message, "issue message", 1_000),
        ):
            stripped = value.strip()
            if not stripped or len(stripped) > maximum:
                raise ValueError(f"{name} must contain between 1 and {maximum} characters")
            object.__setattr__(self, name.replace("issue ", "").replace("provider ", "provider_"), stripped)

    def to_dict(self) -> dict[str, JsonValue]:
        return {"code": self.code, "provider_path": self.provider_path, "message": self.message}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> NormalizationIssue:
        return cls(code=str(data["code"]), provider_path=str(data["provider_path"]), message=str(data["message"]))


@dataclass(frozen=True, slots=True)
class ObservationBatch:
    """A self-contained normalized result suitable for strict JSON journaling."""

    observations: tuple[FinancialObservation, ...] = ()
    evidence: tuple[EvidenceRecord, ...] = ()
    issues: tuple[NormalizationIssue, ...] = ()

    def __post_init__(self) -> None:
        observations = tuple(self.observations)
        evidence = tuple(self.evidence)
        issues = tuple(self.issues)
        if len(issues) > MAX_NORMALIZATION_ISSUES:
            raise ValueError(f"normalization batches may contain at most {MAX_NORMALIZATION_ISSUES} issues")
        observation_ids = [item.observation_id for item in observations]
        evidence_ids = [item.evidence_id for item in evidence]
        if len(observation_ids) != len(set(observation_ids)):
            raise ValueError("observation IDs must be unique within a batch")
        if len(evidence_ids) != len(set(evidence_ids)):
            raise ValueError("evidence IDs must be unique within a batch")
        available_evidence = set(evidence_ids)
        unresolved = sorted(
            evidence_id
            for observation in observations
            for evidence_id in observation.evidence_ids
            if evidence_id not in available_evidence
        )
        if unresolved:
            raise ValueError(f"observations reference evidence absent from the batch: {unresolved!r}")
        object.__setattr__(self, "observations", observations)
        object.__setattr__(self, "evidence", evidence)
        object.__setattr__(self, "issues", issues)

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "observations": [item.to_dict() for item in self.observations],
            "evidence": [item.to_dict() for item in self.evidence],
            "issues": [item.to_dict() for item in self.issues],
        }

    def resolve_evidence(self, evidence_ids: Sequence[str]) -> tuple[EvidenceRecord, ...]:
        """Resolve IDs in caller order and fail closed on duplicates or unknown IDs."""

        if isinstance(evidence_ids, (str, bytes)) or not isinstance(evidence_ids, Sequence):
            raise ValueError("evidence_ids must be a sequence of strings")
        requested = tuple(evidence_ids)
        if any(not isinstance(evidence_id, str) or not evidence_id for evidence_id in requested):
            raise ValueError("evidence_ids must contain only non-empty strings")
        if len(requested) != len(set(requested)):
            raise ValueError("evidence_ids must not contain duplicates")
        indexed = {record.evidence_id: record for record in self.evidence}
        unknown = sorted(set(requested).difference(indexed))
        if unknown:
            raise ValueError(f"unknown evidence IDs: {unknown!r}")
        return tuple(indexed[evidence_id] for evidence_id in requested)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ObservationBatch:
        return cls(
            observations=tuple(FinancialObservation.from_dict(dict(item)) for item in data.get("observations", [])),
            evidence=tuple(EvidenceRecord.from_dict(dict(item)) for item in data.get("evidence", [])),
            issues=tuple(NormalizationIssue.from_dict(dict(item)) for item in data.get("issues", [])),
        )


def canonical_content_hash(value: JsonValue) -> str:
    """Return a stable digest for strict-JSON provider content or locators."""

    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def evidence_id_for(provider: DataProvider, source_kind: str, locator: dict[str, JsonValue]) -> str:
    digest = canonical_content_hash({"provider": provider.value, "source_kind": source_kind, "locator": locator})
    return f"{provider.value}:{source_kind}:{digest[:24]}"


def observation_id_for(
    provider: DataProvider,
    *,
    entity_id: str,
    name: str,
    period: FinancialPeriod,
    evidence_id: str,
) -> str:
    digest = canonical_content_hash(
        {
            "provider": provider.value,
            "entity_id": entity_id,
            "name": name,
            "period": period.to_dict(),
            "evidence_id": evidence_id,
        }
    )
    return f"obs:{provider.value}:{digest[:24]}"


def bounded_issues(
    issues: list[NormalizationIssue],
    *,
    maximum: int = MAX_NORMALIZATION_ISSUES,
) -> tuple[NormalizationIssue, ...]:
    """Bound malformed-provider noise while reporting how many issues were omitted."""

    if maximum < 1:
        raise ValueError("maximum must be positive")
    if len(issues) <= maximum:
        return tuple(issues)
    omitted = len(issues) - (maximum - 1)
    return (
        *issues[: maximum - 1],
        NormalizationIssue(
            code="issues_truncated",
            provider_path="$",
            message=f"{omitted} additional normalization issue(s) omitted",
        ),
    )


def _validate_identifier(value: str, name: str, *, maximum: int) -> None:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    stripped = value.strip()
    if not stripped or stripped != value or len(value) > maximum:
        raise ValueError(f"{name} must be non-empty, unpadded, and at most {maximum} characters")


def _json_object_copy(value: object, name: str) -> dict[str, JsonValue]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError(f"{name} must be an object with string keys")
    try:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
        decoded = json.loads(encoded)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must contain only finite JSON values") from exc
    if not isinstance(decoded, dict):  # pragma: no cover - guarded by the input check
        raise ValueError(f"{name} must be a JSON object")
    return decoded


def _utc_datetime(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be a timezone-aware datetime")
    return value.astimezone(UTC)


def _parse_datetime(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return _utc_datetime(parsed, "datetime")
