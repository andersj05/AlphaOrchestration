"""Deterministic in-memory index for normalized financial observations."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any

from alpha_orchestration.data.observations import (
    EvidenceRecord,
    FinancialObservation,
    FinancialPeriod,
    NormalizationIssue,
    ObservationBatch,
)
from alpha_orchestration.domain import JsonValue

LEDGER_SCHEMA_VERSION = 1
EVIDENCE_PACKET_SCHEMA_VERSION = 1
DEFAULT_PACKET_MAX_OBSERVATIONS = 50
DEFAULT_PACKET_MAX_EVIDENCE = 100
DEFAULT_PACKET_MAX_BYTES = 128_000
MAX_PACKET_OBSERVATIONS = 100
MAX_PACKET_EVIDENCE = 100
MAX_PACKET_BYTES = 256_000


class LedgerCollisionError(ValueError):
    """Raised when a stable ID is reused for a different immutable record."""

    def __init__(self, record_kind: str, record_id: str) -> None:
        self.record_kind = record_kind
        self.record_id = record_id
        super().__init__(f"{record_kind} ID collision for {record_id!r}")


class EvidencePacketLimitError(ValueError):
    """Raised rather than silently dropping observations or evidence."""


@dataclass(frozen=True, slots=True)
class EvidencePacket:
    """A bounded, self-contained prompt packet and tool source-ID allowlist."""

    observations: tuple[FinancialObservation, ...]
    evidence: tuple[EvidenceRecord, ...]

    def __post_init__(self) -> None:
        observations = tuple(sorted(self.observations, key=_observation_sort_key))
        evidence = tuple(sorted(self.evidence, key=lambda record: record.evidence_id))
        observation_ids = [record.observation_id for record in observations]
        evidence_ids = [record.evidence_id for record in evidence]
        if len(observation_ids) != len(set(observation_ids)):
            raise ValueError("evidence packet observation IDs must be unique")
        if len(evidence_ids) != len(set(evidence_ids)):
            raise ValueError("evidence packet evidence IDs must be unique")
        referenced_ids = {evidence_id for observation in observations for evidence_id in observation.evidence_ids}
        available_ids = set(evidence_ids)
        missing = sorted(referenced_ids.difference(available_ids))
        unexpected = sorted(available_ids.difference(referenced_ids))
        if missing:
            raise ValueError(f"evidence packet is missing referenced evidence IDs: {missing!r}")
        if unexpected:
            raise ValueError(f"evidence packet contains unreferenced evidence IDs: {unexpected!r}")
        object.__setattr__(self, "observations", observations)
        object.__setattr__(self, "evidence", evidence)

    @property
    def source_ids(self) -> tuple[str, ...]:
        """Controller-owned allowlist for a scoped deterministic tool executor."""

        return tuple(record.evidence_id for record in self.evidence)

    @property
    def encoded_size_bytes(self) -> int:
        return len(_strict_json_bytes(self.to_dict()))

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "schema_version": EVIDENCE_PACKET_SCHEMA_VERSION,
            "source_ids": list(self.source_ids),
            "observations": [record.to_dict() for record in self.observations],
            "evidence": [record.to_dict() for record in self.evidence],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> EvidencePacket:
        if int(data.get("schema_version", -1)) != EVIDENCE_PACKET_SCHEMA_VERSION:
            raise ValueError("unsupported evidence packet schema_version")
        packet = cls(
            observations=tuple(FinancialObservation.from_dict(dict(record)) for record in data.get("observations", [])),
            evidence=tuple(EvidenceRecord.from_dict(dict(record)) for record in data.get("evidence", [])),
        )
        declared_source_ids = data.get("source_ids")
        if declared_source_ids != list(packet.source_ids):
            raise ValueError("evidence packet source_ids do not match its evidence records")
        return packet


class ObservationLedger:
    """Idempotent, collision-safe storage for normalized provider batches.

    The ledger stores complete domain records rather than flattening values into
    a metric dictionary. Exact units, accounting periods, provider metadata, and
    evidence references therefore remain available to the orchestration layer.
    """

    def __init__(self, batches: Sequence[ObservationBatch] = ()) -> None:
        if isinstance(batches, (str, bytes)) or not isinstance(batches, Sequence):
            raise ValueError("batches must be a sequence of ObservationBatch values")
        self._observations: dict[str, FinancialObservation] = {}
        self._evidence: dict[str, EvidenceRecord] = {}
        self._issues: dict[tuple[str, str, str], NormalizationIssue] = {}
        self._by_entity_name: dict[tuple[str, str], set[str]] = {}
        self._by_entity_name_period: dict[tuple[str, str, FinancialPeriod], set[str]] = {}
        for batch in batches:
            self.ingest(batch)

    @property
    def observations(self) -> tuple[FinancialObservation, ...]:
        return tuple(sorted(self._observations.values(), key=_observation_sort_key))

    @property
    def evidence(self) -> tuple[EvidenceRecord, ...]:
        return tuple(sorted(self._evidence.values(), key=lambda record: record.evidence_id))

    @property
    def issues(self) -> tuple[NormalizationIssue, ...]:
        return tuple(sorted(self._issues.values(), key=_issue_sort_key))

    @property
    def observation_ids(self) -> tuple[str, ...]:
        return tuple(record.observation_id for record in self.observations)

    @property
    def evidence_ids(self) -> tuple[str, ...]:
        return tuple(record.evidence_id for record in self.evidence)

    def ingest(self, batch: ObservationBatch) -> None:
        """Atomically ingest a batch; equal IDs are idempotent, unequal IDs fail."""

        if not isinstance(batch, ObservationBatch):
            raise ValueError("batch must be an ObservationBatch")
        for record in sorted(batch.evidence, key=lambda item: item.evidence_id):
            existing = self._evidence.get(record.evidence_id)
            if existing is not None and existing != record:
                raise LedgerCollisionError("evidence", record.evidence_id)
        for record in sorted(batch.observations, key=lambda item: item.observation_id):
            existing = self._observations.get(record.observation_id)
            if existing is not None and existing != record:
                raise LedgerCollisionError("observation", record.observation_id)

        for record in batch.evidence:
            self._evidence.setdefault(record.evidence_id, record)
        for record in batch.observations:
            if record.observation_id in self._observations:
                continue
            self._observations[record.observation_id] = record
            self._by_entity_name.setdefault((record.entity_id, record.name), set()).add(record.observation_id)
            self._by_entity_name_period.setdefault((record.entity_id, record.name, record.period), set()).add(
                record.observation_id
            )
        for issue in batch.issues:
            self._issues.setdefault(_issue_sort_key(issue), issue)

    def get_observation(self, observation_id: str) -> FinancialObservation:
        _validate_lookup_id(observation_id, "observation_id")
        try:
            return self._observations[observation_id]
        except KeyError as exc:
            raise ValueError(f"unknown observation ID: {observation_id!r}") from exc

    def get_evidence(self, evidence_id: str) -> EvidenceRecord:
        _validate_lookup_id(evidence_id, "evidence_id")
        try:
            return self._evidence[evidence_id]
        except KeyError as exc:
            raise ValueError(f"unknown evidence ID: {evidence_id!r}") from exc

    def resolve_evidence(self, evidence_ids: Sequence[str]) -> tuple[EvidenceRecord, ...]:
        requested = _validated_id_sequence(evidence_ids, "evidence_ids")
        unknown = sorted(set(requested).difference(self._evidence))
        if unknown:
            raise ValueError(f"unknown evidence IDs: {unknown!r}")
        return tuple(self._evidence[evidence_id] for evidence_id in requested)

    def select(
        self,
        *,
        entity_id: str,
        name: str,
        period: FinancialPeriod | None = None,
    ) -> tuple[FinancialObservation, ...]:
        """Select an exact entity/name series or one exact accounting period."""

        _validate_lookup_id(entity_id, "entity_id")
        _validate_lookup_id(name, "name")
        if period is not None and not isinstance(period, FinancialPeriod):
            raise ValueError("period must be a FinancialPeriod or None")
        if period is None:
            observation_ids = self._by_entity_name.get((entity_id, name), set())
        else:
            observation_ids = self._by_entity_name_period.get((entity_id, name, period), set())
        return tuple(
            sorted(
                (self._observations[observation_id] for observation_id in observation_ids),
                key=_observation_sort_key,
            )
        )

    def evidence_packet(
        self,
        observation_ids: Sequence[str],
        *,
        max_observations: int = DEFAULT_PACKET_MAX_OBSERVATIONS,
        max_evidence: int = DEFAULT_PACKET_MAX_EVIDENCE,
        max_bytes: int = DEFAULT_PACKET_MAX_BYTES,
    ) -> EvidencePacket:
        """Build an exact bounded packet; limits raise instead of truncating lineage."""

        requested = _validated_id_sequence(observation_ids, "observation_ids")
        if not requested:
            raise ValueError("observation_ids must not be empty")
        _validate_packet_limit(
            max_observations,
            "max_observations",
            hard_maximum=MAX_PACKET_OBSERVATIONS,
        )
        _validate_packet_limit(max_evidence, "max_evidence", hard_maximum=MAX_PACKET_EVIDENCE)
        _validate_packet_limit(max_bytes, "max_bytes", hard_maximum=MAX_PACKET_BYTES)
        unknown = sorted(set(requested).difference(self._observations))
        if unknown:
            raise ValueError(f"unknown observation IDs: {unknown!r}")
        if len(requested) > max_observations:
            raise EvidencePacketLimitError(
                f"requested {len(requested)} observations exceeds max_observations={max_observations}"
            )
        observations = tuple(self._observations[observation_id] for observation_id in requested)
        source_ids = sorted({evidence_id for observation in observations for evidence_id in observation.evidence_ids})
        if len(source_ids) > max_evidence:
            raise EvidencePacketLimitError(
                f"packet requires {len(source_ids)} evidence records; max_evidence={max_evidence}"
            )
        packet = EvidencePacket(
            observations=observations,
            evidence=self.resolve_evidence(source_ids),
        )
        if packet.encoded_size_bytes > max_bytes:
            raise EvidencePacketLimitError(f"packet is {packet.encoded_size_bytes} bytes; max_bytes={max_bytes}")
        return packet

    def to_batch(self) -> ObservationBatch:
        return ObservationBatch(
            observations=self.observations,
            evidence=self.evidence,
            issues=self.issues,
        )

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "schema_version": LEDGER_SCHEMA_VERSION,
            **self.to_batch().to_dict(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ObservationLedger:
        if int(data.get("schema_version", -1)) != LEDGER_SCHEMA_VERSION:
            raise ValueError("unsupported observation ledger schema_version")
        return cls((ObservationBatch.from_dict(data),))


def _observation_sort_key(record: FinancialObservation) -> tuple[object, ...]:
    period = record.period
    return (
        record.entity_id,
        record.name,
        period.end,
        date.min if period.start is None else period.start,
        period.kind.value,
        -1 if period.fiscal_year is None else period.fiscal_year,
        "" if period.fiscal_period is None else period.fiscal_period,
        record.unit.kind.value,
        record.unit.symbol,
        record.unit.scale,
        record.observation_id,
    )


def _issue_sort_key(issue: NormalizationIssue) -> tuple[str, str, str]:
    return (issue.code, issue.provider_path, issue.message)


def _validated_id_sequence(value: Sequence[str], name: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ValueError(f"{name} must be a sequence of strings")
    requested = tuple(value)
    if any(not isinstance(item, str) or not item for item in requested):
        raise ValueError(f"{name} must contain only non-empty strings")
    if len(requested) != len(set(requested)):
        raise ValueError(f"{name} must not contain duplicates")
    return requested


def _validate_lookup_id(value: str, name: str) -> None:
    if not isinstance(value, str) or not value or value.strip() != value:
        raise ValueError(f"{name} must be a non-empty, unpadded string")


def _validate_packet_limit(value: int, name: str, *, hard_maximum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= hard_maximum:
        raise ValueError(f"{name} must be an integer between 1 and {hard_maximum}")


def _strict_json_bytes(value: JsonValue) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
