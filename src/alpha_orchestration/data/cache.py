"""Integrity-checked, content-addressed JSON cache for live provider payloads."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from alpha_orchestration.data.observations import canonical_content_hash
from alpha_orchestration.domain import JsonValue

CACHE_SCHEMA_VERSION = 1


class CacheIntegrityError(ValueError):
    """Raised when a cache reference or blob fails closed validation."""


@dataclass(frozen=True, slots=True)
class ProviderRequest:
    """Canonical identity for one provider request, independent of credentials."""

    provider: str
    operation: str
    identity: str
    parameters: dict[str, JsonValue]

    def __post_init__(self) -> None:
        for name in ("provider", "operation", "identity"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip() or len(value) > 200:
                raise ValueError(f"cache request {name} must contain between 1 and 200 characters")
            object.__setattr__(self, name, value.strip())
        object.__setattr__(self, "parameters", _json_object(self.parameters, "parameters"))

    @property
    def request_hash(self) -> str:
        return canonical_content_hash(self.to_dict())

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "provider": self.provider,
            "operation": self.operation,
            "identity": self.identity,
            "parameters": _json_object(self.parameters, "parameters"),
        }


@dataclass(frozen=True, slots=True)
class CacheRecord:
    request: ProviderRequest
    payload: JsonValue
    fetched_at: datetime
    content_hash: str
    fresh: bool


class ContentAddressedJsonCache:
    """Store immutable payload blobs and atomically updated request references."""

    def __init__(self, root: Path) -> None:
        if not isinstance(root, Path):
            raise ValueError("cache root must be a pathlib.Path")
        self.root = root

    def get(
        self,
        request: ProviderRequest,
        *,
        max_age: timedelta,
        now: datetime,
    ) -> CacheRecord | None:
        if max_age < timedelta(0):
            raise ValueError("cache max_age must not be negative")
        checked_now = _utc_datetime(now, "now")
        ref_path = self._ref_path(request)
        if not ref_path.exists():
            return None
        envelope = _read_object(ref_path, "cache reference")
        if envelope.get("schema_version") != CACHE_SCHEMA_VERSION:
            raise CacheIntegrityError("unsupported cache reference schema_version")
        if envelope.get("request_hash") != request.request_hash:
            raise CacheIntegrityError("cache reference request hash mismatch")
        if envelope.get("request") != request.to_dict():
            raise CacheIntegrityError("cache reference request mismatch")
        content_hash = envelope.get("content_hash")
        if not _is_digest(content_hash):
            raise CacheIntegrityError("cache reference content hash is invalid")
        raw_fetched_at = envelope.get("fetched_at")
        try:
            fetched_at = _utc_datetime(datetime.fromisoformat(str(raw_fetched_at)), "fetched_at")
        except (TypeError, ValueError) as exc:
            raise CacheIntegrityError("cache reference fetched_at is invalid") from exc
        blob_path = self._blob_path(content_hash)
        if not blob_path.exists():
            raise CacheIntegrityError("cache reference points to a missing content blob")
        payload = _read_json(blob_path, "cache blob")
        if canonical_content_hash(payload) != content_hash:
            raise CacheIntegrityError("cache content blob hash mismatch")
        return CacheRecord(
            request=request,
            payload=payload,
            fetched_at=fetched_at,
            content_hash=content_hash,
            fresh=checked_now - fetched_at <= max_age,
        )

    def put(
        self,
        request: ProviderRequest,
        payload: JsonValue,
        *,
        fetched_at: datetime,
    ) -> CacheRecord:
        checked_payload = _json_value(payload, "payload")
        checked_fetched_at = _utc_datetime(fetched_at, "fetched_at")
        content_hash = canonical_content_hash(checked_payload)
        blob_path = self._blob_path(content_hash)
        if blob_path.exists():
            existing = _read_json(blob_path, "cache blob")
            if canonical_content_hash(existing) != content_hash:
                raise CacheIntegrityError("existing cache content blob hash mismatch")
        else:
            _atomic_write_json(blob_path, checked_payload)
        envelope: dict[str, JsonValue] = {
            "schema_version": CACHE_SCHEMA_VERSION,
            "request": request.to_dict(),
            "request_hash": request.request_hash,
            "content_hash": content_hash,
            "fetched_at": checked_fetched_at.isoformat(),
        }
        _atomic_write_json(self._ref_path(request), envelope)
        return CacheRecord(request, checked_payload, checked_fetched_at, content_hash, True)

    def _ref_path(self, request: ProviderRequest) -> Path:
        return self.root / "refs" / f"{request.request_hash}.json"

    def _blob_path(self, content_hash: str) -> Path:
        return self.root / "blobs" / f"{content_hash}.json"


def _atomic_write_json(path: Path, value: JsonValue) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(_encoded(value), encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _read_object(path: Path, label: str) -> dict[str, Any]:
    value = _read_json(path, label)
    if not isinstance(value, dict):
        raise CacheIntegrityError(f"{label} must be a JSON object")
    return value


def _read_json(path: Path, label: str) -> JsonValue:
    try:
        return _json_value(json.loads(path.read_text(encoding="utf-8")), label)
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        if isinstance(exc, CacheIntegrityError):
            raise
        raise CacheIntegrityError(f"{label} is unreadable or invalid") from exc


def _json_object(value: object, label: str) -> dict[str, JsonValue]:
    checked = _json_value(value, label)
    if not isinstance(checked, dict):
        raise ValueError(f"{label} must be a JSON object")
    return checked


def _json_value(value: object, label: str) -> JsonValue:
    try:
        decoded = json.loads(_encoded(value))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{label} must contain only strict JSON values") from exc
    return decoded


def _encoded(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _utc_datetime(value: datetime, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware")
    return value.astimezone(UTC)


def _is_digest(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(character in "0123456789abcdef" for character in value)
