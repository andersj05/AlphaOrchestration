"""Bounded, citation-checked optional model diligence for automatic research."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from alpha_orchestration.actions import ActionParseError, FinalAction, parse_action
from alpha_orchestration.domain import JsonValue
from alpha_orchestration.ports import ActionModel, ActionModelRequest, ActionModelResult
from alpha_orchestration.tools.schema import SchemaValidationError, validate_json

MAX_ACTION_BYTES = 8_192
MAX_OUTPUT_IDS = 4_096
MAX_PROMPT_IDS = 8_192
MAX_TELEMETRY_BYTES = 16_384
MAX_TOKEN_ID = 2**31 - 1

DILIGENCE_OUTPUT_SCHEMA: dict[str, JsonValue] = {
    "type": "object",
    "required": ["summary", "risks", "questions", "source_ids"],
    "properties": {
        "summary": {"type": "string", "minLength": 1, "maxLength": 2_000},
        "risks": {
            "type": "array",
            "maxItems": 5,
            "items": {"type": "string", "minLength": 1, "maxLength": 500},
        },
        "questions": {
            "type": "array",
            "maxItems": 5,
            "items": {"type": "string", "minLength": 1, "maxLength": 500},
        },
        "source_ids": {
            "type": "array",
            "minItems": 1,
            "maxItems": 50,
            "uniqueItems": True,
            "items": {"type": "string", "minLength": 1},
        },
    },
    "additionalProperties": False,
}


@dataclass(frozen=True, slots=True)
class DiligenceJob:
    ticker: str
    task_id: str
    agent_id: str
    request: ActionModelRequest


@dataclass(frozen=True, slots=True)
class DiligenceResult:
    job: DiligenceJob
    trace: Mapping[str, JsonValue]
    output: Mapping[str, JsonValue] | None
    error_code: str | None
    error: str | None


class BoundedDiligenceRunner:
    """Run one tool-free model turn per job without sharing a lane concurrently."""

    def __init__(
        self,
        model: ActionModel,
        *,
        slots: int = 4,
        timeout_seconds: float = 60.0,
    ) -> None:
        if not 1 <= slots <= 8:
            raise ValueError("diligence slots must be between 1 and 8")
        if not 0 < timeout_seconds <= 120:
            raise ValueError("diligence timeout must be in (0, 120]")
        self.model = model
        self.slots = slots
        self.timeout_seconds = timeout_seconds

    async def run(self, jobs: Sequence[DiligenceJob]) -> tuple[tuple[DiligenceResult, ...], int]:
        semaphore = asyncio.Semaphore(self.slots)
        lane_locks = {job.agent_id: asyncio.Lock() for job in jobs}
        counter_lock = asyncio.Lock()
        active = 0
        peak = 0

        async def execute(job: DiligenceJob) -> DiligenceResult:
            nonlocal active, peak
            async with lane_locks[job.agent_id], semaphore:
                async with counter_lock:
                    active += 1
                    peak = max(peak, active)
                try:
                    try:
                        result = await asyncio.wait_for(
                            self.model.complete(job.request),
                            timeout=self.timeout_seconds,
                        )
                    except Exception as exc:
                        code = "model_timeout" if isinstance(exc, TimeoutError) else "model_error"
                        return DiligenceResult(
                            job,
                            error_trace(job.request.request_id, code),
                            None,
                            code,
                            f"{code}: {type(exc).__name__}",
                        )
                    return validate_result(job, result)
                finally:
                    async with counter_lock:
                        active -= 1

        return tuple(await asyncio.gather(*(execute(job) for job in jobs))), peak


def validate_result(job: DiligenceJob, result: object) -> DiligenceResult:
    error = _model_result_error(result, job.request.request_id)
    if error is not None:
        code, message = error
        return DiligenceResult(job, error_trace(job.request.request_id, code), None, code, message)
    assert isinstance(result, ActionModelResult)
    trace = model_trace(result)
    try:
        action = parse_action(result.output_text, max_bytes=MAX_ACTION_BYTES, max_calls=1)
    except ActionParseError as exc:
        return DiligenceResult(job, trace, None, exc.code.value, str(exc))
    if not isinstance(action, FinalAction):
        return DiligenceResult(
            job,
            trace,
            None,
            "diligence_tool_calls_disallowed",
            "automatic diligence cannot propose tool calls",
        )
    try:
        validate_json(action.payload, DILIGENCE_OUTPUT_SCHEMA)
    except (SchemaValidationError, ValueError, TypeError, OverflowError) as exc:
        return DiligenceResult(job, trace, None, "invalid_diligence_output", str(exc))
    raw_sources = action.payload.get("source_ids")
    if not isinstance(raw_sources, list):
        return DiligenceResult(job, trace, None, "invalid_citations", "source_ids must be a list")
    cited = tuple(str(source_id) for source_id in raw_sources)
    unknown = sorted(set(cited) - set(job.request.allowed_source_ids))
    if unknown:
        return DiligenceResult(
            job,
            trace,
            None,
            "source_not_allowed",
            f"diligence output cites untrusted source IDs: {unknown!r}",
        )
    return DiligenceResult(job, trace, dict(action.payload), None, None)


def _model_result_error(result: object, request_id: str) -> tuple[str, str] | None:
    if not isinstance(result, ActionModelResult):
        return "invalid_model_result", "model result must be ActionModelResult"
    if result.request_id != request_id:
        return "request_id_mismatch", "model result request_id does not match the request"
    if not isinstance(result.output_text, str):
        return "invalid_model_result", "model output must be text"
    try:
        output_bytes = len(result.output_text.encode("utf-8"))
    except UnicodeEncodeError:
        return "invalid_model_result", "model output must be UTF-8 text"
    if output_bytes > MAX_ACTION_BYTES:
        return "action_too_large", f"model output exceeds {MAX_ACTION_BYTES} bytes"
    if result.finish_reason not in {"stop", "eos"}:
        return "non_natural_finish", "model result did not end with a natural stop"
    for name, values, limit in (
        ("prompt_ids", result.prompt_ids, MAX_PROMPT_IDS),
        ("output_ids", result.output_ids, MAX_OUTPUT_IDS),
    ):
        if len(values) > limit:
            return "invalid_model_trace", f"{name} exceeds its bounded trace limit"
        if any(
            isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= MAX_TOKEN_ID for value in values
        ):
            return "invalid_model_trace", f"{name} contains an invalid token ID"
    try:
        telemetry = json.dumps(result.telemetry, allow_nan=False, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError, OverflowError):
        return "invalid_model_trace", "model telemetry must be strict JSON"
    if len(telemetry.encode("utf-8")) > MAX_TELEMETRY_BYTES:
        return "invalid_model_trace", "model telemetry exceeds its bounded trace limit"
    for label in (result.model_fingerprint, result.tokenizer_fingerprint):
        if not isinstance(label, str) or not label or len(label) > 500:
            return "invalid_model_trace", "model and tokenizer fingerprints must be bounded strings"
    return None


def model_trace(result: ActionModelResult) -> dict[str, JsonValue]:
    encoded = result.output_text.encode("utf-8")
    return {
        "request_id": result.request_id,
        "output_text": result.output_text,
        "output_bytes": len(encoded),
        "output_hash": hashlib.sha256(encoded).hexdigest(),
        "output_truncated": False,
        "prompt_ids": list(result.prompt_ids),
        "output_ids": list(result.output_ids),
        "finish_reason": result.finish_reason,
        "telemetry": strict_json(result.telemetry),
        "model_fingerprint": result.model_fingerprint,
        "tokenizer_fingerprint": result.tokenizer_fingerprint,
    }


def error_trace(request_id: str, code: str) -> dict[str, JsonValue]:
    return {
        "request_id": request_id,
        "output_text": "",
        "output_bytes": 0,
        "output_hash": hashlib.sha256(b"").hexdigest(),
        "output_truncated": False,
        "prompt_ids": [],
        "output_ids": [],
        "finish_reason": code,
        "telemetry": {"error_code": code},
        "model_fingerprint": "unavailable",
        "tokenizer_fingerprint": "unavailable",
    }


def strict_json(value: Any) -> JsonValue:
    return json.loads(json.dumps(value, allow_nan=False, sort_keys=True, separators=(",", ":")))


def canonical_hash(value: Any) -> str:
    encoded = json.dumps(value, allow_nan=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
