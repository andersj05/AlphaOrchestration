from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path

import pytest

from alpha_orchestration.domain import EventKind, JsonValue, RunSpec, RunStatus, TaskStatus
from alpha_orchestration.journal import load_events, replay

RUN_ID = "run-integrity"
TIMESTAMP = "2026-08-12T12:00:00+00:00"
EMPTY_SCHEMA: dict[str, JsonValue] = {
    "type": "object",
    "properties": {},
    "additionalProperties": False,
}


def canonical_hash(value: object) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def text_hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def event(
    kind: EventKind,
    payload: dict[str, object],
    sequence: int = 0,
    *,
    agent_id: str | None = None,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "run_id": RUN_ID,
        "sequence": sequence,
        "kind": kind.value,
        "timestamp": TIMESTAMP,
        "message": kind.value,
        "agent_id": agent_id,
        "payload": payload,
    }


def write_events(path: Path, events: list[dict[str, object]]) -> None:
    lines = [json.dumps(item, separators=(",", ":"), allow_nan=False) for item in events]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def assert_invalid(path: Path, fragment: str, line: int = 1) -> None:
    with pytest.raises(ValueError) as caught:
        load_events(path)
    assert f":{line}:" in str(caught.value)
    assert fragment in str(caught.value)


def plan() -> dict[str, object]:
    task = {
        "task_id": "task",
        "agent_id": "agent",
        "depends_on": [],
        "allowed_tools": [],
        "prompt_key": "default",
        "output_schema": deepcopy(EMPTY_SCHEMA),
        "required": True,
        "allow_failed_dependencies": False,
        "max_turns": 1,
        "max_tool_calls": 0,
        "max_calls_per_turn": 1,
        "max_new_tokens": 64,
        "max_action_bytes": 1024,
        "repair_budget": 0,
    }
    return {
        "workflow_id": "workflow",
        "version": "1.0.0",
        "active_slots": 1,
        "tasks": [task],
    }


def planned_payload() -> dict[str, object]:
    exact_plan = plan()
    return {
        "workflow_id": "workflow",
        "workflow_version": "1.0.0",
        "plan": exact_plan,
        "plan_hash": canonical_hash(exact_plan),
        "tasks": deepcopy(exact_plan["tasks"]),
        "effective_active_slots": 1,
        "actual_active_slots": 1,
        "active_slots": 1,
    }


def result_payload(kind: EventKind) -> dict[str, object]:
    result: dict[str, object] = {
        "ok": kind is EventKind.TOOL_COMPLETED,
        "tool": "finance.calculate",
    }
    if kind is EventKind.TOOL_REJECTED:
        result["error"] = {"code": "invalid_schema", "message": "bad"}
    envelope = {
        "call_id": "call",
        "payload": deepcopy(result),
        "source_ids": ["evidence"],
        "retryable": False,
    }
    return {
        "task_id": "task",
        "call_id": "call",
        "result": deepcopy(result),
        "source_ids": ["evidence"],
        "retryable": False,
        "error": deepcopy(result.get("error")),
        "result_envelope": envelope,
        "result_hash": canonical_hash(envelope),
    }


def model_completed_payload() -> dict[str, object]:
    output = '{"kind":"final","payload":{}}'
    trace = {
        "request_id": "request",
        "output_text": output,
        "output_hash": text_hash(output),
        "output_truncated": False,
    }
    return {
        "task_id": "task",
        "output": output,
        "output_hash": text_hash(output),
        "output_truncated": False,
        "trace": trace,
        "trace_hash": canonical_hash(trace),
    }


def test_load_rejects_duplicate_keys_at_any_depth(tmp_path: Path) -> None:
    path = tmp_path / "duplicate.jsonl"
    path.write_text(
        '{"schema_version":1,"run_id":"run-integrity","sequence":0,'
        '"kind":"run_created","timestamp":"2026-08-12T12:00:00+00:00",'
        '"message":"created","agent_id":null,'
        '"payload":{"nested":{"same":1,"same":2}}}\n',
        encoding="utf-8",
    )
    assert_invalid(path, "duplicate JSON key 'same'")


@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity"])
def test_load_rejects_non_finite_json(tmp_path: Path, constant: str) -> None:
    path = tmp_path / f"{constant}.jsonl"
    path.write_text(
        '{"schema_version":1,"run_id":"run-integrity","sequence":0,'
        '"kind":"run_created","timestamp":"2026-08-12T12:00:00+00:00",'
        '"message":"created","agent_id":null,'
        f'"payload":{{"value":{constant}}}}}\n',
        encoding="utf-8",
    )
    assert_invalid(path, f"non-finite JSON constant is not allowed: {constant}")


def test_request_hash_tampering_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "request.jsonl"
    request = {"prompt": "original"}
    payload = {"request": request, "request_hash": canonical_hash(request)}
    request["prompt"] = "tampered"
    write_events(path, [event(EventKind.MODEL_TURN_STARTED, payload)])
    assert_invalid(path, "request_hash mismatch for request")


def test_output_and_trace_hash_tampering_is_rejected(tmp_path: Path) -> None:
    output_path = tmp_path / "output.jsonl"
    output_payload = model_completed_payload()
    output_payload["output"] = "tampered"
    write_events(output_path, [event(EventKind.MODEL_TURN_COMPLETED, output_payload)])
    assert_invalid(output_path, "model_turn_completed output_hash mismatch")

    trace_path = tmp_path / "trace.jsonl"
    trace_payload = model_completed_payload()
    trace_payload["trace"]["telemetry"] = {"tampered": True}
    write_events(trace_path, [event(EventKind.MODEL_TURN_COMPLETED, trace_payload)])
    assert_invalid(trace_path, "trace_hash mismatch for trace")


def test_argument_hashes_detect_tampering(tmp_path: Path) -> None:
    resolved_path = tmp_path / "arguments.jsonl"
    arguments = {"values": {"revenue": 100}}
    payload = {"task_id": "task", "arguments": arguments, "arguments_hash": canonical_hash(arguments)}
    arguments["values"]["revenue"] = 999
    write_events(resolved_path, [event(EventKind.TOOL_STARTED, payload)])
    assert_invalid(resolved_path, "arguments_hash mismatch for arguments")

    proposed_path = tmp_path / "proposed.jsonl"
    arguments = {"base_value": 100}
    proposed = {"base_value": {"observation_id": "obs:one"}}
    payload = {
        "task_id": "task",
        "arguments": arguments,
        "arguments_hash": canonical_hash(arguments),
        "proposed_arguments": proposed,
        "proposed_arguments_hash": canonical_hash(proposed),
    }
    proposed["base_value"] = {"observation_id": "obs:two"}
    write_events(proposed_path, [event(EventKind.TOOL_STARTED, payload)])
    assert_invalid(proposed_path, "proposed_arguments_hash mismatch for proposed_arguments")


@pytest.mark.parametrize("kind", [EventKind.TOOL_COMPLETED, EventKind.TOOL_REJECTED])
def test_result_hash_tampering_is_rejected(tmp_path: Path, kind: EventKind) -> None:
    path = tmp_path / f"{kind.value}.jsonl"
    payload = result_payload(kind)
    payload["result_envelope"]["payload"]["tool"] = "tampered"
    write_events(path, [event(kind, payload)])
    assert_invalid(path, "tool result_hash mismatch")


def test_hashed_result_cannot_mask_tampered_split_fields(tmp_path: Path) -> None:
    path = tmp_path / "split-result.jsonl"
    payload = result_payload(EventKind.TOOL_COMPLETED)
    payload["result"] = {"ok": True, "tool": "finance.calculate", "data": {"tampered": True}}
    write_events(path, [event(EventKind.TOOL_COMPLETED, payload)])
    assert_invalid(path, "tool result_envelope payload does not match payload")


def test_plan_hash_and_completion_identity_are_verified(tmp_path: Path) -> None:
    plan_path = tmp_path / "plan.jsonl"
    payload = planned_payload()
    payload["plan"]["version"] = "tampered"
    write_events(plan_path, [event(EventKind.WORKFLOW_PLANNED, payload)])
    assert_invalid(plan_path, "plan_hash mismatch for plan")

    completed_path = tmp_path / "completed.jsonl"
    completed = {
        "workflow_id": "workflow",
        "workflow_version": "1.0.0",
        "plan_hash": "0" * 64,
    }
    write_events(
        completed_path,
        [
            event(EventKind.WORKFLOW_PLANNED, planned_payload()),
            event(EventKind.WORKFLOW_COMPLETED, completed, 1),
        ],
    )
    assert_invalid(
        completed_path,
        "workflow_completed identity or plan_hash does not match workflow_planned",
        2,
    )


def test_new_plan_requires_result_envelope(tmp_path: Path) -> None:
    path = tmp_path / "missing-envelope.jsonl"
    payload = result_payload(EventKind.TOOL_COMPLETED)
    del payload["result_envelope"]
    write_events(
        path,
        [
            event(EventKind.WORKFLOW_PLANNED, planned_payload()),
            event(EventKind.TOOL_COMPLETED, payload, 1),
        ],
    )
    assert_invalid(path, "fixed-DAG tool result is missing result_envelope", 2)


def test_missing_lifecycle_hash_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "missing-hash.jsonl"
    write_events(path, [event(EventKind.MODEL_TURN_STARTED, {"request": {}})])
    assert_invalid(path, "request payload request_hash must be a lowercase SHA-256 digest")


def test_legacy_unhashed_journal_still_replays(tmp_path: Path) -> None:
    path = tmp_path / "legacy.jsonl"
    spec = RunSpec(run_id=RUN_ID)
    write_events(
        path,
        [
            event(EventKind.RUN_CREATED, {"spec": spec.to_dict()}),
            event(EventKind.RUN_STARTED, {}, 1),
            event(EventKind.RUN_COMPLETED, {}, 2),
        ],
    )
    restored = replay(path)
    assert restored.status is RunStatus.COMPLETE
    assert restored.last_sequence == 2


def test_legacy_unhashed_demo_tool_event_still_loads(tmp_path: Path) -> None:
    path = tmp_path / "legacy-tool.jsonl"
    write_events(
        path,
        [event(EventKind.TOOL_STARTED, {"tool": "sec.company_facts"})],
    )

    loaded = load_events(path)

    assert loaded[0].kind is EventKind.TOOL_STARTED


def test_valid_fixed_dag_journal_replays_purely(tmp_path: Path) -> None:
    path = tmp_path / "fixed.jsonl"
    spec = RunSpec(run_id=RUN_ID)
    request = {"task_id": "task", "request_id": "request", "prompt": "bounded"}
    completed_model = model_completed_payload()
    completed_model["request_id"] = "request"
    arguments = {"operations": []}
    completed_tool = result_payload(EventKind.TOOL_COMPLETED)
    completed_workflow = {
        "workflow_id": "workflow",
        "workflow_version": "1.0.0",
        "plan_hash": planned_payload()["plan_hash"],
        "counts": {"complete": 1},
        "required_failures": [],
        "partial": False,
    }
    write_events(
        path,
        [
            event(EventKind.RUN_CREATED, {"spec": spec.to_dict()}),
            event(EventKind.RUN_STARTED, {}, 1),
            event(
                EventKind.AGENT_REGISTERED,
                {"role": "agent", "lane": "fixed_dag"},
                2,
                agent_id="agent",
            ),
            event(EventKind.WORKFLOW_PLANNED, planned_payload(), 3),
            event(EventKind.TASK_STARTED, {"task_id": "task"}, 4, agent_id="agent"),
            event(
                EventKind.MODEL_TURN_STARTED,
                {
                    "task_id": "task",
                    "request": request,
                    "request_hash": canonical_hash(request),
                },
                5,
                agent_id="agent",
            ),
            event(
                EventKind.MODEL_TURN_COMPLETED,
                completed_model,
                6,
                agent_id="agent",
            ),
            event(
                EventKind.TOOL_STARTED,
                {
                    "task_id": "task",
                    "arguments": arguments,
                    "arguments_hash": canonical_hash(arguments),
                },
                7,
                agent_id="agent",
            ),
            event(EventKind.TOOL_COMPLETED, completed_tool, 8, agent_id="agent"),
            event(
                EventKind.TASK_COMPLETED,
                {"task_id": "task", "output": {}, "partial": False},
                9,
                agent_id="agent",
            ),
            event(EventKind.AGENT_COMPLETED, {}, 10, agent_id="agent"),
            event(EventKind.WORKFLOW_COMPLETED, completed_workflow, 11),
            event(EventKind.RUN_COMPLETED, {}, 12),
        ],
    )

    restored = replay(path)

    assert restored.status is RunStatus.COMPLETE
    assert restored.tasks["task"].status is TaskStatus.COMPLETE
    assert restored.tasks["task"].tool_calls == 1
    assert restored.last_sequence == 12
