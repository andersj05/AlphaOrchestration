from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from alpha_orchestration.domain import (
    AgentStatus,
    EventKind,
    RunEvent,
    RunSpec,
    RunState,
    StateInvariantError,
    TaskStatus,
)
from alpha_orchestration.reducer import reduce_event


def _event(
    run_spec: RunSpec,
    sequence: int,
    kind: EventKind,
    *,
    agent_id: str | None = None,
    message: str | None = None,
    **payload: Any,
) -> RunEvent:
    return RunEvent(
        schema_version=1,
        run_id=run_spec.run_id,
        sequence=sequence,
        kind=kind,
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        message=message or kind.value,
        agent_id=agent_id,
        payload=payload,
    )


def _append(
    state: RunState,
    kind: EventKind,
    *,
    agent_id: str | None = None,
    message: str | None = None,
    **payload: Any,
) -> RunState:
    return reduce_event(
        state,
        _event(
            state.spec,
            state.last_sequence + 1,
            kind,
            agent_id=agent_id,
            message=message,
            **payload,
        ),
    )


def _planned_state() -> RunState:
    spec = RunSpec(run_id="run-task-events")
    state = _append(RunState(spec=spec), EventKind.RUN_CREATED, spec=spec.to_dict())
    state = _append(
        state,
        EventKind.AGENT_REGISTERED,
        agent_id="researcher",
        role="research",
        lane="analysis",
    )
    return _append(
        state,
        EventKind.WORKFLOW_PLANNED,
        workflow_id="company-research",
        workflow_version="1.0",
        tasks=[
            {
                "task_id": "collect",
                "agent_id": "researcher",
                "depends_on": [],
                "required": True,
            },
            {
                "task_id": "synthesize",
                "agent_id": "researcher",
                "depends_on": ["collect"],
                "required": True,
            },
        ],
    )


def test_table_driven_task_lifecycle_projection() -> None:
    state = _planned_state()
    steps = [
        (EventKind.TASK_STARTED, {}, TaskStatus.RUNNING, 0, 0),
        (EventKind.MODEL_TURN_STARTED, {}, TaskStatus.WAITING_MODEL, 1, 0),
        (
            EventKind.MODEL_TURN_COMPLETED,
            {"output": {"action": "calculate"}},
            TaskStatus.RUNNING,
            1,
            0,
        ),
        (EventKind.TOOL_STARTED, {}, TaskStatus.WAITING_TOOL, 1, 1),
        (EventKind.TOOL_COMPLETED, {}, TaskStatus.RUNNING, 1, 1),
        (
            EventKind.TASK_COMPLETED,
            {"output": {"value": 42}},
            TaskStatus.COMPLETE,
            1,
            1,
        ),
    ]

    for kind, payload, status, turns, tool_calls in steps:
        state = _append(
            state,
            kind,
            agent_id="researcher",
            task_id="collect",
            **payload,
        )
        task = state.tasks["collect"]
        assert (task.status, task.turns, task.tool_calls) == (
            status,
            turns,
            tool_calls,
        )

    assert state.tasks["collect"].output == {"value": 42}
    assert state.agents["researcher"].tool_calls == 1

    state = _append(
        state,
        EventKind.TASK_STARTED,
        agent_id="researcher",
        task_id="synthesize",
    )
    state = _append(
        state,
        EventKind.TASK_COMPLETED,
        agent_id="researcher",
        task_id="synthesize",
        partial=True,
        output={"summary": "degraded"},
        error="one optional source unavailable",
    )
    state = _append(state, EventKind.WORKFLOW_COMPLETED)

    assert state.workflow_id == "company-research"
    assert state.workflow_version == "1.0"
    assert state.tasks["synthesize"].status is TaskStatus.PARTIAL
    assert state.tasks["synthesize"].error == "one optional source unavailable"


@pytest.mark.parametrize("failure_kind", [EventKind.TOOL_REJECTED, EventKind.TOOL_FAILED])
def test_tool_failures_release_task_and_agent_before_task_failure(
    failure_kind: EventKind,
) -> None:
    state = _planned_state()
    state = _append(
        state,
        EventKind.TASK_STARTED,
        agent_id="researcher",
        task_id="collect",
    )
    state = _append(
        state,
        EventKind.TOOL_STARTED,
        agent_id="researcher",
        task_id="collect",
    )
    state = _append(
        state,
        failure_kind,
        agent_id="researcher",
        task_id="collect",
        error="provider unavailable",
    )

    task = state.tasks["collect"]
    assert task.status is TaskStatus.RUNNING
    assert task.error == "provider unavailable"
    assert task.tool_calls == 1
    assert state.agents["researcher"].status is AgentStatus.RUNNING

    state = _append(
        state,
        EventKind.TASK_FAILED,
        agent_id="researcher",
        task_id="collect",
        error="retry budget exhausted",
    )
    state = _append(
        state,
        EventKind.TASK_SKIPPED,
        agent_id="researcher",
        task_id="synthesize",
        reason="dependency failed",
    )
    state = _append(state, EventKind.WORKFLOW_COMPLETED)

    assert state.tasks["collect"].status is TaskStatus.FAILED
    assert state.tasks["synthesize"].status is TaskStatus.SKIPPED
    assert state.tasks["synthesize"].error == "dependency failed"


@pytest.mark.parametrize(
    ("tasks", "match"),
    [
        (
            [
                {
                    "task_id": "same",
                    "agent_id": "a",
                    "depends_on": [],
                    "required": True,
                },
                {
                    "task_id": "same",
                    "agent_id": "b",
                    "depends_on": [],
                    "required": False,
                },
            ],
            "duplicate task",
        ),
        (
            [
                {
                    "task_id": "orphan",
                    "agent_id": "a",
                    "depends_on": ["missing"],
                    "required": True,
                }
            ],
            "unknown dependencies",
        ),
        (
            [
                {
                    "task_id": "left",
                    "agent_id": "a",
                    "depends_on": ["right"],
                    "required": True,
                },
                {
                    "task_id": "right",
                    "agent_id": "a",
                    "depends_on": ["left"],
                    "required": True,
                },
            ],
            "cycle",
        ),
    ],
)
def test_workflow_plan_rejects_invalid_graphs(
    tasks: list[dict[str, Any]],
    match: str,
) -> None:
    spec = RunSpec(run_id=f"run-invalid-{match}")
    state = _append(RunState(spec=spec), EventKind.RUN_CREATED, spec=spec.to_dict())

    with pytest.raises(StateInvariantError, match=match):
        _append(
            state,
            EventKind.WORKFLOW_PLANNED,
            workflow_id="invalid",
            workflow_version="1",
            tasks=tasks,
        )


@pytest.mark.parametrize(
    ("kind", "task_id", "agent_id", "match"),
    [
        (EventKind.TASK_STARTED, "missing", "researcher", "unknown task"),
        (
            EventKind.TASK_COMPLETED,
            "collect",
            "researcher",
            "invalid task_completed transition",
        ),
        (
            EventKind.TASK_STARTED,
            "synthesize",
            "researcher",
            "before dependencies completed",
        ),
        (
            EventKind.TASK_STARTED,
            "collect",
            "different-agent",
            "does not own task",
        ),
    ],
)
def test_reducer_rejects_unknown_tasks_and_invalid_transitions(
    kind: EventKind,
    task_id: str,
    agent_id: str,
    match: str,
) -> None:
    state = _planned_state()

    with pytest.raises(StateInvariantError, match=match):
        _append(state, kind, task_id=task_id, agent_id=agent_id)


def test_duplicate_workflow_and_early_completion_are_rejected() -> None:
    state = _planned_state()

    with pytest.raises(StateInvariantError, match="already planned"):
        _append(
            state,
            EventKind.WORKFLOW_PLANNED,
            workflow_id="second",
            workflow_version="2",
            tasks=[
                {
                    "task_id": "replacement",
                    "agent_id": "researcher",
                    "depends_on": [],
                    "required": True,
                }
            ],
        )

    with pytest.raises(StateInvariantError, match="non-terminal tasks"):
        _append(state, EventKind.WORKFLOW_COMPLETED)


def test_replay_is_deterministic_and_recent_event_window_is_bounded() -> None:
    spec = RunSpec(run_id="run-replay")
    events = [
        _event(spec, 0, EventKind.RUN_CREATED, spec=spec.to_dict()),
        _event(
            spec,
            1,
            EventKind.WORKFLOW_PLANNED,
            workflow_id="replay",
            workflow_version="1",
            tasks=[
                {
                    "task_id": "loop",
                    "agent_id": "agent",
                    "depends_on": [],
                    "required": True,
                }
            ],
        ),
        _event(spec, 2, EventKind.TASK_STARTED, task_id="loop", agent_id="agent"),
    ]
    sequence = 3
    for turn in range(45):
        events.append(
            _event(
                spec,
                sequence,
                EventKind.MODEL_TURN_STARTED,
                task_id="loop",
                agent_id="agent",
            )
        )
        sequence += 1
        events.append(
            _event(
                spec,
                sequence,
                EventKind.MODEL_TURN_COMPLETED,
                task_id="loop",
                agent_id="agent",
                output={"turn": turn},
            )
        )
        sequence += 1
    events.extend(
        [
            _event(
                spec,
                sequence,
                EventKind.TASK_COMPLETED,
                task_id="loop",
                agent_id="agent",
                output={"answer": 45},
            ),
            _event(spec, sequence + 1, EventKind.WORKFLOW_COMPLETED),
        ]
    )

    first = RunState(spec=spec)
    second = RunState(spec=spec)
    for event in events:
        first = reduce_event(first, event)
        second = reduce_event(second, event)

    assert first == second
    assert first.tasks["loop"].turns == 45
    assert first.tasks["loop"].output == {"answer": 45}
    assert len(first.recent_events) == 80
    assert first.recent_events[-1].kind is EventKind.WORKFLOW_COMPLETED


def test_taskless_tool_events_preserve_demo_agent_projection() -> None:
    spec = RunSpec(run_id="run-legacy-tools")
    state = _append(RunState(spec=spec), EventKind.RUN_CREATED, spec=spec.to_dict())
    state = _append(
        state,
        EventKind.AGENT_REGISTERED,
        agent_id="legacy",
        role="demo",
        lane="demo",
    )
    state = _append(state, EventKind.AGENT_STARTED, agent_id="legacy")
    state = _append(state, EventKind.TOOL_STARTED, agent_id="legacy")
    assert state.agents["legacy"].status is AgentStatus.WAITING_TOOL
    assert state.agents["legacy"].tool_calls == 1

    state = _append(state, EventKind.TOOL_COMPLETED, agent_id="legacy")
    assert state.agents["legacy"].status is AgentStatus.RUNNING
    assert state.tasks == {}
