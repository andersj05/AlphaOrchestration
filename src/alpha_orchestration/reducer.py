"""Pure event-to-state reduction with strict sequence checks."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from alpha_orchestration.domain import (
    AgentState,
    AgentStatus,
    Candidate,
    CandidateBucket,
    EventKind,
    Evidence,
    RunEvent,
    RunState,
    RunStatus,
    Stage,
    StateInvariantError,
)


def _require(payload: dict[str, Any], key: str) -> Any:
    if key not in payload:
        raise StateInvariantError(f"event payload is missing {key!r}")
    return payload[key]


def _agent(state: RunState, event: RunEvent) -> AgentState:
    if event.agent_id is None:
        raise StateInvariantError(f"{event.kind.value} requires agent_id")
    try:
        return state.agents[event.agent_id]
    except KeyError as exc:
        raise StateInvariantError(f"unknown agent: {event.agent_id}") from exc


def reduce_event(state: RunState, event: RunEvent) -> RunState:
    """Return the next immutable run state for one verified event."""

    if event.schema_version != 1:
        raise StateInvariantError(f"unsupported event schema: {event.schema_version}")
    if event.run_id != state.spec.run_id:
        raise StateInvariantError(f"event run_id {event.run_id!r} does not match {state.spec.run_id!r}")
    expected_sequence = state.last_sequence + 1
    if event.sequence != expected_sequence:
        raise StateInvariantError(f"event sequence {event.sequence} does not match expected {expected_sequence}")
    if state.terminal:
        raise StateInvariantError("cannot append an event after terminal run state")

    next_state = replace(
        state,
        last_sequence=event.sequence,
        recent_events=(*state.recent_events[-79:], event),
    )
    payload: dict[str, Any] = event.payload

    if event.kind is EventKind.RUN_CREATED:
        return replace(next_state, status=RunStatus.PLANNING)
    if event.kind is EventKind.RUN_STARTED:
        return replace(next_state, status=RunStatus.RUNNING)
    if event.kind is EventKind.RUN_PAUSED:
        return replace(next_state, status=RunStatus.PAUSED)
    if event.kind is EventKind.RUN_RESUMED:
        return replace(next_state, status=RunStatus.RUNNING)
    if event.kind is EventKind.RUN_SYNTHESIZING:
        return replace(
            next_state,
            status=RunStatus.SYNTHESIZING,
            current_stage=Stage.SYNTHESIS,
            progress=max(next_state.progress, int(payload.get("progress", 82))),
        )
    if event.kind is EventKind.RUN_COMPLETED:
        return replace(
            next_state,
            status=RunStatus.COMPLETE,
            current_stage=Stage.REVIEW,
            completed_stages=tuple(dict.fromkeys((*state.completed_stages, Stage.SYNTHESIS))),
            progress=100,
        )
    if event.kind is EventKind.RUN_CANCELLED:
        agents = {
            key: replace(agent, status=AgentStatus.CANCELLED)
            if agent.status in {AgentStatus.QUEUED, AgentStatus.RUNNING, AgentStatus.WAITING_TOOL}
            else agent
            for key, agent in state.agents.items()
        }
        return replace(next_state, status=RunStatus.CANCELLED, agents=agents)
    if event.kind is EventKind.RUN_FAILED:
        return replace(
            next_state,
            status=RunStatus.FAILED,
            failure=str(payload.get("error", event.message)),
        )
    if event.kind is EventKind.STAGE_STARTED:
        return replace(
            next_state,
            current_stage=Stage(str(_require(payload, "stage"))),
            progress=max(0, min(100, int(payload.get("progress", state.progress)))),
        )
    if event.kind is EventKind.STAGE_COMPLETED:
        stage = Stage(str(_require(payload, "stage")))
        completed = tuple(dict.fromkeys((*state.completed_stages, stage)))
        return replace(
            next_state,
            completed_stages=completed,
            progress=max(0, min(100, int(payload.get("progress", state.progress)))),
        )
    if event.kind is EventKind.AGENT_REGISTERED:
        if event.agent_id is None:
            raise StateInvariantError("agent_registered requires agent_id")
        if event.agent_id in state.agents:
            raise StateInvariantError(f"agent already registered: {event.agent_id}")
        agents = dict(state.agents)
        agents[event.agent_id] = AgentState(
            agent_id=event.agent_id,
            role=str(_require(payload, "role")),
            lane=str(_require(payload, "lane")),
        )
        return replace(next_state, agents=agents)

    if event.kind in {
        EventKind.AGENT_STARTED,
        EventKind.AGENT_PROGRESS,
        EventKind.AGENT_COMPLETED,
        EventKind.AGENT_FAILED,
        EventKind.TOOL_STARTED,
        EventKind.TOOL_COMPLETED,
        EventKind.EVIDENCE_ADDED,
    }:
        current = _agent(state, event)
        agents = dict(state.agents)
        if event.kind is EventKind.AGENT_STARTED:
            agents[current.agent_id] = replace(
                current,
                status=AgentStatus.RUNNING,
                current_task=event.message,
                progress=max(current.progress, int(payload.get("progress", 5))),
            )
        elif event.kind is EventKind.AGENT_PROGRESS:
            agents[current.agent_id] = replace(
                current,
                status=AgentStatus.RUNNING,
                current_task=event.message,
                progress=max(0, min(100, int(payload.get("progress", current.progress)))),
            )
        elif event.kind is EventKind.AGENT_COMPLETED:
            agents[current.agent_id] = replace(
                current,
                status=AgentStatus.COMPLETE,
                current_task=event.message,
                progress=100,
            )
        elif event.kind is EventKind.AGENT_FAILED:
            agents[current.agent_id] = replace(
                current,
                status=AgentStatus.FAILED,
                current_task=event.message,
            )
        elif event.kind is EventKind.TOOL_STARTED:
            agents[current.agent_id] = replace(
                current,
                status=AgentStatus.WAITING_TOOL,
                current_task=event.message,
                tool_calls=current.tool_calls + 1,
            )
        elif event.kind is EventKind.TOOL_COMPLETED:
            agents[current.agent_id] = replace(
                current,
                status=AgentStatus.RUNNING,
                current_task=event.message,
            )
        else:
            evidence = Evidence(
                evidence_id=str(_require(payload, "evidence_id")),
                title=str(_require(payload, "title")),
                source=str(_require(payload, "source")),
                source_kind=str(_require(payload, "source_kind")),
                summary=str(_require(payload, "summary")),
                observed_at=str(_require(payload, "observed_at")),
                synthetic=bool(payload.get("synthetic", False)),
            )
            evidence_map = dict(state.evidence)
            evidence_map[evidence.evidence_id] = evidence
            agents[current.agent_id] = replace(
                current,
                evidence_count=current.evidence_count + 1,
                current_task=event.message,
            )
            return replace(next_state, agents=agents, evidence=evidence_map)
        return replace(next_state, agents=agents)

    if event.kind is EventKind.CANDIDATE_UPDATED:
        candidate = Candidate(
            candidate_id=str(_require(payload, "candidate_id")),
            ticker=str(_require(payload, "ticker")),
            company=str(_require(payload, "company")),
            bucket=CandidateBucket(str(_require(payload, "bucket"))),
            priority_score=int(_require(payload, "priority_score")),
            variant_wedge=str(_require(payload, "variant_wedge")),
            why_now=str(_require(payload, "why_now")),
            first_rejection=str(_require(payload, "first_rejection")),
            investable_if=str(_require(payload, "investable_if")),
            kill_if=str(_require(payload, "kill_if")),
            next_workflow=str(_require(payload, "next_workflow")),
            evidence_ids=tuple(str(value) for value in payload.get("evidence_ids", [])),
        )
        candidates = dict(state.candidates)
        candidates[candidate.candidate_id] = candidate
        return replace(next_state, candidates=candidates)

    raise StateInvariantError(f"unhandled event kind: {event.kind.value}")
