from datetime import UTC, datetime

import pytest

from alpha_orchestration.domain import (
    CandidateBucket,
    EventKind,
    RunEvent,
    RunSpec,
    RunState,
    RunStatus,
    StateInvariantError,
)
from alpha_orchestration.reducer import reduce_event


def event(run_spec: RunSpec, sequence: int, kind: EventKind, **payload):
    return RunEvent(
        schema_version=1,
        run_id=run_spec.run_id,
        sequence=sequence,
        kind=kind,
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        message=kind.value,
        payload=payload,
    )


def test_reducer_rejects_sequence_gaps() -> None:
    spec = RunSpec(run_id="run-sequence")
    state = RunState(spec=spec)

    with pytest.raises(StateInvariantError, match="expected 0"):
        reduce_event(state, event(spec, 1, EventKind.RUN_CREATED, spec=spec.to_dict()))


def test_terminal_state_rejects_later_events() -> None:
    spec = RunSpec(run_id="run-terminal")
    state = RunState(spec=spec)
    state = reduce_event(state, event(spec, 0, EventKind.RUN_CREATED, spec=spec.to_dict()))
    state = reduce_event(state, event(spec, 1, EventKind.RUN_COMPLETED))

    assert state.status is RunStatus.COMPLETE
    with pytest.raises(StateInvariantError, match="terminal"):
        reduce_event(state, event(spec, 2, EventKind.RUN_PAUSED))


def test_candidate_triage_preserves_rejection_and_kill_tests() -> None:
    spec = RunSpec(run_id="run-candidate")
    state = reduce_event(
        RunState(spec=spec),
        event(spec, 0, EventKind.RUN_CREATED, spec=spec.to_dict()),
    )
    state = reduce_event(
        state,
        event(
            spec,
            1,
            EventKind.CANDIDATE_UPDATED,
            candidate_id="candidate-1",
            ticker="SYN-X",
            company="Synthetic Systems",
            bucket=CandidateBucket.ADVANCE.value,
            priority_score=82,
            variant_wedge="Testable wedge",
            why_now="Dated proof point",
            first_rejection="Demand may be pulled forward",
            investable_if="Cash conversion confirms the signal",
            kill_if="Cancellations rise",
            next_workflow="company_tearsheet",
            evidence_ids=["ev-1"],
        ),
    )

    candidate = state.candidates["candidate-1"]
    assert candidate.bucket is CandidateBucket.ADVANCE
    assert candidate.first_rejection == "Demand may be pulled forward"
    assert candidate.kill_if == "Cancellations rise"
