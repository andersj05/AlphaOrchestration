from datetime import UTC, datetime

import pytest

from alpha_orchestration.domain import (
    CandidateBucket,
    CandidateConfidence,
    CandidateDataQuality,
    CandidateSourceMode,
    EventKind,
    RunEvent,
    RunSpec,
    RunState,
    RunStatus,
    StateInvariantError,
)
from alpha_orchestration.reducer import reduce_event


def event(
    run_spec: RunSpec,
    sequence: int,
    kind: EventKind,
    *,
    agent_id: str | None = None,
    **payload,
):
    return RunEvent(
        schema_version=1,
        run_id=run_spec.run_id,
        sequence=sequence,
        kind=kind,
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        message=kind.value,
        agent_id=agent_id,
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
    assert candidate.financials == ()
    assert candidate.confidence is CandidateConfidence.NOT_ASSESSED
    assert candidate.data_quality is CandidateDataQuality.NOT_ASSESSED
    assert candidate.source_mode is CandidateSourceMode.UNSPECIFIED
    assert candidate.as_of == "not provided"
    assert candidate.evidence_gaps == ()


def test_candidate_triage_parses_financial_provenance_and_quality() -> None:
    spec = RunSpec(run_id="run-candidate-financials")
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
            financials=[
                {
                    "metric": "revenue_growth",
                    "label": "Revenue growth",
                    "value": 0.25,
                    "unit": "ratio",
                    "period": "FY2025 vs FY2024",
                    "source_ids": ["ev-1"],
                }
            ],
            confidence="medium",
            data_quality="partial",
            as_of="2025-12-31",
            source_mode="synthetic",
            evidence_gaps=["Customer concentration"],
        ),
    )

    candidate = state.candidates["candidate-1"]
    assert candidate.financials[0].value == 0.25
    assert candidate.financials[0].source_ids == ("ev-1",)
    assert candidate.confidence is CandidateConfidence.MEDIUM
    assert candidate.data_quality is CandidateDataQuality.PARTIAL
    assert candidate.source_mode is CandidateSourceMode.SYNTHETIC
    assert candidate.evidence_gaps == ("Customer concentration",)


def test_candidate_financial_sources_must_be_linked_as_evidence() -> None:
    spec = RunSpec(run_id="run-candidate-provenance")

    state = reduce_event(
        RunState(spec=spec),
        event(spec, 0, EventKind.RUN_CREATED, spec=spec.to_dict()),
    )
    payload = {
        "candidate_id": "candidate-1",
        "ticker": "SYN-X",
        "company": "Synthetic Systems",
        "bucket": CandidateBucket.ADVANCE.value,
        "priority_score": 82,
        "variant_wedge": "Testable wedge",
        "why_now": "Dated proof point",
        "first_rejection": "Demand may be pulled forward",
        "investable_if": "Cash conversion confirms the signal",
        "kill_if": "Cancellations rise",
        "next_workflow": "company_tearsheet",
        "evidence_ids": ["ev-1"],
        "financials": [
            {
                "metric": "revenue",
                "label": "Revenue",
                "value": 1,
                "unit": "USD",
                "period": "FY2025",
                "source_ids": ["ev-2"],
            }
        ],
    }
    with pytest.raises(StateInvariantError, match="financial sources"):
        reduce_event(state, event(spec, 1, EventKind.CANDIDATE_UPDATED, **payload))


def test_evidence_preserves_optional_live_provenance_and_legacy_defaults() -> None:
    spec = RunSpec(run_id="run-evidence-provenance")
    state = reduce_event(
        RunState(spec=spec),
        event(spec, 0, EventKind.RUN_CREATED, spec=spec.to_dict()),
    )
    state = reduce_event(
        state,
        event(
            spec,
            1,
            EventKind.AGENT_REGISTERED,
            agent_id="analyst",
            role="Issuer analyst",
            lane="ALP",
        ),
    )
    state = reduce_event(
        state,
        event(
            spec,
            2,
            EventKind.EVIDENCE_ADDED,
            agent_id="analyst",
            evidence_id="ev-live",
            title="ALP annual filing",
            source="SEC",
            source_kind="company_facts",
            summary="Live SEC record",
            observed_at="2024-12-31T00:00:00+00:00",
            retrieved_at="2026-08-13T12:00:00+00:00",
            source_url="https://www.sec.gov/Archives/edgar/data/1/alp-2024.htm",
        ),
    )
    state = reduce_event(
        state,
        event(
            spec,
            3,
            EventKind.EVIDENCE_ADDED,
            agent_id="analyst",
            evidence_id="ev-legacy",
            title="Legacy evidence",
            source="Fixture",
            source_kind="legacy",
            summary="Old journal shape",
            observed_at="fixture-v1",
            synthetic=True,
        ),
    )

    live = state.evidence["ev-live"]
    assert live.retrieved_at == "2026-08-13T12:00:00+00:00"
    assert live.source_url == "https://www.sec.gov/Archives/edgar/data/1/alp-2024.htm"
    legacy = state.evidence["ev-legacy"]
    assert legacy.retrieved_at == "not provided"
    assert legacy.source_url == ""
