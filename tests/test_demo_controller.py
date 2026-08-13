import asyncio
from pathlib import Path

from alpha_orchestration.adapters.demo import DemoRuntime, build_demo_events
from alpha_orchestration.controller import RunController
from alpha_orchestration.domain import (
    CandidateBucket,
    CandidateConfidence,
    CandidateDataQuality,
    CandidateSourceMode,
    EventKind,
    RunSpec,
    RunStatus,
)
from alpha_orchestration.journal import JsonlJournal, MemoryJournal, replay
from alpha_orchestration.ports import EventDraft


def test_demo_is_deterministic_and_finishes_as_triage() -> None:
    async def run():
        spec = RunSpec(run_id="run-demo")
        journal = MemoryJournal()
        controller = RunController(spec, DemoRuntime(0), journal)
        state = await controller.run()
        return state, journal

    state, journal = asyncio.run(run())

    assert state.status is RunStatus.COMPLETE
    assert state.progress == 100
    assert len(state.agents) == 8
    assert len(state.evidence) == 7
    assert len(state.candidates) == 3
    candidate = state.candidates["demo:alpx"]
    assert candidate.bucket is CandidateBucket.ADVANCE
    assert len(candidate.financials) == 4
    assert candidate.confidence is CandidateConfidence.MEDIUM
    assert candidate.data_quality is CandidateDataQuality.PARTIAL
    assert candidate.source_mode is CandidateSourceMode.SYNTHETIC
    assert candidate.evidence_gaps
    financial_sources = {source_id for item in candidate.financials for source_id in item.source_ids}
    assert financial_sources <= set(candidate.evidence_ids)
    assert "recommendation" in state.agents["lead"].current_task
    assert [item.sequence for item in journal.events] == list(range(len(journal.events)))
    assert len(build_demo_events(state.spec)) == len(build_demo_events(state.spec))


def test_jsonl_journal_replays_to_equivalent_state(tmp_path: Path) -> None:
    async def run(path: Path):
        spec = RunSpec(run_id="run-replay")
        controller = RunController(spec, DemoRuntime(0), JsonlJournal(path))
        return await controller.run()

    path = tmp_path / "events.jsonl"
    original = asyncio.run(run(path))
    restored = replay(path)

    assert restored.status == original.status
    assert restored.last_sequence == original.last_sequence
    assert restored.evidence == original.evidence
    assert restored.candidates == original.candidates


def test_pause_and_resume_are_journaled() -> None:
    async def run():
        spec = RunSpec(run_id="run-controls")
        journal = MemoryJournal()
        controller = RunController(spec, DemoRuntime(0.005), journal)
        await controller.start()
        await asyncio.sleep(0.02)
        await controller.pause()
        paused_sequence = controller.state.last_sequence
        await asyncio.sleep(0.02)
        assert controller.state.last_sequence == paused_sequence
        await controller.resume()
        state = await controller.wait()
        return state, journal

    state, journal = asyncio.run(run())

    assert state.status is RunStatus.COMPLETE
    kinds = [event.kind.value for event in journal.events]
    assert "run_paused" in kinds
    assert "run_resumed" in kinds


def test_invalid_runtime_draft_never_corrupts_the_journal(tmp_path: Path) -> None:
    class InvalidRuntime:
        async def stream(self, spec: RunSpec):  # type: ignore[no-untyped-def]
            del spec
            yield EventDraft(
                EventKind.TASK_STARTED,
                "invalid task start",
                agent_id="unknown",
                payload={"task_id": "missing"},
            )

    async def run(path: Path):
        controller = RunController(
            RunSpec(run_id="run-invalid-draft"),
            InvalidRuntime(),
            JsonlJournal(path),
        )
        return await controller.run()

    path = tmp_path / "invalid-draft.jsonl"
    state = asyncio.run(run(path))
    restored = replay(path)

    assert state.status is RunStatus.FAILED
    assert restored == state
    assert restored.last_sequence == 2
