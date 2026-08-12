import asyncio
from pathlib import Path

from alpha_orchestration.adapters.demo import DemoRuntime, build_demo_events
from alpha_orchestration.controller import RunController
from alpha_orchestration.domain import CandidateBucket, RunSpec, RunStatus
from alpha_orchestration.journal import JsonlJournal, MemoryJournal, replay


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
    assert state.candidates["demo:alpx"].bucket is CandidateBucket.ADVANCE
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
