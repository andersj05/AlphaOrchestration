import asyncio

from alpha_orchestration.adapters.demo import DemoRuntime
from alpha_orchestration.domain import RunSpec, RunStatus
from alpha_orchestration.journal import MemoryJournal
from alpha_orchestration.tui.app import AlphaApp, MissionScreen, RunScreen


def test_mission_screen_mounts() -> None:
    async def exercise() -> None:
        app = AlphaApp()
        async with app.run_test(size=(120, 38)) as pilot:
            await pilot.pause()
            assert isinstance(app.screen, MissionScreen)

    asyncio.run(exercise())


def test_headless_demo_reaches_human_review() -> None:
    async def exercise() -> None:
        spec = RunSpec(run_id="run-tui")
        journal = MemoryJournal()
        app = AlphaApp(
            initial_spec=spec,
            runtime_factory=lambda _: DemoRuntime(0),
            journal_factory=lambda _: journal,
        )
        async with app.run_test(size=(150, 44)) as pilot:
            await pilot.pause()
            screen = app.screen
            assert isinstance(screen, RunScreen)
            await asyncio.wait_for(screen.completed.wait(), timeout=2)
            await pilot.pause()
            assert screen.controller.state.status is RunStatus.COMPLETE
            assert screen.controller.state.candidates
            assert journal.closed

    asyncio.run(exercise())
