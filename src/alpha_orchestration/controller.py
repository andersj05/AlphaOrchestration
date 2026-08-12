"""Run lifecycle, control actions, journaling, and state reduction."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from typing import Final

from alpha_orchestration.domain import EventKind, RunEvent, RunSpec, RunState, RunStatus, utc_now
from alpha_orchestration.ports import EventDraft, EventJournal, EventSubscriber, OrchestratorRuntime
from alpha_orchestration.reducer import reduce_event

EVENT_SCHEMA_VERSION: Final = 1


class RunController:
    """Own event ordering while treating the runtime as a replaceable producer."""

    def __init__(
        self,
        spec: RunSpec,
        runtime: OrchestratorRuntime,
        journal: EventJournal,
        subscribers: Iterable[EventSubscriber] = (),
    ) -> None:
        self.spec = spec
        self.runtime = runtime
        self.journal = journal
        self.state = RunState(spec=spec)
        self._subscribers = list(subscribers)
        self._gate = asyncio.Event()
        self._gate.set()
        self._publish_lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._cancel_requested = False
        self._journal_closed = False

    def subscribe(self, subscriber: EventSubscriber) -> None:
        self._subscribers.append(subscriber)

    async def _publish(self, draft: EventDraft) -> RunEvent:
        async with self._publish_lock:
            event = RunEvent(
                schema_version=EVENT_SCHEMA_VERSION,
                run_id=self.spec.run_id,
                sequence=self.state.last_sequence + 1,
                kind=draft.kind,
                timestamp=utc_now(),
                message=draft.message,
                agent_id=draft.agent_id,
                payload=draft.payload,
            )
            next_state = reduce_event(self.state, event)
            self.journal.append(event)
            self.state = next_state
            for subscriber in tuple(self._subscribers):
                subscriber(event)
            return event

    async def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("run controller has already started")
        await self._publish(
            EventDraft(
                EventKind.RUN_CREATED,
                f"Research mandate created for {self.spec.sector}",
                payload={"spec": self.spec.to_dict()},
            )
        )
        self._task = asyncio.create_task(self._drive(), name=f"alpha:{self.spec.run_id}")

    async def run(self) -> RunState:
        await self.start()
        return await self.wait()

    async def wait(self) -> RunState:
        if self._task is None:
            raise RuntimeError("run controller has not started")
        await self._task
        return self.state

    async def _drive(self) -> None:
        try:
            await self._publish(
                EventDraft(
                    EventKind.RUN_STARTED,
                    f"Admitted {self.spec.agent_budget} logical agents over {self.spec.active_slots} active slots",
                )
            )
            async for draft in self.runtime.stream(self.spec):
                await self._gate.wait()
                if self._cancel_requested:
                    break
                await self._publish(draft)
            if not self.state.terminal and not self._cancel_requested:
                await self._publish(
                    EventDraft(
                        EventKind.RUN_COMPLETED,
                        "Candidate triage is ready for human review",
                    )
                )
        except asyncio.CancelledError:
            if not self.state.terminal:
                await self._publish(EventDraft(EventKind.RUN_CANCELLED, "Run cancelled; pending work was drained"))
        except Exception as exc:
            if not self.state.terminal:
                await self._publish(
                    EventDraft(
                        EventKind.RUN_FAILED,
                        f"Run failed: {type(exc).__name__}",
                        payload={"error": str(exc)},
                    )
                )
        finally:
            self._close_journal()

    async def pause(self) -> None:
        if self.state.status not in {RunStatus.RUNNING, RunStatus.SYNTHESIZING}:
            return
        await self._publish(EventDraft(EventKind.RUN_PAUSED, "Run paused by operator"))
        self._gate.clear()

    async def resume(self) -> None:
        if self.state.status is not RunStatus.PAUSED:
            return
        self._gate.set()
        await self._publish(EventDraft(EventKind.RUN_RESUMED, "Run resumed by operator"))

    async def cancel(self) -> None:
        if self.state.terminal:
            return
        self._cancel_requested = True
        self._gate.set()
        if self._task is None:
            await self._publish(EventDraft(EventKind.RUN_CANCELLED, "Run cancelled"))
            self._close_journal()
            return
        self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)

    def _close_journal(self) -> None:
        if not self._journal_closed:
            self.journal.close()
            self._journal_closed = True
