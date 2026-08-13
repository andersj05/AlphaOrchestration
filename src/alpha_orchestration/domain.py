"""Provider-neutral domain contracts for research orchestration.

The objects in this module deliberately contain no Textual, SEC, yfinance, or
KernelCubed types.  Provider payloads are normalized at the edge so run state
can be replayed from its event journal without those dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from enum import StrEnum
from math import isfinite
from typing import Any
from uuid import uuid4

JsonValue = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]


def utc_now() -> datetime:
    return datetime.now(UTC)


def new_run_id() -> str:
    return f"run-{uuid4().hex[:10]}"


class ResearchDepth(StrEnum):
    QUICK = "quick"
    STANDARD = "standard"
    DEEP = "deep"


class RunStatus(StrEnum):
    IDLE = "idle"
    PLANNING = "planning"
    RUNNING = "running"
    PAUSED = "paused"
    SYNTHESIZING = "synthesizing"
    COMPLETE = "complete"
    CANCELLED = "cancelled"
    FAILED = "failed"


class AgentStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    WAITING_TOOL = "waiting_tool"
    COMPLETE = "complete"
    CANCELLED = "cancelled"
    FAILED = "failed"


class TaskStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    WAITING_MODEL = "waiting_model"
    WAITING_TOOL = "waiting_tool"
    COMPLETE = "complete"
    PARTIAL = "partial"
    FAILED = "failed"
    SKIPPED = "skipped"


class Stage(StrEnum):
    UNIVERSE = "universe"
    EVIDENCE = "evidence"
    ANALYSIS = "analysis"
    SYNTHESIS = "synthesis"
    REVIEW = "human_review"


STAGE_ORDER: tuple[Stage, ...] = (
    Stage.UNIVERSE,
    Stage.EVIDENCE,
    Stage.ANALYSIS,
    Stage.SYNTHESIS,
    Stage.REVIEW,
)


class EventKind(StrEnum):
    RUN_CREATED = "run_created"
    RUN_STARTED = "run_started"
    RUN_PAUSED = "run_paused"
    RUN_RESUMED = "run_resumed"
    RUN_SYNTHESIZING = "run_synthesizing"
    RUN_COMPLETED = "run_completed"
    RUN_CANCELLED = "run_cancelled"
    RUN_FAILED = "run_failed"
    STAGE_STARTED = "stage_started"
    STAGE_COMPLETED = "stage_completed"
    AGENT_REGISTERED = "agent_registered"
    AGENT_STARTED = "agent_started"
    AGENT_PROGRESS = "agent_progress"
    AGENT_COMPLETED = "agent_completed"
    AGENT_FAILED = "agent_failed"
    WORKFLOW_PLANNED = "workflow_planned"
    TASK_STARTED = "task_started"
    MODEL_TURN_STARTED = "model_turn_started"
    MODEL_TURN_COMPLETED = "model_turn_completed"
    ACTION_REJECTED = "action_rejected"
    TOOL_STARTED = "tool_started"
    TOOL_COMPLETED = "tool_completed"
    TOOL_REJECTED = "tool_rejected"
    TOOL_FAILED = "tool_failed"
    TASK_COMPLETED = "task_completed"
    TASK_FAILED = "task_failed"
    TASK_SKIPPED = "task_skipped"
    WORKFLOW_COMPLETED = "workflow_completed"
    EVIDENCE_ADDED = "evidence_added"
    CANDIDATE_UPDATED = "candidate_updated"


class CandidateBucket(StrEnum):
    ADVANCE = "advance"
    VALUATION_GATED = "valuation_gated"
    EXPOSURE_UNPROVEN = "exposure_unproven"
    DEPRIORITIZED = "deprioritized"

    @property
    def label(self) -> str:
        return {
            CandidateBucket.ADVANCE: "Advance to deeper work",
            CandidateBucket.VALUATION_GATED: "Valuation / expectations gated",
            CandidateBucket.EXPOSURE_UNPROVEN: "Exposure not yet proven",
            CandidateBucket.DEPRIORITIZED: "Deprioritized",
        }[self]


class CandidateConfidence(StrEnum):
    NOT_ASSESSED = "not_assessed"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class CandidateDataQuality(StrEnum):
    NOT_ASSESSED = "not_assessed"
    LIMITED = "limited"
    PARTIAL = "partial"
    COMPLETE = "complete"


class CandidateSourceMode(StrEnum):
    UNSPECIFIED = "unspecified"
    SYNTHETIC = "synthetic"
    LIVE = "live"
    MIXED = "mixed"


@dataclass(frozen=True, slots=True)
class CandidateFinancial:
    """One dated, source-linked financial observation used in candidate triage."""

    metric: str
    label: str
    value: float
    unit: str
    period: str
    source_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name in ("metric", "label", "unit", "period"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"candidate financial {name} must not be empty")
        if (
            isinstance(self.value, bool)
            or not isinstance(self.value, (int, float))
            or not isfinite(self.value)
        ):
            raise ValueError("candidate financial value must be finite")
        if any(not isinstance(source_id, str) or not source_id.strip() for source_id in self.source_ids):
            raise ValueError("candidate financial source IDs must not be empty")
        if len(self.source_ids) != len(set(self.source_ids)):
            raise ValueError("candidate financial source IDs must be unique")


@dataclass(frozen=True, slots=True)
class RunSpec:
    """A bounded research mandate.

    ``active_slots`` is engine concurrency, not the number of model replicas.
    The default mirrors KernelCubed's measured eight-session/four-slot shape.
    """

    sector: str = "Semiconductors"
    depth: ResearchDepth = ResearchDepth.STANDARD
    universe_size: int = 18
    agent_budget: int = 8
    active_slots: int = 4
    mode: str = "synthetic_demo"
    run_id: str = field(default_factory=new_run_id)

    def __post_init__(self) -> None:
        sector = self.sector.strip()
        if not sector:
            raise ValueError("sector must not be empty")
        if not 3 <= self.universe_size <= 500:
            raise ValueError("universe_size must be between 3 and 500")
        if not 1 <= self.agent_budget <= 8:
            raise ValueError("agent_budget must be between 1 and 8")
        if not 1 <= self.active_slots <= self.agent_budget:
            raise ValueError("active_slots must be between 1 and agent_budget")
        if not self.run_id.strip():
            raise ValueError("run_id must not be empty")
        object.__setattr__(self, "sector", sector)

    def restarted(self) -> RunSpec:
        return replace(self, run_id=new_run_id())

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "sector": self.sector,
            "depth": self.depth.value,
            "universe_size": self.universe_size,
            "agent_budget": self.agent_budget,
            "active_slots": self.active_slots,
            "mode": self.mode,
            "run_id": self.run_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RunSpec:
        return cls(
            sector=str(data["sector"]),
            depth=ResearchDepth(str(data["depth"])),
            universe_size=int(data["universe_size"]),
            agent_budget=int(data["agent_budget"]),
            active_slots=int(data["active_slots"]),
            mode=str(data["mode"]),
            run_id=str(data["run_id"]),
        )


@dataclass(frozen=True, slots=True)
class AgentState:
    agent_id: str
    role: str
    lane: str
    status: AgentStatus = AgentStatus.QUEUED
    progress: int = 0
    current_task: str = "Awaiting admission"
    tool_calls: int = 0
    evidence_count: int = 0


@dataclass(frozen=True, slots=True)
class TaskState:
    task_id: str
    agent_id: str
    depends_on: tuple[str, ...]
    required: bool
    allow_failed_dependencies: bool = False
    status: TaskStatus = TaskStatus.QUEUED
    turns: int = 0
    tool_calls: int = 0
    output: JsonValue = None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class Evidence:
    evidence_id: str
    title: str
    source: str
    source_kind: str
    summary: str
    observed_at: str
    synthetic: bool = False


@dataclass(frozen=True, slots=True)
class Candidate:
    candidate_id: str
    ticker: str
    company: str
    bucket: CandidateBucket
    priority_score: int
    variant_wedge: str
    why_now: str
    first_rejection: str
    investable_if: str
    kill_if: str
    next_workflow: str
    evidence_ids: tuple[str, ...] = ()
    financials: tuple[CandidateFinancial, ...] = ()
    confidence: CandidateConfidence = CandidateConfidence.NOT_ASSESSED
    data_quality: CandidateDataQuality = CandidateDataQuality.NOT_ASSESSED
    as_of: str = "not provided"
    source_mode: CandidateSourceMode = CandidateSourceMode.UNSPECIFIED
    evidence_gaps: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name in (
            "candidate_id",
            "ticker",
            "company",
            "variant_wedge",
            "why_now",
            "first_rejection",
            "investable_if",
            "kill_if",
            "next_workflow",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"candidate {name} must not be empty")
        if not 0 <= self.priority_score <= 100:
            raise ValueError("priority_score must be between 0 and 100")
        if any(not isinstance(source_id, str) or not source_id.strip() for source_id in self.evidence_ids):
            raise ValueError("candidate evidence IDs must not be empty")
        if len(self.evidence_ids) != len(set(self.evidence_ids)):
            raise ValueError("candidate evidence IDs must be unique")
        if not isinstance(self.as_of, str) or not self.as_of.strip():
            raise ValueError("candidate as_of must not be empty")
        if any(not isinstance(gap, str) or not gap.strip() for gap in self.evidence_gaps):
            raise ValueError("candidate evidence gaps must not be empty")
        financial_source_ids = {
            source_id for financial in self.financials for source_id in financial.source_ids
        }
        if not financial_source_ids.issubset(self.evidence_ids):
            raise ValueError("candidate financial sources must be present in evidence IDs")


@dataclass(frozen=True, slots=True)
class RunEvent:
    schema_version: int
    run_id: str
    sequence: int
    kind: EventKind
    timestamp: datetime
    message: str
    agent_id: str | None = None
    payload: dict[str, JsonValue] = field(default_factory=dict)

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "sequence": self.sequence,
            "kind": self.kind.value,
            "timestamp": self.timestamp.astimezone(UTC).isoformat(),
            "message": self.message,
            "agent_id": self.agent_id,
            "payload": self.payload,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RunEvent:
        return cls(
            schema_version=int(data["schema_version"]),
            run_id=str(data["run_id"]),
            sequence=int(data["sequence"]),
            kind=EventKind(str(data["kind"])),
            timestamp=datetime.fromisoformat(str(data["timestamp"])),
            message=str(data["message"]),
            agent_id=None if data.get("agent_id") is None else str(data["agent_id"]),
            payload=dict(data.get("payload", {})),
        )


@dataclass(frozen=True, slots=True)
class RunState:
    spec: RunSpec
    status: RunStatus = RunStatus.IDLE
    current_stage: Stage | None = None
    completed_stages: tuple[Stage, ...] = ()
    progress: int = 0
    agents: dict[str, AgentState] = field(default_factory=dict)
    workflow_id: str | None = None
    workflow_version: str | None = None
    tasks: dict[str, TaskState] = field(default_factory=dict)
    evidence: dict[str, Evidence] = field(default_factory=dict)
    candidates: dict[str, Candidate] = field(default_factory=dict)
    recent_events: tuple[RunEvent, ...] = ()
    last_sequence: int = -1
    failure: str | None = None

    @property
    def terminal(self) -> bool:
        return self.status in {
            RunStatus.COMPLETE,
            RunStatus.CANCELLED,
            RunStatus.FAILED,
        }

    @property
    def active_agents(self) -> int:
        return sum(agent.status in {AgentStatus.RUNNING, AgentStatus.WAITING_TOOL} for agent in self.agents.values())

    @property
    def complete_agents(self) -> int:
        return sum(agent.status is AgentStatus.COMPLETE for agent in self.agents.values())


class StateInvariantError(RuntimeError):
    """Raised when an append-only event stream cannot be replayed safely."""
