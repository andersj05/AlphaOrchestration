"""Deterministic, offline runtime used by the first terminal prototype."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from alpha_orchestration.domain import (
    CandidateBucket,
    CandidateConfidence,
    CandidateDataQuality,
    CandidateSourceMode,
    EventKind,
    RunSpec,
    Stage,
)
from alpha_orchestration.ports import EventDraft


@dataclass(frozen=True, slots=True)
class AgentBlueprint:
    agent_id: str
    role: str
    lane: str


AGENT_BLUEPRINTS: tuple[AgentBlueprint, ...] = (
    AgentBlueprint("universe", "Universe Mapper", "Coverage"),
    AgentBlueprint("filings", "Filing Analyst", "SEC evidence"),
    AgentBlueprint("market", "Market Data Analyst", "Price + profile"),
    AgentBlueprint("fundamentals", "Fundamentals Forecaster", "Operating model"),
    AgentBlueprint("valuation", "Valuation Analyst", "Expectations"),
    AgentBlueprint("catalysts", "Catalyst Analyst", "Why now"),
    AgentBlueprint("risk", "Risk Skeptic", "Falsification"),
    AgentBlueprint("lead", "Synthesis Lead", "Candidate triage"),
)


def _evidence(
    agent_id: str,
    evidence_id: str,
    title: str,
    source: str,
    summary: str,
) -> EventDraft:
    return EventDraft(
        EventKind.EVIDENCE_ADDED,
        f"Recorded evidence: {title}",
        agent_id=agent_id,
        payload={
            "evidence_id": evidence_id,
            "title": title,
            "source": source,
            "source_kind": "synthetic_fixture",
            "summary": summary,
            "observed_at": "fixture-v1",
            "synthetic": True,
        },
    )


def _candidate(
    *,
    candidate_id: str,
    ticker: str,
    company: str,
    bucket: CandidateBucket,
    score: int,
    variant_wedge: str,
    why_now: str,
    first_rejection: str,
    investable_if: str,
    kill_if: str,
    next_workflow: str,
    evidence_ids: list[str],
    financials: list[dict[str, object]],
    confidence: CandidateConfidence,
    data_quality: CandidateDataQuality,
    as_of: str,
    evidence_gaps: list[str],
) -> EventDraft:
    return EventDraft(
        EventKind.CANDIDATE_UPDATED,
        f"Triaged {ticker} into {bucket.label.lower()}",
        agent_id="lead",
        payload={
            "candidate_id": candidate_id,
            "ticker": ticker,
            "company": company,
            "bucket": bucket.value,
            "priority_score": score,
            "variant_wedge": variant_wedge,
            "why_now": why_now,
            "first_rejection": first_rejection,
            "investable_if": investable_if,
            "kill_if": kill_if,
            "next_workflow": next_workflow,
            "evidence_ids": evidence_ids,
            "financials": financials,
            "confidence": confidence.value,
            "data_quality": data_quality.value,
            "as_of": as_of,
            "source_mode": CandidateSourceMode.SYNTHETIC.value,
            "evidence_gaps": evidence_gaps,
        },
    )


def build_demo_events(spec: RunSpec) -> tuple[EventDraft, ...]:
    """Build the same evidence-rich event sequence for a given mandate."""

    blueprints = AGENT_BLUEPRINTS[: spec.agent_budget]
    available = {blueprint.agent_id for blueprint in blueprints}
    events: list[EventDraft] = []

    for blueprint in blueprints:
        events.append(
            EventDraft(
                EventKind.AGENT_REGISTERED,
                f"Registered {blueprint.role}",
                agent_id=blueprint.agent_id,
                payload={"role": blueprint.role, "lane": blueprint.lane},
            )
        )

    def add(agent_id: str, event: EventDraft) -> None:
        if agent_id in available:
            events.append(event)

    events.append(
        EventDraft(
            EventKind.STAGE_STARTED,
            "Defining a bounded research universe",
            payload={"stage": Stage.UNIVERSE.value, "progress": 3},
        )
    )
    add(
        "universe",
        EventDraft(
            EventKind.AGENT_STARTED,
            f"Mapping {spec.sector} into comparable operating groups",
            agent_id="universe",
        ),
    )
    add(
        "universe",
        EventDraft(
            EventKind.TOOL_STARTED,
            "Calling demo.universe_map",
            agent_id="universe",
            payload={"tool": "demo.universe_map"},
        ),
    )
    add(
        "universe",
        EventDraft(
            EventKind.TOOL_COMPLETED,
            f"Normalized {spec.universe_size} synthetic issuers",
            agent_id="universe",
            payload={"tool": "demo.universe_map", "rows": spec.universe_size},
        ),
    )
    add(
        "universe",
        _evidence(
            "universe",
            "demo:universe:v1",
            "Comparable universe normalized",
            "Synthetic universe fixture",
            "Issuer identifiers, sub-sector pathways, and liquidity gates were normalized.",
        ),
    )
    add(
        "universe",
        EventDraft(
            EventKind.AGENT_COMPLETED,
            "Universe map handed to evidence lanes",
            agent_id="universe",
        ),
    )
    events.append(
        EventDraft(
            EventKind.STAGE_COMPLETED,
            "Universe locked",
            payload={"stage": Stage.UNIVERSE.value, "progress": 18},
        )
    )

    events.append(
        EventDraft(
            EventKind.STAGE_STARTED,
            "Collecting filings, fundamentals, and market context",
            payload={"stage": Stage.EVIDENCE.value, "progress": 20},
        )
    )
    for agent_id, message in (
        ("filings", "Scanning synthetic filing metadata and XBRL facts"),
        ("market", "Building dated market snapshots"),
        ("fundamentals", "Normalizing operating drivers and units"),
    ):
        add(
            agent_id,
            EventDraft(EventKind.AGENT_STARTED, message, agent_id=agent_id),
        )
    add(
        "filings",
        EventDraft(
            EventKind.TOOL_STARTED,
            "Calling demo.sec_submissions",
            agent_id="filings",
            payload={"tool": "demo.sec_submissions"},
        ),
    )
    add(
        "market",
        EventDraft(
            EventKind.TOOL_STARTED,
            "Calling demo.market_history",
            agent_id="market",
            payload={"tool": "demo.market_history"},
        ),
    )
    add(
        "filings",
        EventDraft(
            EventKind.TOOL_COMPLETED,
            "Parsed 42 synthetic filing records",
            agent_id="filings",
            payload={"tool": "demo.sec_submissions", "records": 42},
        ),
    )
    add(
        "filings",
        _evidence(
            "filings",
            "demo:filings:v1",
            "Disclosure change map",
            "Synthetic SEC submissions fixture",
            "Three issuers showed material changes in backlog and customer-concentration language.",
        ),
    )
    add(
        "market",
        EventDraft(
            EventKind.TOOL_COMPLETED,
            "Aligned price history to filing dates",
            agent_id="market",
            payload={"tool": "demo.market_history", "records": 18},
        ),
    )
    add(
        "market",
        _evidence(
            "market",
            "demo:market:v1",
            "Expectations dispersion map",
            "Synthetic market fixture",
            "The strongest exposure signals were not uniformly reflected in relative valuation.",
        ),
    )
    add(
        "fundamentals",
        EventDraft(
            EventKind.TOOL_STARTED,
            "Calling demo.company_facts",
            agent_id="fundamentals",
            payload={"tool": "demo.company_facts"},
        ),
    )
    add(
        "fundamentals",
        EventDraft(
            EventKind.TOOL_COMPLETED,
            "Reconciled periods, units, and fiscal calendars",
            agent_id="fundamentals",
            payload={"tool": "demo.company_facts", "metrics": 96},
        ),
    )
    add(
        "fundamentals",
        _evidence(
            "fundamentals",
            "demo:fundamentals:v1",
            "Operating leverage screen",
            "Synthetic company-facts fixture",
            "Two issuers retained positive incremental margin patterns under the base scenario.",
        ),
    )
    for agent_id, message in (
        ("filings", "Filing evidence packet complete"),
        ("market", "Market context packet complete"),
        ("fundamentals", "Normalized fundamentals packet complete"),
    ):
        add(
            agent_id,
            EventDraft(EventKind.AGENT_COMPLETED, message, agent_id=agent_id),
        )
    events.append(
        EventDraft(
            EventKind.STAGE_COMPLETED,
            "Evidence packets are source-linked and ready for challenge",
            payload={"stage": Stage.EVIDENCE.value, "progress": 53},
        )
    )

    events.append(
        EventDraft(
            EventKind.STAGE_STARTED,
            "Testing valuation, catalysts, and first-rejection risks",
            payload={"stage": Stage.ANALYSIS.value, "progress": 56},
        )
    )
    for agent_id, message in (
        ("valuation", "Testing what the synthetic price path appears to discount"),
        ("catalysts", "Mapping dated proof points and trigger windows"),
        ("risk", "Attempting to reject every surviving candidate"),
    ):
        add(agent_id, EventDraft(EventKind.AGENT_STARTED, message, agent_id=agent_id))
    add(
        "valuation",
        EventDraft(
            EventKind.AGENT_PROGRESS,
            "Separated quality signal from multiple expansion",
            agent_id="valuation",
            payload={"progress": 64},
        ),
    )
    add(
        "valuation",
        _evidence(
            "valuation",
            "demo:valuation:v1",
            "Valuation gate",
            "Synthetic calculation fixture",
            "One high-quality screen result remains expectations-heavy and is gated from advancement.",
        ),
    )
    add(
        "catalysts",
        EventDraft(
            EventKind.AGENT_PROGRESS,
            "Separated confirmed windows from inferred catalysts",
            agent_id="catalysts",
            payload={"progress": 72},
        ),
    )
    add(
        "catalysts",
        _evidence(
            "catalysts",
            "demo:catalyst:v1",
            "Proof-point calendar",
            "Synthetic events fixture",
            "The next two synthetic reporting windows can test backlog conversion and margin durability.",
        ),
    )
    add(
        "risk",
        EventDraft(
            EventKind.AGENT_PROGRESS,
            "Downgraded one name for unproven theme attribution",
            agent_id="risk",
            payload={"progress": 78},
        ),
    )
    add(
        "risk",
        _evidence(
            "risk",
            "demo:risk:v1",
            "First-rejection ledger",
            "Synthetic challenge fixture",
            "Customer concentration, working-capital reversal, and exposure attribution are the leading gaps.",
        ),
    )
    for agent_id, message in (
        ("valuation", "Expectations review complete"),
        ("catalysts", "Catalyst review complete"),
        ("risk", "Independent challenge complete"),
    ):
        add(agent_id, EventDraft(EventKind.AGENT_COMPLETED, message, agent_id=agent_id))
    events.append(
        EventDraft(
            EventKind.STAGE_COMPLETED,
            "Analysis lanes reconciled",
            payload={"stage": Stage.ANALYSIS.value, "progress": 81},
        )
    )

    events.append(
        EventDraft(
            EventKind.RUN_SYNTHESIZING,
            "Fanning evidence into a constrained candidate schema",
            payload={"progress": 84},
        )
    )
    add(
        "lead",
        EventDraft(
            EventKind.AGENT_STARTED,
            "Reconciling support, objections, and next workflows",
            agent_id="lead",
        ),
    )
    add(
        "lead",
        _candidate(
            candidate_id="demo:alpx",
            ticker="ALP-X",
            company="Alpine Logic Systems · synthetic",
            bucket=CandidateBucket.ADVANCE,
            score=86,
            variant_wedge="Backlog conversion may create more operating leverage than the synthetic base case assumes.",
            why_now="A fresh disclosure change and a dated conversion window create a falsifiable next step.",
            first_rejection="The backlog signal may reflect customer pull-forward rather than durable demand.",
            investable_if="Two reporting periods confirm conversion, cash realization, and stable concentration.",
            kill_if="Cancellations rise or incremental margins reverse before conversion is visible.",
            next_workflow="company_tearsheet",
            evidence_ids=[
                "demo:filings:v1",
                "demo:fundamentals:v1",
                "demo:market:v1",
                "demo:valuation:v1",
                "demo:catalyst:v1",
            ],
            financials=[
                {
                    "metric": "revenue",
                    "label": "Revenue",
                    "value": 1280.0,
                    "unit": "USD millions",
                    "period": "FY2025 fixture",
                    "source_ids": ["demo:fundamentals:v1"],
                },
                {
                    "metric": "revenue_growth",
                    "label": "Revenue growth",
                    "value": 0.24,
                    "unit": "ratio",
                    "period": "FY2025 vs FY2024 fixture",
                    "source_ids": ["demo:fundamentals:v1"],
                },
                {
                    "metric": "operating_margin",
                    "label": "Operating margin",
                    "value": 0.18,
                    "unit": "ratio",
                    "period": "FY2025 fixture",
                    "source_ids": ["demo:fundamentals:v1"],
                },
                {
                    "metric": "enterprise_value_to_revenue",
                    "label": "EV / revenue",
                    "value": 4.8,
                    "unit": "x",
                    "period": "fixture-v1",
                    "source_ids": ["demo:market:v1", "demo:valuation:v1"],
                },
            ],
            confidence=CandidateConfidence.MEDIUM,
            data_quality=CandidateDataQuality.PARTIAL,
            as_of="fixture-v1 · not live",
            evidence_gaps=[
                "Backlog cancellation and customer-concentration detail",
                "Live filing and market-data verification",
            ],
        ),
    )
    add(
        "lead",
        _candidate(
            candidate_id="demo:ionx",
            ticker="ION-X",
            company="Ion Foundry Networks · synthetic",
            bucket=CandidateBucket.VALUATION_GATED,
            score=73,
            variant_wedge="Quality appears durable, but much of the synthetic scenario may already be discounted.",
            why_now="The next reporting window can separate execution from multiple-driven enthusiasm.",
            first_rejection="The screen may simply be rediscovering a widely recognized quality compounder.",
            investable_if="A better entry or upward estimate revisions improve risk compensation.",
            kill_if="Growth decelerates while the expectations premium remains elevated.",
            next_workflow="scenario_sensitivity",
            evidence_ids=[
                "demo:fundamentals:v1",
                "demo:market:v1",
                "demo:valuation:v1",
            ],
            financials=[
                {
                    "metric": "revenue",
                    "label": "Revenue",
                    "value": 9400.0,
                    "unit": "USD millions",
                    "period": "FY2025 fixture",
                    "source_ids": ["demo:fundamentals:v1"],
                },
                {
                    "metric": "revenue_growth",
                    "label": "Revenue growth",
                    "value": 0.16,
                    "unit": "ratio",
                    "period": "FY2025 vs FY2024 fixture",
                    "source_ids": ["demo:fundamentals:v1"],
                },
                {
                    "metric": "operating_margin",
                    "label": "Operating margin",
                    "value": 0.31,
                    "unit": "ratio",
                    "period": "FY2025 fixture",
                    "source_ids": ["demo:fundamentals:v1"],
                },
                {
                    "metric": "enterprise_value_to_revenue",
                    "label": "EV / revenue",
                    "value": 9.6,
                    "unit": "x",
                    "period": "fixture-v1",
                    "source_ids": ["demo:market:v1", "demo:valuation:v1"],
                },
            ],
            confidence=CandidateConfidence.MEDIUM,
            data_quality=CandidateDataQuality.PARTIAL,
            as_of="fixture-v1 · not live",
            evidence_gaps=[
                "Current entry-price and consensus expectations",
                "Live filing and market-data verification",
            ],
        ),
    )
    add(
        "lead",
        _candidate(
            candidate_id="demo:nmbx",
            ticker="NMB-X",
            company="Nimbus Interconnect · synthetic",
            bucket=CandidateBucket.EXPOSURE_UNPROVEN,
            score=58,
            variant_wedge="Narrative exposure could be larger than reported segment labels imply.",
            why_now="Management language changed, but no quantified revenue pathway is yet visible.",
            first_rejection="The thematic link may be promotional language without orders, backlog, or revenue proof.",
            investable_if="A filing or call quantifies the pathway from product exposure to reported economics.",
            kill_if="The next disclosure again omits orders, backlog, revenue, and margin attribution.",
            next_workflow="meeting_prep",
            evidence_ids=[
                "demo:filings:v1",
                "demo:fundamentals:v1",
                "demo:market:v1",
                "demo:valuation:v1",
                "demo:risk:v1",
            ],
            financials=[
                {
                    "metric": "revenue",
                    "label": "Revenue",
                    "value": 760.0,
                    "unit": "USD millions",
                    "period": "FY2025 fixture",
                    "source_ids": ["demo:fundamentals:v1"],
                },
                {
                    "metric": "revenue_growth",
                    "label": "Revenue growth",
                    "value": 0.29,
                    "unit": "ratio",
                    "period": "FY2025 vs FY2024 fixture",
                    "source_ids": ["demo:fundamentals:v1"],
                },
                {
                    "metric": "operating_margin",
                    "label": "Operating margin",
                    "value": 0.09,
                    "unit": "ratio",
                    "period": "FY2025 fixture",
                    "source_ids": ["demo:fundamentals:v1"],
                },
                {
                    "metric": "enterprise_value_to_revenue",
                    "label": "EV / revenue",
                    "value": 6.9,
                    "unit": "x",
                    "period": "fixture-v1",
                    "source_ids": ["demo:market:v1", "demo:valuation:v1"],
                },
            ],
            confidence=CandidateConfidence.LOW,
            data_quality=CandidateDataQuality.LIMITED,
            as_of="fixture-v1 · not live",
            evidence_gaps=[
                "Quantified theme exposure in orders, backlog, revenue, and margin",
                "Live filing and market-data verification",
            ],
        ),
    )
    add(
        "lead",
        EventDraft(
            EventKind.AGENT_PROGRESS,
            "Validated every candidate against observed evidence IDs",
            agent_id="lead",
            payload={"progress": 95},
        ),
    )
    add(
        "lead",
        EventDraft(
            EventKind.AGENT_COMPLETED,
            "Candidate funnel ready; no investment recommendation issued",
            agent_id="lead",
        ),
    )
    events.append(
        EventDraft(
            EventKind.STAGE_COMPLETED,
            "Synthesis complete",
            payload={"stage": Stage.SYNTHESIS.value, "progress": 98},
        )
    )
    return tuple(events)


class DemoRuntime:
    """Replay scripted event drafts with realistic asynchronous pacing."""

    def __init__(self, delay_seconds: float = 0.18) -> None:
        if delay_seconds < 0:
            raise ValueError("delay_seconds must be non-negative")
        self.delay_seconds = delay_seconds

    async def stream(self, spec: RunSpec):
        for event in build_demo_events(spec):
            if self.delay_seconds:
                await asyncio.sleep(self.delay_seconds)
            yield event
