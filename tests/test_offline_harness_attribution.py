from __future__ import annotations

import pytest

from alpha_orchestration.domain import JsonValue
from alpha_orchestration.fixed_dag import FixedDagRuntime
from alpha_orchestration.offline_harness import (
    ISSUERS,
    BranchMetricResult,
    MultiIssuerHarnessModel,
    RankedResultsProjector,
    build_issuer_evidence,
    build_workflow,
)
from alpha_orchestration.tools.finance import build_financial_tool_registry


def _projector_ready_for_ranking() -> tuple[
    RankedResultsProjector, list[dict[str, JsonValue]]
]:
    evidence_by_task = build_issuer_evidence()
    runtime = FixedDagRuntime(
        build_workflow(),
        MultiIssuerHarnessModel(evidence_by_task),
        build_financial_tool_registry(),
        evidence_packets_by_task={
            task_id: evidence.packet for task_id, evidence in evidence_by_task.items()
        },
    )
    projector = RankedResultsProjector(runtime, evidence_by_task)
    projector._branch_results = {  # noqa: SLF001 - focused trust-boundary test
        issuer.task_id: BranchMetricResult(
            issuer=issuer,
            revenue_growth=round(
                (issuer.current_revenue - issuer.prior_revenue) / issuer.prior_revenue,
                4,
            ),
            net_margin=round(issuer.net_income / issuer.current_revenue, 4),
            source_ids=evidence_by_task[issuer.task_id].packet.source_ids,
        )
        for issuer in ISSUERS
    }
    briefs: list[dict[str, JsonValue]] = [
        {
            "ticker": issuer.ticker,
            "variant_wedge": issuer.variant_wedge,
            "why_now": issuer.why_now,
            "first_rejection": issuer.first_rejection,
            "investable_if": issuer.investable_if,
            "kill_if": issuer.kill_if,
            "next_workflow": issuer.next_workflow,
            "source_ids": list(evidence_by_task[issuer.task_id].packet.source_ids),
        }
        for issuer in ISSUERS
    ]
    projector._validator_output = {  # noqa: SLF001 - focused trust-boundary test
        "proposed_ranked_ids": ["ticker:FORGED", ISSUERS[2].entity_id, ISSUERS[1].entity_id],
        "briefs": briefs,
    }
    return projector, briefs


def test_validator_brief_sources_are_projected_after_strict_attribution() -> None:
    """This proves citation attribution, not semantic entailment of narrative text."""
    projector, briefs = _projector_ready_for_ranking()

    artifact = projector._build_artifact()  # noqa: SLF001 - focused boundary test

    expected = {
        str(brief["ticker"]): tuple(brief["source_ids"])
        for brief in briefs
        if isinstance(brief["source_ids"], list)
    }
    assert {
        result.candidate.ticker: result.candidate.evidence_ids
        for result in artifact.candidates
    } == expected


def test_validator_brief_rejects_cross_issuer_sources() -> None:
    projector, briefs = _projector_ready_for_ranking()
    briefs[0]["source_ids"] = briefs[1]["source_ids"]

    with pytest.raises(RuntimeError, match="exactly match.*ALP"):
        projector._build_artifact()  # noqa: SLF001 - focused boundary test


@pytest.mark.parametrize(
    ("mutation", "error"),
    (
        ("missing", "exactly match.*ALP"),
        ("unknown", "exactly match.*ALP"),
        ("duplicate", "must be unique.*ALP"),
    ),
)
def test_validator_brief_rejects_invalid_source_sets(
    mutation: str,
    error: str,
) -> None:
    projector, briefs = _projector_ready_for_ranking()
    sources = briefs[0]["source_ids"]
    assert isinstance(sources, list)
    assert all(isinstance(source_id, str) for source_id in sources)

    if mutation == "missing":
        briefs[0]["source_ids"] = sources[:-1]
    elif mutation == "unknown":
        briefs[0]["source_ids"] = [*sources[:-1], "evidence:unknown"]
    else:
        briefs[0]["source_ids"] = [sources[0], sources[0], *sources[2:]]

    with pytest.raises(RuntimeError, match=error):
        projector._build_artifact()  # noqa: SLF001 - focused boundary test
