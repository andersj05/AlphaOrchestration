# Current status

Last updated: 2026-08-13

## Snapshot

The fail-closed automatic-universe prototype was released through feature PR #4 into
`dev` and release PR #5 into `main`. It passed the authoritative offline verifier and
completed both network and fresh-cache real-provider smoke runs. With no arguments,
`python -m alpha_orchestration` launches automatic live research only when non-secret
readiness checks pass; otherwise it shows preflight and starts no research. Manual
`--live --tickers` and offline `--demo` paths remain available explicitly.

The default `US_LARGE_LIQUID_V1` profile targets 300 selected issuers, requires at least
200, inspects at most 1,000 market-cap-ranked provider rows, and uses eight reusable
logical lanes. The baseline is labeled `RULE-BASED SCREEN`; optional model diligence is
disabled unless separately configured.

## Active milestone

Collect reviewer feedback on the broad-universe funnel, provenance, exclusions, partial
failures, and surfaced research priorities. Investigate the dated live smoke's 72/300
issuer SEC evidence-selection gaps before treating current coverage as sufficient for a
broader research process.

## Current branch model

Use `main <- dev <- codex/<feature>`. Feature pull requests target `dev`; reviewed
release pull requests target `main`. GitHub branch-protection rules enforce pull
requests, resolved review conversations, administrator coverage, no force pushes or
branch deletion, and strict Python 3.11/3.12/3.13 verification checks on both branches.

## Verification baseline

The authoritative eight-stage `.venv/bin/python scripts/verify.py` gate passed project
memory, offline network isolation, Ruff, 281 strict offline tests, the 53-event fixed-DAG
harness, the automatic scale harness, package import, and CLI smoke. The hermetic
300-issuer fixture completed and replayed 4,061 contiguous events with 300 selected,
eligible, and screened issuers; 25 surfaced research-priority candidates; 300 persisted
universe rows; exactly eight reusable lanes; observed provider and analysis peaks of
eight; and replay equivalence.

Two opt-in real-provider runs on 2026-08-13 also completed and replayed: network run
`run-01dc1d3c76`, followed by fresh-cache run `run-e291f0ad0d`. Each reported 2,908
provider matches, inspected 500, selected 300, screened 228, failed 72, excluded 200
after inspection, left 2,408 uninspected, and surfaced 25 priorities. All 72 issuer
failures were `sec/evidence_selection: no comparable annual revenue fact`; there were
zero network/provider failures. Each run retained 1,699 evidence records, persisted
3,144 events, observed provider and analysis peaks of eight, and replayed complete. The
first cache posture was network and the second was cache.

Both live runs produced the same candidate order: SCHW, LNG, ACN, COF, HON, PLTR, META,
ADBE, UBER, EW, PGR, INTU, ALAB, MSFT, STX, APH, MRVL, OKE, MPWR, NFLX, NEM, GOOGL, KMI,
ADSK, NDAQ. This is dated smoke evidence, not a deterministic or permanent market
expectation and not an investment recommendation.

## Known boundaries

- The automatic baseline is deterministic rule-based triage, not LLM reasoning,
  expected-return analysis, or an investment recommendation.
- Provider-reported matches, inspected rows, selected issuers, screened issuers,
  surfaced priorities, post-inspection exclusions, uninspected matches, and failures
  are distinct counts. Uninspected matches are not researched or excluded.
- The live smoke exposed a material evidence-selection coverage gap: 72 selected issuers
  lacked a comparable annual revenue fact under the current SEC fact-selection policy.
- SEC identity/company facts and source-bound yfinance screen fields retain provenance,
  retrieval times, and cache posture. Provider terms and broad-universe operational
  behavior require continuing review beyond these two dated runs.
- The default gate must never load `.env`. Its verifier-launched Python processes deny
  standard Internet socket/DNS paths, but the guard is not OS-level isolation and does
  not cover deliberately unguarded or non-Python children.
- Optional model diligence remains a separately configured, bounded trust-boundary
  extension and cannot change controller-owned ranking or evidence.
- Type checking, coverage policy, dependency auditing, and repository-wide formatting
  cleanup remain deferred controls.

## Next handoff

Capture reviewer feedback on Results and Universe interpretation. Prioritize diagnosis
of the 72 issuer SEC annual-revenue gaps, then decide whether to expand fact-selection
coverage, source drill-through, or analysis depth before model-backed diligence or
release integration.
