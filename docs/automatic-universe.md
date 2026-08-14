# Automatic broad-universe research

## Product path

Running `python -m alpha_orchestration` with no arguments starts the automatic live
research workflow when its non-secret preflight is ready. Missing SEC identity,
market-data support, or runtime composition blocks before provider I/O; the application
never substitutes fixture results. Manual one-to-eight ticker research and the offline
demo remain expert paths.

This workflow is public-equity idea triage, not a recommendation engine. It evaluates a
large, explicitly defined cohort, promotes a smaller evidence-complete set, and records
what should be researched next. It does not claim that every selected issuer received a
full investment-underwriting process.

## `US_LARGE_LIQUID_V1` cohort

The controller-owned default policy is versioned and hashable:

- U.S. `EQUITY` records on Yahoo exchange codes `NMS`, `NGM`, `NCM`, or `NYQ`;
- market capitalization of at least $300 million, share price of at least $1, and
  three-month average daily volume of at least 200,000 shares;
- live pages sorted by market capitalization descending, with a stable ticker tie-break;
- an exact join to the SEC company-ticker-exchange association, limited to Nasdaq and
  NYSE issuers;
- one security per SEC CIK, retaining the highest-market-cap share class; and
- a target of 300 selected issuers, with a fail-closed minimum of 200.

The market provider can report more matches than the workflow inspects. The default
inspects market-cap-ordered pages only until it can form the selected cohort, bounded at
1,000 rows. Provider matches, inspected rows, selected issuers, uninspected rows,
exclusions, and analysis failures are therefore separate facts. An uninspected row is
never described as excluded or researched.

The SEC cautions that ticker/CIK associations are periodically updated but are not
guaranteed complete. The manifest is consequently a reproducible research cohort, not a
claim to contain every tradable U.S. security.

## Funnel and execution

The live journal carries complete snapshots of this funnel:

1. **Discover** — cache or retrieve source pages and construct the immutable manifest.
2. **Select** — apply identity, security-type, exchange, liquidity, completeness, and
   duplicate-issuer rules.
3. **Screen** — process selected issuers through eight persistent, bounded analysis
   lanes; retrieve SEC company facts and compute controller-owned metrics.
4. **Prioritize** — rank only exact tool results with an explicit missing-data policy.
5. **Diligence** — build bounded research cards for the promoted set. An optional action
   model can supplement these cards, but cannot change identity, arithmetic, rank, source
   allowlists, budgets, or terminal status.
6. **Surface** — persist ranked candidates and a bounded row for every selected issuer.

The Results view is for surfaced research candidates. The Universe view retains the
hundreds-name audit list with status and rank so the operator can verify actual coverage.
Candidate labels mean “advance to deeper work,” “valuation/expectations gated,” “exposure
not yet proven,” or “deprioritized”; they do not mean buy, sell, or expected return.

## Evidence and source posture

- The official SEC ticker/CIK/exchange map establishes issuer identity.
- SEC company-facts observations provide comparable filing-derived financial inputs.
- yfinance screener rows provide the cohort's market price, capitalization, currency,
  exchange, and liquidity inputs.
- A valuation metric is unavailable when market and filing currencies cannot be matched.
- Raw provider responses use request-addressed, content-hashed cache records with an
  explicit freshness boundary. Corrupt, mismatched, or stale records fail closed rather
  than becoming silent fallback data.
- Displayed facts retain evidence IDs, source locators, retrieval times, and URLs.
  Citation validation establishes source attribution; it does not prove that narrative
  text is semantically entailed by a source.

yfinance is an unofficial market-data adapter intended for personal research use. Review
its current terms before using this prototype beyond that context.

## Integrity and replay

The append-only journal is the source of truth. The controller sequences every event and
owns the manifest, concurrency limits, calculations, rank, candidate identity, and final
coverage state. Replay reduces the journal without providers, models, tools, `.env`, or
network access. The offline development gate uses deterministic fakes for the
hundreds-name scale contract; live-provider smoke runs remain explicit and separate.

## Known boundary

The default automatic prototype performs deterministic rule-based analysis across eight
logical lanes. The optional `ActionModel` diligence seam is validated and bounded, but a
local KernelCubed model runtime is not silently installed or enabled. When no model is
configured, the UI says `RULE-BASED`; it does not report synthetic model turns.
