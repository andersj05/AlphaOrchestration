# Initial architecture

## Design objective

Build a useful research harness around a small local model without asking that model to
invent and manage an open-ended agent graph. The first production-shaped loop is a
controller-authored, bounded fan-out/fan-in workflow with narrow model microtasks,
deterministic tools, explicit evidence, and human review.

KernelCubed's current finance traces are a capability baseline, not autonomous-finance
readiness. The orchestration layer therefore treats model output as an untrusted proposal
that must pass schema, policy, grounding, budget, and termination checks.

## Ownership

AlphaOrchestration owns:

- research mandates, task graphs, dependencies, budgets, deadlines, and retry rules;
- complete transcripts and prompt rendering for every turn;
- tool schemas, allowlists, argument validation, idempotency, and result-size limits;
- source/evidence lineage, candidate validation, and human approval gates;
- journals, artifacts, recovery, observability, and the TUI.

KernelCubed owns:

- one memory-bounded local engine lifecycle;
- logical-session admission and same-session FIFO ordering;
- token streaming, cancellation, cleanup, and scheduler telemetry.

## First workflow

```text
normalize universe
        │
        ├──────────────┬─────────────────┐
        ▼              ▼                 ▼
  SEC evidence    market context   deterministic calculations
        └──────────────┴─────────────────┘
                       │
              issuer evidence packets
                       │
            ┌──────────┴──────────┐
            ▼                     ▼
      candidate draft       independent skeptic
            └──────────┬──────────┘
                       ▼
             deterministic validator
                       ▼
             candidate-triage funnel
                       ▼
                   human review
```

The model does not author this graph. Issuer branches can run concurrently, but each
task has fixed dependencies and bounds. Failed branches may produce an explicitly
partial report; they cannot silently disappear.

## Small-model action loop

Each model turn should eventually produce exactly one compact action envelope:

```json
{"kind":"tool_calls","calls":[{"name":"sec_get_company_facts","arguments":{}}]}
```

or:

```json
{"kind":"final","payload":{}}
```

Initial policy:

1. Give each task only the evidence and tools it needs.
2. Validate the tool name, strict argument schema, allowlist, deadline, and byte budget.
3. Feed exact, normalized tool results back into the authoritative transcript.
4. Derive an evidence-ID allowlist from observed results and reject unknown citations.
5. Allow at most one short JSON-only repair for syntax/schema failure.
6. Do not repair semantic errors, fabricated evidence, non-natural stops, or policy blocks.
7. Never automatically replay a side-effecting tool action.

Constrained decoding belongs in the engine adapter when supported. Alpha should not
import vLLM directly to bypass KernelCubed's runtime boundary.

## State and replay

The controller assigns schema version, run ID, global sequence, and timestamp to every
event. The JSONL journal is the source of truth; `RunState` is a projection. Sequence
gaps, unknown schema versions, mismatched run IDs, and events after terminal state are
audit failures.

The first schema keeps UI-relevant evidence inline. Full filings and large tool payloads
should later live in a content-addressed artifact store, referenced by hash and stable
source ID from the journal.

Durable generation traces should include exact prompt/output token IDs and hashes,
finish reason, sampling controls, request/session IDs, queue state, timings, model and
tokenizer fingerprints, and batch/concurrency context. Decoded text is not an exact
replay identity.

## Evidence and candidate policy

Provider data is normalized into source and evidence records before a model sees it.
Facts, calculations, assumptions, estimates, and PM judgments must remain distinguishable.
Calculations cite their input evidence IDs and retain units and periods. Dated market
data carries an as-of timestamp. SEC facts retain CIK, form/accession, fiscal period,
unit, and locator.

The initial TUI uses the public-equity idea-triage vocabulary:

- Advance to deeper work
- Valuation / expectations gated
- Exposure not yet proven
- Deprioritized

Advancement is not an investment recommendation. A thematic candidate cannot advance
without a source-backed pathway from the claimed driver to orders, backlog, revenue,
margins, or estimate revisions. Every candidate needs a first rejection, investable-if
condition, kill test, evidence list, and next research workflow.

## Data-source plan

Phase 1 uses:

- SEC submissions for issuer metadata and filing history;
- SEC company facts for standardized XBRL facts;
- SEC filing documents/sections when the source ledger and artifact cache land;
- yfinance for basic dated price history, profiles, and corporate-action context;
- deterministic calculators for ratios, growth, scenarios, and unit-safe transformations.

The SEC client requires a descriptive user agent and stays at or below the SEC's
published ten-request-per-second ceiling. For sector-wide refreshes, prefer nightly bulk
archives, caching, conditional requests, and a central fetch queue.

The yfinance adapter is explicitly a replaceable convenience source. It must not become
the canonical provider shape in domain objects, and its terms/data quality must be
reviewed before use beyond personal research.

## Build sequence

1. **Current:** event core, deterministic demo, TUI, replay, adapter seams.
2. Add source/evidence ledgers, content-addressed artifacts, and read-only tool registry.
3. Implement SEC/yfinance tools plus deterministic calculators and cache/rate policies.
4. Add fixed task DAG, budgets, partial completion, and typed retry/overload handling.
5. Add constrained action/final schemas, dynamic evidence allowlists, and held-out evals.
6. Wire KernelCubed behind a supervised engine process and preserve its exact telemetry.
7. Add forecasts, scenarios, deeper-work routing, report export, and thesis monitoring.

Capability evaluation and serving/load evaluation remain separate gates. Human review
remains mandatory throughout the research prototype.

