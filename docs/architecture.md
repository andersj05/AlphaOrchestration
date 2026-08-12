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

The model does not author this graph. Every task has a controller-owned identity,
dependency list, tool allowlist, output schema, turn/tool/token/byte budgets, and bounded
repair policy. The current `FixedDagRuntime` executes a stable topological order
serially and journals `actual_active_slots: 1`. The graph contract retains an
`active_slots` capacity for a later concurrent scheduler. Failed dependencies either
skip dependents or propagate explicit degraded ancestry; they cannot silently disappear.

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

## State, journaling, and replay

The controller alone assigns schema version, run ID, global sequence, and timestamp to
each event. It reduces an event before appending it, so an invalid draft cannot corrupt
the journal. The JSONL journal is the source of truth and `RunState` is a projection.
Sequence gaps, unknown schema versions, mismatched run IDs, invalid lifecycle
transitions, and events after terminal state are replay failures.

A fixed-DAG journal records the exact canonical workflow plan and hash, model requests
and request hashes, bounded generation traces and hashes, proposed and controller-bound
tool arguments and hashes, and complete tool-result envelopes and hashes. Loading a
journal rejects duplicate JSON keys, non-finite JSON constants, hash mismatches, plan or
workflow identity mismatches, and inconsistent duplicated envelope fields. Existing
non-fixed-DAG synthetic journals remain readable where lifecycle integrity envelopes do
not apply.

Generation traces carry bounded prompt/output token IDs, finish reason, sampling
controls, request/session IDs, telemetry, and model/tokenizer fingerprints. These
records make execution auditable and deterministic state replay possible; replay does
not rerun a model, tool, or provider call. Exact inference reproduction still depends on
the future KernelCubed adapter and its engine-level telemetry.

The first schema keeps UI-relevant evidence inline. Full filings and large tool payloads
should later live in a content-addressed artifact store, referenced by hash and stable
source ID from the journal.

## Operational debugger

The TUI keeps a screen-owned copy of the complete event stream for inspection, separate
from the reducer's bounded recent-event projection. The Debug / Journal tab supports
event-family, agent, unassigned-controller, and free-text filtering; follow-tail
navigation; aggregate tool/rejection/failure counters; exact JSON for the selected
record; and a per-agent transcript. This is a diagnostic view over in-process events,
not an editor for the append-only journal.

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
- SEC filing documents/sections when the content-addressed artifact cache lands;
- yfinance for basic dated price history, profiles, and corporate-action context;
- deterministic calculators for ratios, growth, scenarios, and unit-safe transformations.

The SEC client requires a descriptive user agent and stays at or below the SEC's
published ten-request-per-second ceiling. For sector-wide refreshes, prefer nightly bulk
archives, caching, conditional requests, and a central fetch queue.

The yfinance adapter is explicitly a replaceable convenience source. It must not become
the canonical provider shape in domain objects, and its terms/data quality must be
reviewed before use beyond personal research.

## Build sequence

1. **Implemented:** event core, deterministic demo, append-only journals, replay, TUI,
   strict action/final envelopes, read-only tool registry, and finance calculators.
2. **Implemented:** normalized SEC/yfinance observations, evidence records, observation
   ledger/packets, fixed-DAG execution, observation-bound finance calls, lifecycle
   hashes, replay-integrity checks, and the Debug / Journal tab.
3. Add content-addressed filing artifacts, provider cache/queue policy, normalized
   task-output row binding for `finance.rank`, and a concurrent fixed-DAG scheduler.
4. Wire KernelCubed behind a supervised engine process and preserve its exact telemetry,
   cancellation, overload, and timeout behavior.
5. Add sensitivity/IRR tools, audited statement bridges, deeper-work routing, report
   export, thesis monitoring, and held-out model/tool-policy evaluations.

Capability evaluation and serving/load evaluation remain separate gates. Human review
remains mandatory throughout the research prototype.

