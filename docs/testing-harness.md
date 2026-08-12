# Testing harness

## Purpose

The harness verifies the trust boundaries that let a small model participate without
owning arithmetic, evidence, workflow topology, or replay state. Everything in the
default suite is offline and deterministic. No test requires SEC, Yahoo, KernelCubed, a
GPU, or an API key.

## One-command gate

From the repository root:

```bash
.venv/bin/pytest
.venv/bin/ruff check .
.venv/bin/python scripts/run_dag_harness.py
```

The first command runs the complete test suite, the second checks production, tests, and
scripts with the repository Ruff policy, and the third performs a real append/replay
cycle with an offline model fixture and finance registry.

## Coverage by boundary

| Boundary | Focused command | What a failure means |
| --- | --- | --- |
| Observation records | `.venv/bin/pytest tests/test_financial_observations.py tests/test_currency_unit_validation.py` | A strict-JSON, stable-ID, finite-number, unit, period, or evidence-reference invariant regressed. |
| SEC normalization | `.venv/bin/pytest tests/test_sec_normalization.py` | Concept/sign/unit mapping, exact periods, restatement selection, or malformed-record handling regressed. |
| yfinance normalization | `.venv/bin/pytest tests/test_yfinance_normalization.py` | Snapshot/history mapping, time normalization, adjustment metadata, action filtering, duplicate handling, or currency policy regressed. |
| Ledger and packets | `.venv/bin/pytest tests/test_observation_ledger.py` | Atomic ingestion, collision protection, deterministic selection/serialization, evidence resolution, or packet bounds regressed. |
| Finance arithmetic | `.venv/bin/pytest tests/test_financial_tools.py tests/test_financial_metrics.py tests/test_financial_forecasts.py tests/test_financial_market_ranking.py` | A formula, convention, unavailable-value rule, rounding rule, schema, sorting rule, or registry guard regressed. |
| Action and DAG contracts | `.venv/bin/pytest tests/test_actions.py tests/test_dag.py` | Strict action parsing, schema trust, topology, plan hashing, bounds, or tool-policy validation regressed. |
| Fixed-DAG runtime | `.venv/bin/pytest tests/test_fixed_dag.py` | Observation binding, preflight atomicity, repair policy, budgets, dependency/partial propagation, citations, bounded traces, or lifecycle journaling regressed. |
| Replay integrity | `.venv/bin/pytest tests/test_journal_integrity.py tests/test_reducer.py tests/test_task_events.py` | Strict JSON loading, canonical hash verification, workflow identity, sequencing, or projection rules regressed. |
| Debug UI | `.venv/bin/pytest tests/test_tui_debug.py tests/test_tui.py` | Full-stream retention, filtering, counters, exact detail rendering, transcript selection, follow-tail, shortcuts, or narrow-layout behavior regressed. |

These commands intentionally name files instead of relying on markers, so they are easy
to copy into a local debugging loop.

## Fixed-DAG execution/replay fixture

Run the self-checking fixture without retaining artifacts:

```bash
.venv/bin/python scripts/run_dag_harness.py
```

The fixture constructs normalized observations and evidence, builds a bounded
`EvidencePacket`, defines one controller-owned task, and uses a deterministic two-turn
model fixture:

1. the model proposes `finance.calculate` with trusted observation references;
2. the controller resolves values and lineage, validates the entire call batch, executes
   the deterministic calculation, and journals proposed/resolved arguments and result;
3. the model returns a schema-valid final action with an allowed citation; and
4. the harness reloads and replays the JSONL journal, then compares workflow identity,
   run/task status, and final sequence with the executed state.

The summary is JSON. A successful run has `"ok": true` and
`"replay_equivalent": true`. It also reports event count, model turns, tool calls, task
status, and the journal location. The current fixture produces 15 events, two model
turns, and one tool call.

Retain the event file for manual inspection by passing a path that does not already
exist:

```bash
.venv/bin/python scripts/run_dag_harness.py \
  --output artifacts/runs/offline-dag/events.jsonl
.venv/bin/alpha-orchestrate --replay \
  artifacts/runs/offline-dag/events.jsonl
```

`JsonlJournal` creates parent directories and opens the event file exclusively. Remove
or choose a different fixture path before running that exact command again.

## What replay verifies

Loading a fixed-DAG journal verifies:

- unique JSON object keys and finite JSON values;
- event schema, run identity, sequence, and lifecycle transitions;
- the canonical workflow plan hash and completion identity;
- model request hashes;
- bounded model trace and output hashes, including duplicated-field consistency;
- model-proposed and controller-resolved tool-argument hashes; and
- complete tool-result envelope hashes and duplicated-field consistency.

The adversarial integrity tests mutate one part of each envelope and assert that loading
fails on the exact journal line. Existing non-fixed-DAG synthetic journals remain
loadable where the envelopes do not apply, but a fixed-DAG event cannot omit required
integrity fields.

Replay is a state projection, not a second execution. It never calls the model, tool
registry, SEC, or yfinance.

## Debug-view verification

`tests/test_tui_debug.py` drives Textual headlessly. It checks more events than the
reducer's recent-event cap, combines family/agent/text filters, selects exact tool
arguments, toggles follow-tail, exercises `D` and `O`, and renders a narrow terminal.
This catches data-loss and interaction regressions without needing an interactive shell.

For manual visual QA:

```bash
.venv/bin/python -m alpha_orchestration --demo
```

Press `D`, filter to `MODEL` or `TOOL`, select an event, and compare the exact JSON
pane with the agent transcript. Press `O` to return to the operational overview.

## Network boundary

Provider mapper tests use local dictionaries shaped like SEC and yfinance responses.
The live clients are not invoked. When adding a new provider field, first add an offline
fixture that proves normalization, evidence locators, unit/period behavior, malformed
input handling, and deterministic IDs. Live smoke checks, if performed separately,
must be explicit and must not become prerequisites for this test gate.

