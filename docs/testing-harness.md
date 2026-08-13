# Testing harness

## Purpose

The harness verifies the trust boundaries that let a small model participate without
owning arithmetic, evidence, workflow topology, or replay state. Everything in the
default suite is offline and deterministic. No test requires SEC, Yahoo, KernelCubed, a
GPU, or an API key.

## One-command gate

From the repository root:

```bash
.venv/bin/python scripts/verify.py
```

The command fails at the first unsuccessful stage and runs, in order:

1. Ruff lint across production code, tests, and scripts;
2. the complete pytest suite with strict configuration and marker validation;
3. a multi-issuer fan-out/fan-in append/replay cycle with an offline model fixture;
4. an installed-package import smoke check; and
5. the module CLI's `--help` path.

The verifier resolves the repository root from its own location, disables third-party
pytest plugin auto-loading, removes ambient SEC identity and pytest options, hides GPUs,
and enables common library offline flags. It does not load `.env`, start KernelCubed,
or call SEC, Yahoo, model hubs, or any other network service. Dependency installation is
separate and must already be complete.

Formatting is intentionally not a blocking stage yet. `ruff format --check .` currently
reports pre-existing formatting drift across older files; adopt that check only with a
dedicated formatting cleanup so an infrastructure change does not force an unrelated
bulk rewrite.

Print the exact ordered subprocesses without running them:

```bash
.venv/bin/python scripts/verify.py --list
```

## Coverage by boundary

| Boundary | Focused command | What a failure means |
| --- | --- | --- |
| Observation records | `.venv/bin/pytest tests/test_financial_observations.py tests/test_currency_unit_validation.py` | A strict-JSON, stable-ID, finite-number, unit, period, or evidence-reference invariant regressed. |
| SEC normalization | `.venv/bin/pytest tests/test_sec_normalization.py` | Concept/sign/unit mapping, exact periods, restatement selection, or malformed-record handling regressed. |
| yfinance normalization | `.venv/bin/pytest tests/test_yfinance_normalization.py` | Snapshot/history mapping, time normalization, adjustment metadata, action filtering, duplicate handling, or currency policy regressed. |
| Ledger and packets | `.venv/bin/pytest tests/test_observation_ledger.py` | Atomic ingestion, collision protection, deterministic selection/serialization, evidence resolution, or packet bounds regressed. |
| Finance arithmetic | `.venv/bin/pytest tests/test_financial_tools.py tests/test_financial_metrics.py tests/test_financial_forecasts.py tests/test_financial_market_ranking.py` | A formula, convention, unavailable-value rule, rounding rule, schema, sorting rule, or registry guard regressed. |
| Action and DAG contracts | `.venv/bin/pytest tests/test_actions.py tests/test_dag.py` | Strict action parsing, schema trust, topology, plan hashing, bounds, or tool-policy validation regressed. |
| Fixed-DAG runtime | `.venv/bin/pytest tests/test_fixed_dag.py tests/test_offline_harness.py tests/test_offline_harness_attribution.py` | Observation binding, bounded parallelism, fan-in ordering, trusted ranking, per-issuer attribution, or lifecycle journaling regressed. |
| Replay integrity | `.venv/bin/pytest tests/test_journal_integrity.py tests/test_reducer.py tests/test_task_events.py` | Strict JSON loading, canonical hash verification, workflow identity, sequencing, or projection rules regressed. |
| Debug UI | `.venv/bin/pytest tests/test_tui_debug.py tests/test_tui.py` | Full-stream retention, filtering, counters, exact detail rendering, transcript selection, follow-tail, shortcuts, or narrow-layout behavior regressed. |

These commands intentionally name files instead of relying on markers, so they are easy
to copy into a local debugging loop.

## Fixed-DAG execution/replay fixture

Run the self-checking fixture without retaining artifacts:

```bash
.venv/bin/python scripts/run_dag_harness.py
```

The fixture constructs three bounded `EvidencePacket` values from normalized revenue
and net-income observations, defines three issuer branches plus a validator dependency,
and uses a deterministic model fixture:

1. all issuer branches enter their first model call concurrently, proving a measured
   peak of three within the controller's three-slot limit;
2. each branch proposes `finance.metrics` with trusted observation references, and the
   controller binds values and lineage before deterministic execution;
3. the validator starts only after all branches complete; each narrative's unique
   citations must exactly match that issuer packet;
4. the harness-owned projector ranks exact tool outputs and emits typed candidates; and
5. journal replay must restore identical task, evidence, candidate, and ordering state.

The summary is JSON. A successful run has `"ok": true` and
`"replay_equivalent": true`. It reports the slot limit and measured overlap peaks,
completed branches, dependency ordering, candidate count and ranked IDs, complete
source coverage, data-quality posture, validator override protection, and results
readiness. The current fixture has 53 events, seven model turns, three finance calls,
three complete branches, and three ranked candidates over nine evidence records.

The strict brief citation check proves per-issuer attribution, not semantic entailment.
The projector is an offline UI/replay prototype contract, not yet the live-provider
production bridge or the fail-closed live ranking binding.

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

The pytest suite also installs an automatic connection guard that rejects IPv4 and IPv6
connections before they reach the operating system while preserving local Unix-domain
and socket-pair IPC. `ALPHA_ALLOW_LIVE_NETWORK=1` disables that guard only for an
explicit, separately invoked live test; the single-command offline gate removes that
variable from its child environment.

## Continuous integration

`.github/workflows/offline-verification.yml` runs the same single command on Python
3.11, 3.12, and 3.13 for pushes, pull requests, and manual dispatches. CI installs only
the base project and `dev` extra; it does not install yfinance or KernelCubed and does
not receive provider or model credentials.

