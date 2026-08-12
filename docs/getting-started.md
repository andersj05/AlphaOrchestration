# Getting started

The current milestone has two offline entry points:

- the synthetic terminal application, which exercises the event reducer, journal, replay,
  Overview tab, and Debug / Journal tab; and
- a deterministic fixed-DAG harness, which exercises strict model actions, normalized
  evidence binding, a finance tool call, lifecycle hashes, journal persistence, and
  replay equivalence without KernelCubed or network access.

## Install

From WSL:

```bash
cd /home/base/AlphaOrchestration
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
```

## Launch the terminal application

Start a synthetic run immediately:

```bash
.venv/bin/python -m alpha_orchestration --demo
```

Open the mission setup screen:

```bash
.venv/bin/alpha-orchestrate
```

Run the same synthetic flow as plain terminal output:

```bash
.venv/bin/alpha-orchestrate --plain --demo-delay 0
```

Controls inside the full-screen run view:

- `D`: open Debug / Journal
- `O`: return to Overview
- `Space`: pause or resume event consumption
- `C`: cancel and drain the run
- `R`: restart the same mandate
- `N`: return to mission setup
- `?`: show help
- `Q`: quit

The debugger keeps the complete event stream owned by the screen rather than the
reducer's bounded recent-event summary. Filter by event family or agent, search message
and payload text, toggle follow-tail, and select a row to inspect its exact JSON record
and complete per-agent transcript. The counter strip reports total/visible events,
agents, tool calls, rejections, and failures.

## Run the fixed-DAG harness

Execute a two-turn offline fixture and immediately replay its journal:

```bash
.venv/bin/python scripts/run_dag_harness.py
```

Without `--output`, the harness uses a temporary journal and removes it after verifying
equivalent run status, workflow identity, task state, and final sequence. To retain a
journal, provide a new path that does not already exist:

```bash
.venv/bin/python scripts/run_dag_harness.py \
  --output artifacts/runs/offline-dag/events.jsonl
```

The harness prints a strict JSON summary with the run status, event count, model turns,
task status, tool-call count, journal path, and `replay_equivalent` result. Its
`EvidencePacket` is built from offline normalized observations and evidence; it does
not fetch SEC or Yahoo data. The current fixed-DAG scheduler is serial and records
`actual_active_slots: 1`.

## Artifacts, integrity, and replay

Full-screen and plain runs write an append-only journal to
`artifacts/runs/<run-id>/events.jsonl`. Summarize a saved journal with:

```bash
.venv/bin/alpha-orchestrate --replay artifacts/runs/<run-id>/events.jsonl
```

Journal loading verifies strict JSON plus any present lifecycle integrity envelopes.
For fixed-DAG runs this includes the canonical plan, model request/trace, proposed and
resolved tool arguments, and tool-result hashes. Replay reduces recorded events only; it
does not call the model, tools, SEC, or yfinance.

Capture a deterministic SVG of the completed Overview dashboard for visual QA:

```bash
.venv/bin/python scripts/capture_tui.py artifacts/tui-demo.svg
```

## Optional live data adapters

Install yfinance only when explicitly working on the live data plane:

```bash
.venv/bin/pip install -e ".[data]"
```

SEC JSON access requires a descriptive identity with contact information:

```bash
export ALPHA_SEC_USER_AGENT="AlphaOrchestration your-email@example.com"
```

`SecDataClient` and `YFinanceClient` calls are explicit. Their mapper functions turn
provider payloads into normalized offline domain records, but neither the synthetic TUI
nor deterministic finance tools fetch provider data implicitly.

## Verify

Run the complete offline suite and lint check:

```bash
.venv/bin/pytest
.venv/bin/ruff check .
```

Run the major slices independently while developing:

```bash
.venv/bin/pytest \
  tests/test_financial_observations.py \
  tests/test_sec_normalization.py \
  tests/test_yfinance_normalization.py \
  tests/test_observation_ledger.py

.venv/bin/pytest \
  tests/test_actions.py \
  tests/test_dag.py \
  tests/test_fixed_dag.py \
  tests/test_journal_integrity.py

.venv/bin/pytest tests/test_tui_debug.py
```

All of these tests use deterministic local fixtures. See
[Testing harness](testing-harness.md) for the coverage matrix and expected failure
checks.

