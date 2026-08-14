# Getting started

The current milestone has two deterministic offline entry points and one explicit live
path:

- the synthetic terminal application, which exercises the event reducer, journal, replay,
  Overview, Results, and Debug / Journal tabs; and
- a deterministic fixed-DAG harness, which exercises three parallel issuer branches,
  normalized evidence binding, deterministic finance metrics and ranking, a validator
  fan-in, typed candidate projection, lifecycle hashes, journal persistence, and replay
  equivalence without KernelCubed or network access; and
- a rule-based live bridge for an operator-supplied ticker universe, with explicit SEC
  and yfinance collection, normalized evidence, bounded issuer analysis, and trusted
  controller-owned ranking.

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

Execute the three-issuer fan-out plus validator fan-in fixture and replay its journal:

```bash
.venv/bin/python scripts/run_dag_harness.py
```

Without `--output`, the harness uses a temporary journal and removes it after verifying
equivalent run status, workflow identity, task/evidence/candidate state, controller-owned
rank order, and final sequence. To retain a journal, provide a new path that does not
exist:

```bash
.venv/bin/python scripts/run_dag_harness.py \
  --output artifacts/runs/offline-dag/events.jsonl
```

The harness prints a strict JSON summary including completed branches, slot limit,
measured scheduler/model peaks, dependency-order status, candidate count and ranked IDs,
source coverage, data-quality posture, results readiness, and replay equivalence. A
successful current run proves three overlapping branch calls within a three-slot limit,
three completed branches, and three candidates.

Each branch binds offline normalized revenue and net-income observations to
`finance.metrics`. After all three complete, the validator supplies research narratives
but cannot alter controller-owned identities or ranks. Its per-issuer citation list must
exactly match that issuer's evidence packet before projection into the typed candidate.
This proves attribution, not semantic entailment. The harness-owned projector is the
offline UI/replay fixture contract and remains separate from the rule-based live
projection.

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

## Run the rule-based live prototype

Add yfinance to an existing development environment, or install both extras together:

```bash
.venv/bin/pip install -e ".[data]"
# New environment alternative:
.venv/bin/pip install -e ".[dev,data]"
```

SEC JSON access requires a descriptive identity with contact information. Put it in the
repository-local `.env` file (or export the same variable in the shell):

```dotenv
ALPHA_SEC_USER_AGENT="AlphaOrchestration your-email@example.com"
```

Launch the live Results UI for an explicit universe of one to eight tickers:

```bash
.venv/bin/python -m alpha_orchestration --live --tickers AAPL,MSFT,NVDA
```

Use plain mode for streamed event output and a ranked terminal summary:

```bash
.venv/bin/python -m alpha_orchestration --live --tickers AAPL,MSFT,NVDA --plain
```

This prototype is rule-based: providers supply evidence while controller-owned finance
tools calculate comparable metrics and ranks. It is fail-closed and never substitutes
synthetic fixture data. Provider failures remain visible, issuers without trusted SEC
evidence are excluded, and the run fails if no requested issuer is eligible.

Normalized evidence carries stable source IDs, provider locators, retrieval times, and
content hashes into the append-only run journal at
`artifacts/runs/<run-id>/events.jsonl`. Provider responses use an integrity-checked,
content-addressed cache at `artifacts/live-cache/`; the default freshness window is six
hours, with seven days for the official SEC ticker map. Override these roots with
`--artifacts` and `--live-cache`. Keep both directories local because they contain
downloaded provider data and research artifacts.

The [yfinance documentation](https://ranaroussi.github.io/yfinance/) states that the
library is intended for research and education and Yahoo Finance data is intended for
personal use. Review its legal disclaimer and Yahoo's terms before using the live path.

## Verify

Run the complete deterministic offline gate:

```bash
.venv/bin/python scripts/verify.py
```

This command runs seven stages: project-memory integrity, a Python-process network-guard
self-test, Ruff lint, pytest with strict configuration and markers, the fixed-DAG
execution/replay fixture, and lightweight installed-package and CLI smoke checks. The
default code path does not load `.env`, invoke a live provider, start KernelCubed, or
require a GPU.

The socket/DNS guard is defense for verifier-launched Python processes, not an OS network
namespace or firewall; see [Testing harness](testing-harness.md) for its exact boundary.
Formatting remains a documented follow-up gate because the existing tree needs a
separate one-time Ruff formatting cleanup.

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

