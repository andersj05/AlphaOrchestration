# Getting started

The current milestone defaults to a fail-closed automatic live screen and keeps manual
and offline modes available explicitly:

- no arguments discover the source-bound `US_LARGE_LIQUID_V1` cohort and launch an
  eight-lane, rule-based screen when live preflight is ready;
- `--live --tickers` runs the same trusted SEC/yfinance analysis for an operator-supplied
  universe of one to eight symbols;
- `--demo` runs the deterministic synthetic terminal application; and
- the fixed-DAG and 300-issuer scale harnesses exercise concurrency, trusted projection,
  journal persistence, and replay equivalence without KernelCubed or network access.

The automatic baseline is a research-priority screen, not expected-return analysis or an
investment recommendation. It is labeled rule-based; optional model diligence is not
active unless separately configured.

## Install

From WSL:

```bash
cd /home/base/AlphaOrchestration
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
```

## Launch the terminal application

After configuring the live prerequisites below, launch the automatic screen with no
required arguments:

```bash
.venv/bin/python -m alpha_orchestration
```

A missing SEC identity, optional market package, or runtime prerequisite opens a
narrow-safe preflight screen and starts no research. Use **Expert Setup** there for
manual controls. To run the synthetic fixture explicitly:

```bash
.venv/bin/python -m alpha_orchestration --demo
```

Run that same synthetic flow as plain terminal output:

```bash
.venv/bin/alpha-orchestrate --demo --plain --demo-delay 0
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

## Run the automatic or manual rule-based live prototype

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

Launch automatic live research with the default 300-issuer target, 200-issuer minimum,
1,000-row inspection bound, and eight reusable lanes:

```bash
.venv/bin/python -m alpha_orchestration
```

The funnel reports provider matches, inspected rows, selected issuers, screened issuers,
optional deep reviews, surfaced priorities, post-inspection exclusions, uninspected
matches, and failures. Uninspected provider matches are never presented as researched or
excluded.
See [Automatic universe research](automatic-universe.md) for the exact discovery,
coverage, failure, concurrency, and provenance contracts.

For an expert/manual universe, launch the Results UI with one to eight tickers:

```bash
.venv/bin/python -m alpha_orchestration --live --tickers AAPL,MSFT,NVDA
```

Use plain mode for streamed event output and a ranked terminal summary:

```bash
.venv/bin/python -m alpha_orchestration --live --tickers AAPL,MSFT,NVDA --plain
```

Both live paths are rule-based: providers supply evidence while controller-owned finance
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

This command runs eight stages: project-memory integrity, a Python-process network-guard
self-test, Ruff lint, pytest with strict configuration and markers, the fixed-DAG
execution/replay fixture, the automatic 300-issuer scale harness, and lightweight
installed-package and CLI smoke checks. The default code path does not load `.env`,
invoke a live provider, start KernelCubed, or require a GPU.

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

