# AlphaOrchestration

AlphaOrchestration is a deterministic research-orchestration layer for small local
models, currently designed around Qwen3.5 0.8B. The controller owns workflow shape,
budgets, schemas, evidence, arithmetic, and the append-only audit trail. The model is
limited to bounded structured actions and interpretation.

The current slice includes:

- normalized SEC company-facts and yfinance observations with explicit units, periods,
  stable evidence IDs, and provider locators;
- a collision-safe in-memory `ObservationLedger` and bounded, self-contained
  `EvidencePacket` values;
- six deterministic finance tools for arithmetic, metrics, forecasts, DCF, market
  statistics, and ranking;
- controller-authored fixed DAGs with strict action parsing, tool allowlists, bounded
  repair, dependency/partial-result policy, and exact lifecycle journaling;
- replay-time integrity checks for workflow plans, model requests/traces, tool arguments,
  and tool results; and
- an Overview tab plus a full-stream Debug / Journal tab for inspecting agent messages,
  tool calls, rejections, failures, and exact event JSON.

The fixed-DAG executor is deliberately serial in this slice and reports
`actual_active_slots: 1`, even when a workflow declares more capacity. SEC and
yfinance calls are explicit adapter operations: tests and the DAG harness are offline,
and no provider fetch occurs implicitly during tool execution.

## Quick start

From WSL:

```bash
cd /home/base/AlphaOrchestration
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
.venv/bin/pytest
.venv/bin/ruff check .
```

Launch the synthetic TUI and open the debugger with `D`:

```bash
.venv/bin/python -m alpha_orchestration --demo
```

Run the deterministic fixed-DAG execution/replay harness:

```bash
.venv/bin/python scripts/run_dag_harness.py
```

See [Getting started](docs/getting-started.md), [Financial data](docs/financial-data.md),
[Financial tools](docs/financial-tools.md), [Architecture](docs/architecture.md), and the
[Testing harness](docs/testing-harness.md) for the full contracts and verification flow.
