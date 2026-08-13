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
- Overview, Results, and full-stream Debug / Journal tabs for inspecting ranked research
  candidates, agent messages, tool calls, rejections, failures, and exact event JSON.

The fixed-DAG executor enforces controller-owned slot bounds and dependency ordering.
The offline harness measures three issuer branches overlapping in three active slots,
then runs a validator fan-in and projects a typed, source-backed ranked-results artifact
before workflow completion. That trusted projector is a deterministic prototype fixture,
not yet the production bridge to live SEC/yfinance collection. Provider calls remain
explicit adapter operations; no provider fetch occurs implicitly during tool execution.

## Quick start

From WSL:

```bash
cd /home/base/AlphaOrchestration
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
.venv/bin/python scripts/verify.py
```

Launch the synthetic TUI and open the debugger with `D`:

```bash
.venv/bin/python -m alpha_orchestration --demo
```

Run only the deterministic fixed-DAG execution/replay fixture:

```bash
.venv/bin/python scripts/run_dag_harness.py
```

See [Getting started](docs/getting-started.md), [Financial data](docs/financial-data.md),
[Financial tools](docs/financial-tools.md), [Architecture](docs/architecture.md), and the
[Testing harness](docs/testing-harness.md) for the full contracts and verification flow.
