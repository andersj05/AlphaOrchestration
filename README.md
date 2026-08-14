# AlphaOrchestration

AlphaOrchestration is a deterministic public-equity research-orchestration layer. The
controller owns workflow shape, budgets, schemas, evidence, arithmetic, ranking, and the
append-only audit trail. The default automatic prototype is a rule-based screen and does
not imply LLM analysis; an optional model may supply only bounded, untrusted diligence
output.

The current slice includes:

- normalized SEC company-facts and yfinance observations with explicit units, periods,
  stable evidence IDs, and provider locators;
- source-bound automatic discovery of the `US_LARGE_LIQUID_V1` cohort, followed by an
  eight-lane rule-based screen and controller-owned result ranking;
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
before workflow completion. That trusted projector remains a deterministic offline
fixture and is distinct from the rule-based live SEC/yfinance bridge. Provider calls
remain explicit adapter operations; no provider fetch occurs implicitly during tool
execution.

## Quick start

From WSL:

```bash
cd /home/base/AlphaOrchestration
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
.venv/bin/python scripts/verify.py
```

The eight-stage gate checks durable project memory, self-tests Python-process network
denial, runs Ruff and strict pytest, executes/replays the deterministic fixed-DAG and
300-issuer scale harnesses, and smoke-tests the installed package and CLI. The network guard is defense in depth for
verifier-launched Python processes, not an OS network namespace or firewall; see the
[Testing harness](docs/testing-harness.md) for its exact enforcement boundary.

Launch the synthetic TUI and open the debugger with `D`:

```bash
.venv/bin/python -m alpha_orchestration --demo
```

## Live rule-based prototype

Install the live data support and set a descriptive SEC identity in a local `.env`:

```bash
.venv/bin/pip install -e ".[dev,data]"
```

```dotenv
ALPHA_SEC_USER_AGENT="AlphaOrchestration your-email@example.com"
```

With those prerequisites ready, the no-argument command launches the automatic Results
UI. If preflight is not ready, it shows the missing key/package state and starts no
research; it never substitutes the demo fixture.

```bash
.venv/bin/python -m alpha_orchestration
```

The default `US_LARGE_LIQUID_V1` profile inspects at most 1,000 source-ranked provider
rows, requires at least 200 eligible issuers, selects 300, and screens them through eight
reusable logical lanes. Results distinguish provider matches, inspected rows, selected
and screened issuers, optional deep reviews, surfaced priorities, post-inspection
exclusions, uninspected matches, and failures.

Expert users can instead supply one to eight tickers or request plain output explicitly:

```bash
.venv/bin/python -m alpha_orchestration --live --tickers AAPL,MSFT,NVDA
.venv/bin/python -m alpha_orchestration --live --tickers AAPL,MSFT,NVDA --plain
```

Both live modes apply controller-owned calculations and ranking to normalized SEC and
yfinance evidence. They fail closed instead of substituting synthetic fixture data:
provider failures are surfaced, issuers without trusted SEC evidence are excluded, and
the run fails if no issuer is eligible. Append-only run journals are written beneath
`artifacts/runs/`; integrity-checked provider payloads are cached beneath
`artifacts/live-cache/`. Both locations are configurable with CLI flags.

The [yfinance documentation](https://ranaroussi.github.io/yfinance/) describes the
library as intended for research and education and Yahoo Finance data as intended for
personal use. Review its legal disclaimer and Yahoo's terms before using live data.

Run only the deterministic fixed-DAG execution/replay fixture:

```bash
.venv/bin/python scripts/run_dag_harness.py
```

See [Getting started](docs/getting-started.md),
[Automatic universe](docs/automatic-universe.md), [Financial data](docs/financial-data.md),
[Financial tools](docs/financial-tools.md), [Architecture](docs/architecture.md), and the
[Testing harness](docs/testing-harness.md) for the full contracts and verification flow.
Coding agents start with the versioned [project memory](.agents/memory/README.md).
