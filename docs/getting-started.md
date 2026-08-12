# Getting started

The current milestone is a deterministic, offline terminal prototype. It does not use
real company data or start KernelCubed.

## Install

From WSL:

```bash
cd /home/base/AlphaOrchestration
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
```

## Launch

Start a synthetic run immediately:

```bash
.venv/bin/python -m alpha_orchestration --demo
```

Open the mission setup screen:

```bash
.venv/bin/alpha-orchestrate
```

Run the same flow as plain terminal output:

```bash
.venv/bin/alpha-orchestrate --plain --demo-delay 0
```

Controls inside the full-screen run view:

- `Space`: pause or resume event consumption
- `C`: cancel and drain the run
- `R`: restart the same mandate
- `N`: return to mission setup
- `?`: show help
- `Q`: quit

## Artifacts and replay

Full-screen and plain runs write an append-only journal to
`artifacts/runs/<run-id>/events.jsonl`. Summarize a saved journal with:

```bash
.venv/bin/alpha-orchestrate --replay artifacts/runs/<run-id>/events.jsonl
```

Capture a deterministic SVG of the completed dashboard for visual QA:

```bash
.venv/bin/python scripts/capture_tui.py artifacts/tui-demo.svg
```

## Optional data adapter

Install yfinance only when working on the live data plane:

```bash
.venv/bin/pip install -e ".[data]"
```

SEC JSON access requires a descriptive identity with contact information:

```bash
export ALPHA_SEC_USER_AGENT="AlphaOrchestration your-email@example.com"
```

The live provider clients are adapter foundations only; the TUI intentionally stays on
synthetic fixtures until tool policy, source/evidence ledgers, and model-output
validation are wired end to end.

## Verify

```bash
.venv/bin/pytest
.venv/bin/ruff check .
```

