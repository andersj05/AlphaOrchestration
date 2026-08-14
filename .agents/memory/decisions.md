# Decisions

## Protocol

Entries below are append-only and chronological. To change a decision, append a dated
entry that names the superseded decision and explains the replacement.

## Decision log

### 2026-08-12 — Controller-owned trust boundary

The controller owns workflow topology, concurrency and call budgets, tool policies,
evidence binding, deterministic calculations and ranking, candidate identity, and
terminal state. Model output remains untrusted bounded input.

### 2026-08-12 — Journal-first replay

The append-only journal is the durable source of truth. Replay is a pure integrity-
checked reduction and performs no model, tool, provider, or network calls.

### 2026-08-13 — Hermetic default gate

The default developer/CI gate is deterministic, CPU-only, network-denied, and independent
of `.env`, provider credentials, KernelCubed, and model hubs. Live checks are explicit
and separate.

### 2026-08-13 — Offline ranked-results projector

The three-issuer projector may rank only exact controller-bound branch tool results.
Validator rank and identity proposals cannot override it. Validator citations must
exactly match each issuer packet; this proves attribution, not semantic entailment. The
projector is an offline prototype contract, not the live-provider production bridge.

### 2026-08-13 — Integration and release branches

Feature work uses `codex/<feature>` from `dev`, integrates through a reviewed
feature-to-`dev` pull request, and releases through a separate reviewed `dev`-to-`main`
pull request after the complete CI matrix passes.

### 2026-08-13 — Python-process network guard scope

This clarifies the earlier “Hermetic default gate” decision. The verifier injects a
Python `sitecustomize` guard and offline environment into its child processes; direct
pytest commands install the equivalent fixture. These controls deny standard Internet
socket and DNS paths while preserving local IPC. They are defense for default Python
code paths, not an OS network namespace or firewall. Non-Python subprocesses, native
extensions, deliberately cleared guard variables, and lower-level system calls remain
outside this enforcement boundary.

### 2026-08-13 - Protected integration and release branches

GitHub rulesets now protect `dev` and `main` with required pull requests, resolved
review conversations, administrator enforcement, no force pushes or deletion, and
strict required verification contexts for Python 3.11, 3.12, and 3.13.

### 2026-08-13 - Rule-based live provider contract

This corrects the terminology in the protected-branches entry above: `dev` and `main`
use GitHub branch-protection rules, not rulesets.

A live run accepts an explicit operator-supplied universe of one to eight unique tickers.
The official SEC company-ticker map supplies the authoritative ticker/CIK identity used
to join provider data. Comparable SEC company-facts evidence is required for issuer
eligibility; yfinance market data is optional, and its absence produces an explicit
partial-data posture rather than synthetic fallback.

The controller owns normalized calculations, cross-issuer ranking, and candidate
projection. A provider request may use a cache record only while it is fresh and its
request reference and content hash pass integrity checks; otherwise the provider is
called or the operation fails closed.
