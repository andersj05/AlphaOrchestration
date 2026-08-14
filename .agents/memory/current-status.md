# Current status

Last updated: 2026-08-13

## Snapshot

The first rule-based live-equity prototype is implemented on
`codex/feature-live-equity-prototype` and is pending a reviewed pull request into `dev`.
A real AAPL/MSFT/NVDA validation reviewed 3/3 requested issuers, retained 23 evidence
records, projected three ranked candidates, persisted and replayed 63 events, and
completed a successful cache rerun.

## Active milestone

Release the rule-based live prototype through the protected feature-to-`dev` workflow,
then collect review feedback. Keep live services separate from the offline gate. A
model-backed action bridge remains a later milestone.

## Current branch model

Use `main <- dev <- codex/<feature>`. Feature pull requests target `dev`; reviewed
release pull requests target `main`. GitHub branch-protection rules enforce pull
requests, resolved review conversations, administrator coverage, no force pushes or
branch deletion, and strict Python 3.11/3.12/3.13 verification checks on both branches.

## Verification baseline

The authoritative command is `.venv/bin/python scripts/verify.py`. The integrated gate
passed project-memory integrity, the Python-process network self-test, Ruff, 245 strict
tests, the 53-event deterministic harness with measured concurrency 3/3, package import
smoke, and CLI smoke. Python 3.11, 3.12, and 3.13 remain the required GitHub Actions
contexts.

## Known boundaries

- The rule-based SEC/yfinance live bridge is implemented and pending reviewed release
  integration. Model-backed action composition remains later work.
- Offline fixture attribution is strict per issuer; it does not prove narrative
  entailment.
- The default gate must never load `.env`. Its verifier-launched Python processes deny
  standard Internet socket/DNS paths, but the guard is not OS-level isolation and does
  not cover deliberately unguarded or non-Python children.
- `ruff format --check .` still reports repository-wide drift (27 files at the latest
  audit), including active live-work files; formatting remains a dedicated cleanup.
- Type checking, coverage policy, and dependency auditing are deferred controls.

## Next handoff

Report the feature pull request and required CI results, then capture reviewer feedback
on the live Results, evidence, partial-data, and cache experience. Keep the model-backed
action bridge explicitly deferred to its own milestone.
