# Repository agent instructions

## Start here

Read `.agents/memory/README.md`, `.agents/memory/current-status.md`,
`.agents/memory/decisions.md`, and `.agents/memory/backlog.md` before planning or editing.
They record the current milestone, settled trust boundaries, known gaps, and verification
contract. At the End of slice, update current status, append new decisions, reconcile the
backlog, and use `.agents/memory/handoff-template.md` whenever the work changed those
facts.

## Non-negotiable trust boundaries

- The controller owns workflow topology, budgets, tool allowlists, entity identity,
  deterministic arithmetic/ranking, evidence binding, and persisted lifecycle state.
- Model output is untrusted structured input. Validate it before it can affect tools,
  candidates, rankings, citations, or run status.
- Every displayed financial fact or derived metric must retain exact observation and
  evidence lineage. Citation checks establish attribution; do not describe them as
  proof that narrative text is semantically entailed by a source.
- Replay is a pure reduction of the append-only journal. It must not call models,
  providers, tools, or the network.
- The default development gate is deterministic and CPU-only. Verifier-launched Python
  processes deny standard Internet socket and DNS paths, but this is process-level
  defense rather than OS isolation. Never load `.env` from tests, scripts, or CI.
  Live-provider checks must be explicit and separate from the default gate.
- Synthetic or fixture results must be labeled as such in domain values and UI copy.
  Do not present them as live coverage or investment recommendations.

## Required verification

From the repository root, run:

```bash
.venv/bin/python scripts/verify.py
```

This is the release-blocking local gate. Its eight stages check project-memory integrity,
self-test Python-process network denial, run Ruff and strict pytest, execute and replay
the deterministic fan-out/fan-in and 300-issuer automatic-universe harnesses, smoke-test
the installed package, and exercise CLI help. The guard covers default Python code paths,
not non-Python or deliberately unisolated child processes. Add a focused regression test
for every bug or trust-boundary change before running the full gate.

Formatting is not yet a blocking gate because the repository has acknowledged legacy
formatting drift. Do not add `ruff format --check .` incidentally or reformat unrelated
files; make that a dedicated cleanup.

## Development workflow

Use `main <- dev <- codex/<feature>`:

1. branch from an up-to-date `dev`;
2. keep the feature diff scoped and preserve unrelated user/agent changes;
3. run the one-command gate;
4. open a feature-to-`dev` PR and require green CI plus review;
5. release with a separate `dev`-to-`main` PR after integration CI is green.

Do not commit directly to `main` or `dev`. Do not rewrite shared history. Prefer small,
reviewable commits that keep tests and implementation together.

## Code ownership map

- `fixed_dag.py`, `controller.py`, and `reducer.py`: scheduling, lifecycle, replay, and
  controller trust boundaries.
- `data/` and `calculations/`: normalized observations, lineage, deterministic formulas,
  and ranking.
- `offline_harness.py` and `scripts/verify.py`: hermetic executable acceptance contract.
- `tui/` and `adapters/demo.py`: presentation and explicitly synthetic demo behavior.
- live-provider/model adapters: external-I/O boundary; keep them out of default tests.

When concurrent agents are active, announce the files you own before editing and avoid
overlapping another agent's live files without coordinating first.
