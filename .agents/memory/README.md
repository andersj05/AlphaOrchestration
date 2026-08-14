# Project memory protocol

This directory is the concise, durable handoff between humans and coding agents. It is
versioned with the code and must contain decisions and current state only—never secrets,
credential values, raw prompts, transcripts, or copied run logs.

- [Current status](current-status.md)
- [Decision log](decisions.md)
- [Prioritized backlog](backlog.md)
- [Handoff template](handoff-template.md)

## Startup

Before planning or editing:

1. read this protocol and `AGENTS.md`;
2. read current status, decisions, and backlog;
3. inspect the worktree and current branch rather than assuming the recorded state is
   newer than Git; and
4. announce file ownership when other agents are active.

## During work

- Treat settled decisions as constraints unless the user explicitly changes them.
- Record a new dated decision when a trust boundary, interface, or release policy
  changes. Decision entries are append-only: supersede an old entry with a new one; do
  not rewrite history.
- Keep the backlog prioritized and outcome-oriented. Do not use it as a transcript.
- Do not copy `.env`, tokens, provider payloads, or journal/run contents here.

## End of slice

Before handoff:

1. update `current-status.md` with the actual milestone and verified baseline;
2. append any new decisions and reconcile the backlog;
3. use the handoff template in the final report;
4. run `python scripts/check_project_memory.py`; and
5. run the relevant focused tests and `.venv/bin/python scripts/verify.py` when the
   change is implementation-bearing.

## Content rules

Memory files stay short and link to durable code or documentation instead of embedding
large output. Dates use `YYYY-MM-DD`. Status claims must say whether behavior is live,
fixture-only, partial, or deferred. Citation validation must be described as attribution,
not semantic entailment.
