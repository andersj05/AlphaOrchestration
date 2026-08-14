# Backlog

Last reviewed: 2026-08-13

## Now

1. Release the rule-based live prototype through the reviewed feature-to-`dev` pull
   request, then through the separate `dev`-to-`main` release workflow when ready.
2. Gather reviewer feedback on Results interpretation, evidence, partial-data posture,
   cache behavior, and operational clarity; convert it into scoped follow-up changes.
3. Define the model-backed action bridge as a separate milestone that preserves
   controller-owned evidence binding, calculations, ranking, budgets, and replay.

## Next

1. Pin GitHub Actions and CI dependency resolution reproducibly.
2. Add evidence drill-through/export and normalize currency, period end, reported versus
   estimate, and market-data timestamps for real payloads.
3. Add ranking sort/filter controls after live payload density is understood.

## Later

1. Adopt type checking, a coverage policy, and dependency/security auditing as explicit
   gates with dedicated cleanup work.
2. Make formatting a blocking gate only after the documented repository-wide formatting
   cleanup.
3. Add minimum-supported dependency and schema-compatibility test lanes.
