# correction-fenced-replay-bounded

## Evidence trail

- Identified from: correction finalization fence (dc6f462), exact replay
  retention (90e5ed3), live-correction-belief requirement (8adbda0), and the
  generated correction-finalization law (ede7999) — the four commits that
  made replay honest.
- Code paths: `commit_correction` / `finalize_command` correction arm
  (`R__correction_routines.sql`, `sql/command/finalize_correction.sql`);
  replay plan with original chart versions (`Runtime/Correction`);
  `HistoryPurged` typed refusal.
- Failure scenario: retention purge advances the watermark while a replay
  plan is being computed; lease expires mid replay-commit; replay budget
  boundary crossed exactly at a purged transition.
- Key observations: deterministic laws cover the model; the purge-racing-
  replay window is a genuine interleaving target.

## Instrumentation

- Workload-side: committed-belief-derived-from-complete-history assertion.
  SUT-side: none; missing.

## Open Questions

- None.
