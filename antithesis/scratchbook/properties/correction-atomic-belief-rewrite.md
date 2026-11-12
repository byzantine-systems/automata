# correction-atomic-belief-rewrite

## Evidence trail

- Identified from: `Storage/Temporal.fs:165-212` (corrections archive, never
  erase; transactional rewrite) and E16 (empty correction leaving no live
  belief; fixed by making `CorrectionCommit.Beliefs` non-empty by type,
  commit 8adbda0).
- Code paths: `correct_beliefs` (`R__temporal_routines.sql`);
  `belief/correct.sql`; `PostgresTemporalStore.Correct`; the
  `temporal_versioning` trigger maintaining system-time history.
- Failure scenario: worker killed inside `correct_beliefs` between archive
  and rewrite; or between rewrite of two beliefs of one entity.
- Key observations: single-threaded correction laws exist (b004ca1,
  8adbda0); interruption windows are untested.

## Instrumentation

- Workload-side: exactly-one-live-belief and append-only-archive assertion
  after recovery. SUT-side: none; missing.

## Open Questions

- None.
