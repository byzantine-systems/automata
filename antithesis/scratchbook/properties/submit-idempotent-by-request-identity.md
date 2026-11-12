# submit-idempotent-by-request-identity

## Evidence trail

- Identified from: E8 and C5 fix (commit b2dee03): `request_digest bytea`
  computed in-database (sha256 over canonical request), `AlreadySubmitted`
  returns the original id, `Refused(KeyReused existing)` on digest mismatch;
  runtime no longer bypasses submission validation
  (`Runtime/Machine.fs:444-455`).
- Code paths: `submit_command`, `submit_correction` (`R__command_routines.sql`,
  `R__correction_routines.sql`); `PostgresCommandInbox.Submit`;
  `SqliteCommandInbox.Submit` (same typed refusal).
- Failure scenario: client retries the original submission under connection
  loss (never saw the first answer); or races a different request under the
  same key.
- Key observations: locally mutation-validated (disabling the PostgreSQL
  digest comparison failed the law immediately). jsonb normalization makes
  key-reordered payloads digest equally.

## Instrumentation

- Workload-side: single-row assertion per key plus id stability. SUT-side:
  none; missing.

## Open Questions

- None.
