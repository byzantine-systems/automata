# finalize-fenced-by-lease-and-epoch

## Evidence trail

- Identified from: `Storage/Processing.fs:103-125` (lease/epoch fencing
  contract); C0 equations "fencing conservation" and "epoch atomicity";
  `AlreadyFinalized` typed in C6 (d4b0a0d).
- Code paths: `finalize_command` (`R__finalize_routines.sql`): token match,
  expected-epoch match, `LeaseLost`/refused outcomes; SQLite equivalent in
  `SqliteCommandProcessorStore`.
- Failure scenario: lease expires while a throttled worker is between claim
  and commit; takeover worker commits; the original's finalize arrives last.
- Key observation: the contract model asserts `LeaseLost`-with-no-write
  deterministically; the expiry-racing-commit interleaving needs fault
  scheduling.

## Instrumentation

- Workload-side: state-unchanged assertion after forced stale finalize.
  SUT-side: none; missing.

## Open Questions

- None.
