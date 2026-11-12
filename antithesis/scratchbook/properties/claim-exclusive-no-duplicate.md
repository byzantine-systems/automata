# claim-exclusive-no-duplicate

## Evidence trail

- Identified from: C0 "claim exclusivity" equation; the lease mechanism in
  `claim_commands` (UPDATE ... SET lease/token ... RETURNING over claimable
  rows) in `R__command_routines.sql` and SQLite `claim.sql`.
- Code paths: `PostgresCommandInbox.Claim` (`PostgresCommandInbox.fs`),
  `SqliteCommandInbox.Claim`; lease columns `lease_token`, `lease_deadline`.
- Failure scenario: two worker processes claim concurrently (Profile 2) with
  throttling staggering statements; both receive the same command id.
- Key observation: single-process exclusivity is covered by the contract
  model; the cross-connection statement-level race is the untested surface.

## Instrumentation

- Workload-side: dedupe assertion over all workers' claimed ids. SUT-side:
  none; missing.

## Open Questions

- None specific.
