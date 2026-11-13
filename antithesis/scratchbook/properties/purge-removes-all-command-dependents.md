# purge-removes-all-command-dependents

## Evidence trail

- Identified from: E1 — `purge_commands` deleted a doomed command's
  transition, error and dead letters but not its `command_correction` row;
  the NO ACTION FK then failed every subsequent `run_maintenance` for every
  machine. Fixed in C1 (commit 90e5ed3) by adding the corrections CTE.
- Code paths: `purge_commands` (`R__maintenance_routines.sql:172-174` area);
  FK `command_correction -> command` (`009_correction.sql`, NO ACTION).
- Guard (C1 finish, landed): a schema-contract test reads `pg_constraint` for
  every FK into `<schema>.command` and `pg_get_functiondef` for the routine's
  delete targets, so the next child table cannot reintroduce the wedge.
  Mutation-validated: removing the corrections CTE fails it, naming
  `command_correction`.
- Failure scenario: any future child table (or the historical correction
  case) aged past the horizon.

## Instrumentation

- Workload-side: maintenance-succeeds assertion across an age sweep. SUT-side:
  the catalog-based contract test is the deterministic oracle.

## Open Questions

- None.
