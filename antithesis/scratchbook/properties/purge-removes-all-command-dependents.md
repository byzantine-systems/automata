# purge-removes-all-command-dependents

## Evidence trail

- Identified from: E1 — `purge_commands` deleted a doomed command's
  transition, error and dead letters but not its `command_correction` row;
  the NO ACTION FK then failed every subsequent `run_maintenance` for every
  machine. Fixed in C1 (commit 90e5ed3) by adding the corrections CTE.
- Code paths: `purge_commands` (`R__maintenance_routines.sql:172-174` area);
  FK `command_correction -> command` (`009_correction.sql`, NO ACTION).
- Missing guard (part of C1's finish): a schema-contract test reading
  `pg_constraint` for every FK into `<schema>.command` and comparing with the
  routine's delete targets, so the next child table cannot reintroduce the
  wedge. Planned in the B/C closure slice.
- Failure scenario: any future child table (or the historical correction
  case) aged past the horizon.

## Instrumentation

- Workload-side: maintenance-succeeds assertion across an age sweep. SUT-side:
  none; missing. The catalog-based contract test is the deterministic oracle.

## Open Questions

- None.
