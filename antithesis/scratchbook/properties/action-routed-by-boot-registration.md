# action-routed-by-boot-registration

## Evidence trail

- Identified from: E7 and C4 fix (commit fbf059f): `finalize_command` reads
  `machine_maintenance.action_queue`; missing registration with actions
  raises `invalid_parameter_value` containing "action delivery is not
  registered"; `ActionDelivery.Deliver|Discard` explicit at both providers;
  `ActionQueueName` validates pgmq's alphabet and 47-char limit.
- Code paths: `sql/command/finalize.sql` (no queue parameter);
  `R__finalize_routines.sql`; `PostgresMachineStore` boot registration;
  SQLite `MachineStoreOptions.ActionDelivery`.
- Failure scenario: two boots racing registration; commit racing a re-boot
  that changes the registration (PostgreSQL reads registration inside the
  finalize transaction, which is what must hold).

## Instrumentation

- Workload-side: queue-content assertion keyed by registered name. SUT-side:
  none; missing.

## Open Questions

- None.
