# boot-refusal-leaves-store-unchanged

## Evidence trail

- Identified from: `Storage/Maintenance.fs:43-56` (boot check refuses with
  typed defects, store unchanged) and `Storage.Sqlite/SqliteBoot.fs:103-120`
  (defect list: version, journal mode, schema, journal drift).
- Code paths: `sql/system/boot_check.sql` (to_regnamespace, journal,
  routines); `SqlResources system/journal.sql`; PostgreSQL boot's idempotent
  configuration (queue creation via `ensure_action_queue`, maintenance
  registration) after the check; Phase D will route queue-ownership refusal
  (`BootDefect.Misconfigured`) through the same gate.
- Failure scenario: boot racing an operator's parallel migration (journal
  read mid-upgrade); boot's configuration step failing halfway (queue
  created, registration not written).
- Key observation: Phase D's second-schema-claims-queue refusal is the
  forward-looking motivation for keeping this property general.

## Instrumentation

- Workload-side: state-diff assertion around forced boot defects. SUT-side:
  `Reachable` markers on each `BootDefect` branch would guide search; all
  missing.

## Open Questions

- Mid-configuration fault path: partial — docs say check precedes
  configuration; the half-configured window is untested.

### Investigation Log

#### Mid-configuration fault path

- Examined: `PostgresMachineStore.boot` ordering (check, then
  `ensure_action_queue`, then maintenance registration), boot SQL
  (`sql/system/boot_check.sql`), `Storage/Maintenance.fs:43-56`.
- Found: the boot check precedes all configuration writes, so a failed check
  writes nothing; each configuration step is itself idempotent
  (ensure-queue, upsert registration).
- Not found: a test that kills the process between the configuration steps
  and re-boots, verifying the half-configured state is both detected and
  safely completable.
- Conclusion: `(partial: ordering and idempotence confirmed; interrupted-configuration window untested)`.
