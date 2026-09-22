# PostgreSQL schema evolution

`ByzantineSystems.Automata.Storage.Postgres` embeds its SQL and applies it through
`Migrator.migrate`. The schema requires PostgreSQL 18 and one extension, `btree_gist`, for the
temporal keys. `btree_gist` is trusted, so a role with `CREATE` on the database can install it
without superuser. The command inbox on its own still needs no extension.

## Where SQL lives

Two embedded trees, with different lifecycles and different jobs.

- `migrations/main/*.sql` are ordered and journaled. Each script runs once. They are numbered in
  **dependency order**, which is why no script contains an `ALTER TABLE`: every table is created
  complete, so its definition is the whole truth about it.
  - `000_bootstrap.sql` holds what needs a right on the database rather than on a schema:
    `btree_gist` and `CREATE SCHEMA fsm`.
  - `001_chart_version.sql` is `fsm.machine_chart_version`. It precedes the inbox because
    `fsm.command` references it.
  - `002_command.sql` is `fsm.command`, its domains, constraints and indexes.
  - `003_instance_state.sql` is the bitemporal belief table and its history twin.
  - `004_supervision.sql` is the supervision audit log.
- `migrations/repeatable/*.sql` are reapplied whenever their content changes. Routines live in
  `R__command_routines.sql`, `R__chart_routines.sql` and `R__temporal_routines.sql`, one file per
  domain, as `CREATE OR REPLACE`, so editing a routine body is an edit to its own migration
  rather than a new file. `R__temporal_routines.sql` also carries a `CREATE OR REPLACE TRIGGER`,
  because `main` runs before `repeatable` and a trigger declared beside its table would reference
  a function that does not exist yet.
- `sql/<domain>/<operation>.sql` are the statements the application sends, loaded by
  `SqlResources` into a map keyed by domain and operation. They are not migrations. Putting them
  in files rather than F# string literals is what lets `pg_format` reach them through `nix fmt`,
  and what gives a `WHERE` clause that must not drift from an index predicate somewhere to say
  so at length.

## Rules the schema keeps

**Routines are the only mutation surface.** Nothing writes `fsm.command` directly. That is what
lets the per-entity head invariant, the gapless sequence and the lease fence be reasoned about in
one place rather than at every call site.

**No column is nullable.** Absence always has a value: `'-infinity'` for an unset instant,
`'infinity'` for an open upper bound, the empty string for unsupplied text, `0` for no lease. A
`CHECK` constraint passes when its expression is true *or* null, so a nullable column quietly
turns a constraint into a suggestion. `SchemaContractTests` fails if a nullable column appears.

**Three predicates must not drift apart.** `command_claim_idx`, `fsm.claim_commands` and the
`runnable` bucket of `fsm.command_metrics` all state the same condition. A claim whose `WHERE`
clause has drifted from its index still returns correct rows; it just stops using the index, and
nothing fails to tell you. The plan test in `SchemaContractTests` is what notices.

**The reader's column contract is a test, not a generator.** `PostgresCommandInbox` reads by
column name, and `SchemaContractTests` asserts the exact column list each query returns. Adding a
column to `fsm.command` is therefore a failing test rather than a runtime surprise.

## Temporal tables

A table opts into system-time versioning by having a `system_time tstzrange` column, a twin named
`<table>_history`, and the shared trigger attached:

```sql
CREATE TABLE fsm.<t>_history (LIKE fsm.<t> INCLUDING DEFAULTS INCLUDING CONSTRAINTS);

CREATE OR REPLACE TRIGGER <t>_versioning_trigger
    BEFORE INSERT OR UPDATE OR DELETE ON fsm.<t>
    FOR EACH ROW EXECUTE FUNCTION fsm.temporal_versioning ();
```

`fsm.temporal_versioning` reads the table name at fire time and finds the twin by that naming
convention, so one function serves every temporal table.

The two omissions from `LIKE` are deliberate. **Not `INCLUDING INDEXES`**, because a history twin
exists to hold superseded and therefore overlapping rows, and a temporal key would reject them.
**Not `INCLUDING GENERATED`**, because the trigger writes rows verbatim through
`INSERT … SELECT ($1).*`, which a generated column cannot accept. `INCLUDING CONSTRAINTS` is
wanted: the twin has neither the temporal key nor the trigger, so the `CHECK` constraints are its
only defence against a zero-width row.

Valid-time mutations go through `fsm.close_and_open`, never through a direct `UPDATE` of
`valid_during`, so the close-then-insert split exists in one place.

## Adding a change

1. Add a new zero-padded script under `migrations/main`. Never edit a numbered migration that may
   already have shipped. Until 1.0 the schema is still being cut, and renumbering the set is
   allowed while nothing is deployed; after that it is not.
2. Edit the matching `R__*_routines.sql` in place when a routine changes; it is reapplied on
   content change. Keep its result shape compatible with the reader, or change both in the same
   release. A new domain of routines gets its own repeatable file.
3. Declare volatility, parallel safety and `search_path` on every new routine. The first two are
   promises the planner acts on. The third stops an unqualified function resolving through the
   connecting role's `search_path`.
4. Return a value from every routine, never `void`. Npgsql has no codec claiming `void`'s
   typsend, so the decoder receives an unknown OID carrying an empty payload.
5. Run `make db-reset` against a disposable database, then `make test-integration` with
   `AUTOMATA_TEST_DB` set.

`make db-reset` is destructive: it drops the `fsm` schema and the DbUp journal before rerunning
migrations. Use it only on local disposable databases.

## Chart versions while iterating

`fsm.machine_chart_version` records the fingerprint of the chart that first claimed each declared
version, and never overwrites it. Editing a chart's structure without bumping its `ChartVersion`
therefore reports a mismatch, which is the point.

While iterating locally that will happen often, and the remedy is deliberately manual: bump the
version, or run `make db-reset`. No routine forgets a registration. One would be reached for by
habit, and the tripwire only works if disarming it is inconvenient. The foreign key also refuses
to delete a version that any command still references, so there is no quiet way around it.

## Deferred: partitioning `fsm.command`

`fsm.command` is deliberately not partitioned, and that is a decision with a price attached.

Adding `PARTITION BY RANGE (received_at)` later is a table rewrite, and it is not only a
rewrite: a partitioned table's unique constraints must contain every partition-key column, so
`command_unique_idem` and `command_unique_seq` would both have to gain `received_at`. That
changes what "unique" means. An idempotency key would become unique per key *and arrival time*
rather than outright, which is not the invariant the inbox needs, so partitioning would require
rethinking deduplication rather than just adding a clause.

Deferring is cheap only while retention keeps the table small, which is an assumption a
benchmark should test rather than a fact. The partition-lifecycle shape to adopt, if it comes to
that, is a creation call in the migration, a recurring job that creates future partitions, and a
*blocking* boot check: a dead scheduler or a restored old backup otherwise leaves ingestion
facing a table that cannot accept inserts.
