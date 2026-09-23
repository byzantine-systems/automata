# PostgreSQL schema evolution

`ByzantineSystems.Automata.Storage.Postgres` embeds its SQL and applies it through
`Migrator.migrate`. The schema requires PostgreSQL 19 and two extensions.

- **`btree_gist`**, for the temporal keys. It is trusted, so a role with `CREATE` on the database
  can install it without superuser.
- **`pgmq`**, for action delivery. It is **not** trusted and needs a superuser, which makes
  `000_bootstrap.sql` a genuine DBA step rather than one only in principle.

The command inbox on its own still needs no extension. A deployment that only submits and claims
commands runs on a host where nobody may install anything.

## Where SQL lives

Two embedded trees, with different lifecycles and different jobs.

- `migrations/main/*.sql` are ordered and journaled. Each script runs once. They are numbered in
  **dependency order**, which is why no script contains an `ALTER TABLE`: every table is created
  complete, so its definition is the whole truth about it.
  - `000_bootstrap.sql` holds what needs a right on the database rather than on a schema:
    `btree_gist`, `pgmq` and `CREATE SCHEMA fsm`.
  - `001_chart_version.sql` is `fsm.machine_chart_version`. It precedes the inbox because
    `fsm.command` references it.
  - `002_command.sql` is `fsm.command`, its domains, constraints and indexes.
  - `003_instance_state.sql` is the bitemporal belief table and its history twin.
  - `004_transition.sql` is the append-only transition log.
  - `005_supervision.sql` is the supervision audit log.
  - `006_action.sql` is `fsm.action_dead_letter`, the record of effects that never happened.
- `migrations/repeatable/*.sql` are reapplied whenever their content changes. Routines live in
  `R__command_routines.sql`, `R__chart_routines.sql`, `R__temporal_routines.sql`,
  `R__finalize_routines.sql` and `R__action_routines.sql`, one file per domain, as `CREATE OR REPLACE`, so editing a routine body is an edit to its own migration
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

**The action queue is the application's to create.** `fsm.ensure_action_queue` is called once at
startup, not by a migration: a queue's name is configuration, and one machine's queue is not
another's. It has to happen before any command commits, because a commit enqueues into it inside
its own transaction and a missing queue would fail the commit rather than only the delivery.

**Three predicates must not drift apart.** `command_claim_idx`, `fsm.claim_commands` and the
`runnable` bucket of `fsm.command_metrics` all state the same condition. A claim whose `WHERE`
clause has drifted from its index still returns correct rows; it just stops using the index, and
nothing fails to tell you. The plan test in `SchemaContractTests` is what notices.

**The reader's column contract is a test, not a generator.** `PostgresCommandInbox` reads by
column name, and `SchemaContractTests` asserts the exact column list each query returns. Adding a
column to `fsm.command` is therefore a failing test rather than a runtime surprise.

## Action delivery, and why pgmq is wrapped

A commit enqueues its actions through `pgmq.send`, inside `fsm.finalize_command`, in the same
transaction as the transition and the belief. That is the whole transactional-outbox property
with no outbox table: `pgmq.send` is an ordinary insert into an ordinary table, so it commits or
rolls back with the transition it belongs to. A commit whose effects were not queued, and queued
effects for a commit that did not happen, are both unrepresentable.

**Nothing in F# calls `pgmq.archive` or `pgmq.set_vt`.** Those two take a `msg_id` and ignore
`read_ct`, so they cannot tell the lease holder from a worker that stalled past its visibility
timeout and woke up after somebody else reclaimed the message. The effect is delivered twice and
acknowledged once.

`read_ct` is the fencing token the design needs and pgmq already maintains: every read increments
it, so the value a worker was handed at claim time is stale the moment anyone else reads the
message. `fsm.complete_action`, `fsm.reschedule_action` and `fsm.abandon_action` each take the row
lock first and compare it second, in one transaction, so the comparison cannot be overtaken
between checking and acting. That comparison is the only reason these routines exist.

A queue belongs to one machine. Sharing one between machines would need a predicate on the claim,
and pgmq cannot express one without scanning the queue. The name reaches dynamic SQL as an
identifier rather than a parameter, so `fsm.assert_queue_name` applies the `^[a-z_][a-z0-9_]*$`
allowlist in the database as well as in the machine builder: the layer that does the interpolating
is the layer that cannot afford to assume.

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
`valid_during`, so the split exists in one place. It delegates the actual
`UPDATE … FOR PORTION OF` to `fsm.split_belief`, because that clause will not accept a plpgsql
variable as a bound: the bound expressions are parsed without plpgsql's variable substitution, so
a variable there reads as a column reference and the statement fails. A SQL function's parameters
work, which is what `fsm.split_belief` is.

## The transition log and the epoch

`fsm.transition` is append-only, one row per finalized command that changed state. Its primary
key `(machine_id, entity_id, epoch)` makes the epoch gapless per entity, and
`transition_unique_command` makes one command's transition unique, so a retried finalize cannot
append twice however the routine above it behaves.

Nothing serialises finalizers explicitly. At most one command per entity is claimable, so at most
one worker can be finalizing an entity at a time; the key is the backstop for that reasoning
rather than the mechanism behind it. `fsm.instance_state.epoch` denormalises the same value so a
current-state read is one lookup rather than a join.

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
