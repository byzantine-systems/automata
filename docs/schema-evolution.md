# PostgreSQL schema evolution

The PostgreSQL provider defaults to current snapshots plus the committed transition log.
Bitemporal history is an explicit installation and store-type choice. PostgreSQL 19 is
required for the optional temporal keys. The current layout is a **clean-reset boundary**
relative to earlier releases: these scripts do not preserve or upgrade pre-existing data.

## Migration layout

```text
migrations/
  common/
    main/          # core tables, domains, profile, and inbox consistency view
    repeatable/    # shared transaction primitives, inbox, actions, maintenance
  snapshot/
    repeatable/    # guarded snapshot commit entry point
  bitemporal/
    main/          # temporal tables, seed, and belief views
    repeatable/    # temporal commit paths, trigger, correction, retention
```

`common` always installs. Its numbered scripts are journaled in `<schema>.schemaversions`;
its repeatable scripts run on every migration. The selected profile is applied independently:
snapshot uses `<schema>.snapshot_schemaversions`, and bitemporal uses
`<schema>_temporal.schemaversions`. Each set can add numbered migrations without coordinating
its numbering with the others. The bitemporal profile installs an explicit refusal at the
old snapshot entry point, so an old client cannot bypass history.

The same `$schema$` and `$temporal_schema$` substitution applies to migrations and embedded
`sql/` statements. `SchemaName` validates the identifier before substitution. Unknown named
dollar tokens are packaging defects; use anonymous `$$` for PostgreSQL function bodies.

## Core and optional objects

`<schema>` contains the chart registry, command inbox and correction requests, errors,
transitions, actions, maintenance registrations, supervision, purge watermark, `entity_state`,
and `store_profile`. Correction requests remain inbox facts even if a particular worker
does not support them.

`entity_state` is the **only epoch authority in both modes**. Ordinary commits and replayed
corrections advance it atomically with the transition and closed command. Erasing an operator
belief timeline cannot reset an entity's epoch. Its command FK pins the producing successful
command: `purge_commands` excludes snapshot-referenced commands, even if a later rejected
command is now the newest inbox entry.

`<schema>_temporal` contains `instance_state`, `instance_state_history`, the versioning trigger,
split/open routines, `belief`, `current_belief`, `belief_cold`, and correction/history-purge
routines. Core snapshot operation requires none of those objects.

No stored column is nullable. Optional values use the schema's documented sentinels. User
state, events, and actions remain codec-backed payloads.

## Failure, event, and origin ownership

A command failure is relational: `command_error` is a discriminant header with exactly one child
among `command_error_domain`, `command_error_machine`, and `command_error_replay`. Only the domain
error is a codec-backed JSON payload; machine reasons and replay metadata are typed columns with
checked discriminants and sentinel-filled fields. The header and child are written in one
transaction, and `purge_commands` removes the complete transitive graph. Supervision reasons are
plain `text`.

`transition` stores no event column. History, result, and replay-suffix reads join the submitted
command by its primary key, so the event is owned once and never rewritten: PostgreSQL rejects
updates naming `command.event`, and SQLite already refuses every submitted request column. An
ordinary continuing commit derives its origin from the locked `entity_state` row; only the first
commit supplies an initial origin, and corrections retain an explicit source. A single destination
payload feeds the transition, the snapshot, and the optional timeline.

Temporal corrections travel as six typed arrays projected from ordered belief rows. The database
validates cardinality, nulls, and ordering before rewriting. An empty operator batch erases a
suffix; a committed replay requires at least one belief. A user state that encodes JSON `null` is
distinct from SQL `NULL`.

Corrupt action envelopes that carry no usable business identity are preserved in
`action_quarantine` keyed by queue and message id, and archived under the same fence and
transaction. Its retention shares the action-archive batch budget. A poison message cannot strand
the healthy rows claimed alongside it.

## Install, reset, and opt in

Snapshot is the default:

```sh
make db-reset SCHEMA=fsm BELIEFS=snapshot
```

Build a snapshot store with `MachineStoreOptions.forEntityId ... |> PostgresMachineStore`.
Temporal and correction capability discovery returns `None`.

To opt in on an existing **current-layout** snapshot installation:

```sh
make migrate SCHEMA=fsm BELIEFS=bitemporal
```

Then use `options |> MachineStoreOptions.bitemporal |> PostgresBitemporalMachineStore`.
Direct tool invocation accepts `--beliefs bitemporal` or `BS_AUTOMATA_BELIEFS=bitemporal`.
Host options bind `Automata:Postgres:Beliefs` and validate `snapshot` or `bitemporal` on start.

Opt-in takes the profile and snapshot locks for the complete temporal migration transaction.
It seeds one open valid-time belief per core snapshot, starting at its `effective_at`, with
knowledge time beginning at opt-in. Earlier opinions are unknown, not reconstructed. The
seed and final `store_profile` flip commit together. Boot checks configured type against
profile; the snapshot commit routine also refuses a bitemporal profile.

The migrator refuses automatic bitemporal-to-snapshot changes. For a deliberate destructive
downgrade, stop workers first, unschedule this installation's jobs, then explicitly remove the
module and change the profile:

```sql
SELECT fsm.unschedule_maintenance();
BEGIN;
DROP SCHEMA fsm_temporal CASCADE;
UPDATE fsm.store_profile SET beliefs = 'snapshot' WHERE singleton;
COMMIT;
```

Reapply snapshot migrations and restart only snapshot clients. `make db-reset` is the simpler
fully destructive alternative; it removes this installation's cron jobs and owned pgmq queues,
then drops both schemas and installs the requested profile.

## Extensions and database-global names

`pgmq` uses its extension-defined `pgmq` schema. Bitemporal bootstrap additionally installs
`btree_gist WITH SCHEMA public`. A caller's `search_path` cannot place it into the droppable
Automata schema. An administrator must preinstall extensions when the migration role lacks
the required privileges. Snapshot boot does not require `btree_gist`.

NOTIFY channels, command/maintenance advisory keys, cron jobs, and journals are namespaced
by the validated schema. Queue names are caller-configured and database-global;
`ensure_action_queue` records ownership on the physical queue table and refuses another
schema's ownership at boot. Action routing comes from the boot registration. `Discard`
records transition actions without delivering them.

## Temporal clocks and retention

Both time axes use half-open ranges. The trigger stamps knowledge with
`GREATEST(STATEMENT_TIMESTAMP(), LOWER(OLD.system_time))`. A superseded version whose lower
bound equals that instant was never observable and is not archived as a zero-width range.
`ValidAtAsync` reads the current database opinion, not an application clock. `BeforeAsync`
uses a strict business-time boundary rather than subtracting a provider-specific tick.

The belief view unites current opinions, recent history, and cold materialized history.
The stored UTC cutoff is the boundary, so a delayed refresh costs work without losing rows.
Staleness comes from `max(cutoff)` in `belief_cold`; there is no separate refresh marker for
pg_cron to forget. Empty cold views may be refreshed again until history exists.

History purge is indexed on `(machine_id, upper(system_time))` and bounded by ordered
`LIMIT ... FOR UPDATE SKIP LOCKED`. `RetentionPolicy.BeliefHistory` applies only to bitemporal
history; snapshot boot emits a diagnostic warning if a finite value is configured.
**Closed valid-time rows in `instance_state` are retained indefinitely.** They are not system-time
history and are not removed by this retention setting.

`ITransitionLog.SuffixAsync` reads the suffix and then the monotonic purge watermark. A suffix
starting at or before the greatest purged effective time is incomplete. Corrections therefore
do not mistake surviving epoch order for complete business-time history.

## Row-write evidence

The integration test counts transaction-local PostgreSQL tuple inserts plus updates. For a
single entity, empty action list, and no queued successor, the measured counts are:

| Profile | First commit | Subsequent commit |
|---|---:|---:|
| Snapshot | 3 | 3 |
| Bitemporal | 4 | 6 |

The three core writes are the transition, snapshot, and command. The temporal split additionally
inserts the carried interval, updates the current belief, and archives the old opinion. Actions
and successor promotion add their own writes. These counts do not make a latency or byte-volume
claim.

## Generated types and checks

Generation targets the default `fsm` **snapshot** installation:

```sh
make db-reset SCHEMA=fsm BELIEFS=snapshot
make schema-generate SCHEMA=fsm BELIEFS=snapshot
make schema-check SCHEMA=fsm BELIEFS=snapshot
```

Runtime statements are schema-specialized SQL with named decoders; generated types do not
choose the runtime schema. `make test-integration-schema` runs both schema names and includes
snapshot and bitemporal conformance plus their mismatch, opt-in, temporal-boundary, and custom
correction tests. See [corrections](corrections.md) for the three-operation extension surface.
