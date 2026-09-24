# PostgreSQL schema evolution

`ByzantineSystems.Automata.Storage.Postgres` embeds its SQL and applies it through `Migrator.migrate`. The schema requires PostgreSQL 19 and two extensions.

- **`btree_gist`**, for the temporal keys. It is trusted, so a role with `CREATE` on the database can install it without superuser.
- **`pgmq`**, for action delivery. It is **not** trusted and needs a superuser, which makes `000_bootstrap.sql` a genuine DBA step rather than one only in principle.

The command inbox on its own still needs no extension. A deployment that only submits and claims commands runs on a host where nobody may install anything.

## Where SQL lives

Two embedded trees, with different lifecycles and different jobs.

- `migrations/main/*.sql` are ordered and journaled. Each script runs once.
- `migrations/repeatable/*.sql` are reapplied on **every** migration, not only when their content changes
- `sql/<domain>/<operation>.sql` are the statements the application sends, loaded by SqlResources` into a map keyed by domain and operation. They are not migrations. Putting them in files rather than F# string literals is what lets `pg_format` reach them through `nix fmt`, and what gives a `WHERE` clause that must not drift from an index predicate somewhere to say so at length.

## Rules the schema keeps

**Routines are the only mutation surface.** Nothing writes `fsm.command` directly.

**No column is nullable.** Absence always has a value: `'-infinity'` for an unset instant, `'infinity'` for an open upper bound, the empty string for unsupplied text, `0` for no lease.

**The action queue is the application's to create.** `fsm.ensure_action_queue` is called by the store's boot, not by a migration: a queue's name is configuration, and one machine's queue is not another's. 

**Four predicates must not drift apart.** `command_claim_idx`, `fsm.claim_commands`, the `runnable` bucket of `fsm.command_metrics` and the command probe in `fsm.notify_pending` all state the same condition. 

## Action delivery, and why pgmq is wrapped

A commit enqueues its actions through `pgmq.send`, inside `fsm.finalize_command`, in the same transaction as the transition and the belief. 

## Temporal tables

A table opts into system-time versioning by having a `system_time tstzrange` column, a twin named `<table>_history`, and the shared trigger attached:

```sql
CREATE TABLE fsm.<t>_history (LIKE fsm.<t> INCLUDING DEFAULTS INCLUDING CONSTRAINTS);

CREATE OR REPLACE TRIGGER <t>_versioning_trigger
    BEFORE INSERT OR UPDATE OR DELETE ON fsm.<t>
    FOR EACH ROW EXECUTE FUNCTION fsm.temporal_versioning ();
```

`fsm.temporal_versioning` reads the table name at fire time and finds the twin by that naming convention, so one function serves every temporal table.

## Corrections, and the one routine allowed to write behind the live belief
