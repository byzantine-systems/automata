# PostgreSQL schema evolution

`ByzantineSystems.Automata.Storage.Postgres` embeds its SQL migrations and applies them through `Migrator.migrate`. The current schema requires PostgreSQL 18 because chart-version validity uses `WITHOUT OVERLAPS`.

## Migration sets

The embedded scripts have two lifecycles:

- `migrations/main/*.sql` are ordered, journaled migrations. Each script runs once.
- `migrations/repeatable/*.sql` are reapplied by DbUp. They contain replaceable database objects such as the leased claim functions.

The initial schema defines machine and chart-version metadata, entity snapshots, append-only transition history, the action outbox, durable retries, dead letters, and supervision events. Queue and outbox claims use leases with `FOR UPDATE SKIP LOCKED`, allowing multiple workers to claim work without selecting the same row.

## Adding a change

1. Add a new zero-padded script under `migrations/main` for table, column, constraint, or index changes. Never edit a numbered migration that may already have shipped.
2. Update `R__claim_functions.sql` when a replaceable claim function changes. Keep its result shape compatible with the store reader, or update the application in the same release.
3. Keep schema values non-null where the current model expects sentinels. In particular, unlocked leases use `-infinity`, open range bounds use `infinity`, and a missing retry error uses an empty string.
4. Run `make migrate` against a disposable PostgreSQL 18 database, then run the integration suite with `AUTOMATA_TEST_DB` set.
5. Test both a fresh database and an upgrade from the latest released schema before publishing.

`make db-reset` is destructive: it drops the `fsm` schema and DbUp journal before rerunning migrations. Use it only for local disposable databases.
