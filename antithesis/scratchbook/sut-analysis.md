---
sut_path: /home/mbenevides/Code/Personal/byzantine-systems/automata
commit: 9bdc555187e735cdf31431de3515832d3d0fc9d3
updated: 2026-11-12
external_references: []
---

# SUT Analysis: ByzantineSystems.Automata

Provenance note: the Architecture/Product-Context findings below were surfaced by
an ensemble agent before sub-agent capacity ran out; the remaining focuses
(State, Concurrency, Safety, Liveness, Bug History, Test Strategy, Failure
Modes, Dependencies, Assumptions, Wildcard) were completed in single-agent mode
from direct code reads in this repository only. All guarantees listed are
**claims the system makes** (docs, comments, tests) that properties must test;
none is stated as holding.

## Architecture and data flow

A modular F#/.NET 10 statechart toolkit for durable entity workflows
(`README.md:25-36`, `docs/index.md:3-14`). Packages: Core (pure charts),
Storage (contracts, store algebra), Storage.Postgres, Storage.Sqlite, Runtime
(processor, dispatchers, Machine facade), Resilience (Polly pipelines,
supervisor), DependencyInjection (hosted services). Data flow: caller
submits an `EventEnvelope` through `ICommandInbox.Submit` -> per-entity
durable ordering -> worker `CommandProcessor.PollAsync` claims a batch ->
pure `Chart.resolve` (`src/ByzantineSystems.Automata.Core/Chart.fs:338-397`) ->
`ICommandProcessorStore.Commit` atomically finalizes -> `ActionDispatcher`
claims queued actions from an `IActionQueue` and delivers them at least once.
Callers and workers meet only through the database
(`examples/ByzantineSystems.Automata.Examples.Hosted/Program.fs:139-178`).
`Machine.send` waits via local completion notification raced against store
polling (`src/ByzantineSystems.Automata.Runtime/Machine.fs:396-463`).
Serialization boundary: System.TextJson codecs per machine
(`Storage/Serialization.fs`); every store I/O goes through the store-algebra
interpreter (`Storage.Postgres/Sql.fs:196-239`).

## State management and persistence

PostgreSQL schema `fsm` (migrations under
`Storage.Postgres/migrations/{main,repeatable}`): `command` with per-entity
`seq` and request digests, `command_correction`, `transition` log (epoch
ordered), `instance_state`/`belief` bitemporal views (valid time + system
time, `008_belief_views.sql`, `R__temporal_routines.sql` with
`temporal_versioning` trigger using `tg_table_schema`), pgmq action queues,
`entity_purge` retention watermark (C7), supervision audit. Leases are
database-stamped deadlines with 64-bit fenced tokens; epochs gate commits.
SQLite mirror: single-file WAL store with `IMMEDIATE`-transaction write gate,
claim-time lease reclamation, no temporal/correction/maintenance capability
(`Storage.Sqlite/SqliteMachineStore.fs:63-77`). Every commit is claimed to be
one transaction: transition + snapshot advance + action enqueue + command
close (`docs/index.md:9-12`, `sql/command/finalize.sql`). Durable retry lives
in the inbox (reschedule with database-computed backoff), deliberately not in
the Polly pipeline (`Resilience/TransientPolicy.fs:67-81`).

## Concurrency model

Per-entity serialization via PostgreSQL advisory locks taken inside
`submit_command`/`finalize_command` (`R__command_routines.sql`,
`R__maintenance_routines.sql:429`) and via SQLite `IMMEDIATE` transactions.
Workers: `PollAsync` fans a claimed batch over `SemaphoreSlim(policy.Concurrency)`
(`Runtime/CommandProcessor.fs:471-484`, `Runtime/ActionDispatcher.fs:242-257`);
long-running loops are recursive `task` computations with `return! loop`
(`CommandProcessor.fs:506-543`, `ActionDispatcher.fs:277-309`,
`ObserverDispatcher.drain`, `Core/Boundary.fs:96 Recurring.repeat`,
`Machine.fs:399 awaitSettled`) — a known defect (E41: per-iteration state
machine leak; stack overflow on synchronous full-batch drains). Work
wake-ups: bounded-channel signals + `IWorkNotifications` LISTEN/NOTIFY session
(`Storage.Postgres/PostgresWorkNotifications.fs:103-115`, channels
`fsm_command`/`fsm_action` — global names, E10). Time: `TimeProvider`
throughout the runtime, except `PostgresTemporalStore.fs:131`
(`DateTimeOffset.UtcNow`, E14/B9).

## Safety guarantees (claims to test)

- At most one non-terminal command per entity is claimable; per-entity order
  is `seq` order (`README.md:29`, `docs/public-api.md:81-83`).
- Submit idempotency by request identity: identical retry returns the original
  command id; different request under the same key is refused (`docs/public-api.md:81`).
- Arrival timestamps follow sequence order; earlier caller arrivals refused (C2).
- Commit is one atomic fact; ambiguous finalization is safely repeatable
  (`docs/public-api.md:73`).
- Lease tokens and snapshot epochs fence every finalize and every action
  acknowledgement (`Storage/Processing.fs:103-125`).
- Actions are delivered at least once, unordered; duplicates are the
  destination's contract (`Storage/Action.fs:39-78`).
- Corrections archive (never erase) superseded beliefs, transactionally, fenced
  (`Storage/Temporal.fs:165-212`).

## Liveness guarantees (claims to test)

- A terminal command releases its entity for the next claim; a failing command
  dead-letters rather than blocking forever (`docs/public-api.md:41-45`).
- Notifications are hints: losing all of them costs latency, never commands
  (`Storage/Maintenance.fs:58-70`); polling always runs underneath.
- Expired leases are reclaimed (PostgreSQL maintenance; SQLite claim-time).
- `send` settles once the durable outcome is known (`Machine.fs:423-463`).
  Known open defect: a command purged while awaited polls forever (E44).
- Processor escalations stop the loop for the owner to decide
  (`CommandProcessor.fs:487-494`).

## Failure and degradation modes

Driver failures are classified (`Cancelled|Unavailable|Violated|Unexpected`)
inside store adapters; Polly pipeline (total timeout -> retry -> breaker ->
attempt timeout) wraps only the driver call (`TransientPolicy.fs:175-200`).
Failed polls are logged and waited out, never fatal
(`CommandProcessor.fs:487-494`); failed claims are reported not raised. Poison
rows (undecodable payloads) are quarantined per row with the batch returned
(C3, `ClaimRecovery.collect`). Undeliverable actions are abandoned after
`MaxAttempts` with a durable record. Boot refuses with typed `BootDefect`s and
leaves the store unchanged (`Storage/Maintenance.fs:43-56`,
`Storage.Sqlite/SqliteBoot.fs:103-120`). Known defects: connection leak when
session construction throws (E19), SQLite `CloseAsync()` without token
(`SqliteBoot.fs:141`), `ofConnectionString` ownership hole (B9).

## Bug history and density

The `NEW_PLAN.md` edge register (E1-E60) plus git history show clusters:
correctness fixes C1-C7 (purge wedge, arrival ordering, poison rows, action
routing, key honesty, finalize reporting, retention watermark) all landed in
2026-11 (commits 90e5ed3..fbf059f) with reproduction tests; async/runtime
defects E41/E54/E55 remain open (B10/B11); resource/clock items E14/E19 open
(B9); global-name hazards E10-E12 open (Phase D). Quiet areas: SQLite-specific
concurrency (WAL busy behavior) and the DI hosted-service lifecycle have thin
test history — possibly undertested rather than correct.

## Existing test strategy

Expecto + FsCheck. Core carries the A2 law portfolio (chart/rule/fragment
laws, generators through public constructors, asserted coverage). Both SQL
providers link a shared `tests/StorageContractProperties.fs` generated
state-machine model (registry, inbox, finalize, idempotency, arrival order,
poison batches) — but per-provider duplicated contract tests still exist (C0
finish pending: dedicated conformance library, TAP-shape equations, C1 catalog
guard). Schema contract tests pin live-schema shapes; `verify-sqlhydra.sh`
checks generated types. No fault injection, no chaos, no long-duration
soak; timing-sensitive interleavings (lease takeover mid-commit, notification
loss under load, connection drop inside finalize) are exactly where coverage
stops — Antithesis's territory. Local mutation practice exists (each C-fix
proved its oracle) but no systematic mutation sweep.

## External dependencies and integration points

PostgreSQL 19 + `btree_gist` + `pgmq` (optional `pg_cron`) via Npgsql (auto-
prepare enabled, `DataSource.fs:34-38`); SQLite (WAL, >= 3.38) via
Microsoft.Data.Sqlite; DbUp migrations; SqlHydra 5.1 codegen (row types only —
no runtime SqlHydra queries); Polly; Microsoft.Extensions.Hosting for DI.
Fault boundary: every store call can fail transiently; the resilience
pipeline is the only retry layer. LISTEN sessions require direct connectivity
around transaction-mode poolers (`PostgresWorkNotifications.fs:11-19`).

## Product context

Headline workflow: durable, idempotent, ordered command processing with
at-least-once action delivery. Most consequential failures change business
outcomes: reordered commands, partial commits, lost actions, stale-worker
overwrites, false history (detailed in the ensemble Product Context report,
preserved in `properties/*.md` evidence). Administrative paths (migration,
cron scheduling, retention tuning, queue administration) matter but are
secondary.

## Unproven assumptions (wildcard pass highlights)

- "Notifications are only hints" is claimed but never tested under total
  notification loss with a live workload.
- Lease expiry vs. in-flight commit: the fence is assumed to hold under every
  interleaving of expiry and finalize; only deterministic single-threaded
  tests exist.
- `instance_state` grows one closed row per commit with no retention (E3,
  documented): unbounded table growth under sustained load is accepted but
  never measured.
- Global database names (channels, advisory-lock key prefixes, cron jobs,
  pgmq queues, `public.schemaversions`) are assumed unique per database
  (E10-E12) — false as soon as a second schema exists (Phase D target).
- SQLite `WriteGate` keying of `:memory:`/`file:`/symlink paths (E22) is
  imperfect; assumed harmless for single-file deployments.
- The recursive-loop leak (E41) is diagnosed from F# task semantics, not yet
  demonstrated by a failing test (B10 will write it first).

## Assumptions

- Antithesis SDK is not yet referenced anywhere; all catalog properties are
  currently unimplemented assertions (see `existing-assertions.md`).
- Deterministic FsCheck/integration laws in this repo complement, not replace,
  the Antithesis catalog; each catalog property names its deterministic
  counterpart where one exists.

## Open Questions

- Which fault classes will be enabled for this tenant (node termination for
  crash-recovery properties is commonly disabled by default)? `(needs human input)`
- Is a two-replica SUT profile acceptable for cross-process ordering
  properties, given every extra container expands the search space? `(partial: one replica already exercises entity-gated concurrency; two are needed for true cross-host claim interleavings)`
