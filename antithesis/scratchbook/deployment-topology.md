---
sut_path: /home/mbenevides/Code/Personal/byzantine-systems/automata
commit: 9bdc555187e735cdf31431de3515832d3d0fc9d3
updated: 2026-11-12
external_references: []
---

# Deployment Topology (minimal)

Three profiles; start with Profile 1 and promote only when
cross-process properties demand it. Every container is justified below —
anything else expands state space without adding coverage.

## Profile 1: PostgreSQL, single worker (baseline)

| Container | Image | Role | Runs | Connections |
|---|---|---|---|---|
| `postgres` | official `postgres` (needs `pgmq`, `btree_gist`; `pg_cron` optional) | dependency | stock server, extensions preloaded via init script | accepts 5432 from `automata`, `client` |
| `automata` | new Dockerfile (build stage: .NET SDK 10; runtime: single worker process) | service (SUT) | one generic host: `Machine` + `CommandProcessor.RunAsync` + `ActionDispatcher.RunAsync` + boot/maintenance registration, Antithesis .NET SDK instrumented | -> `postgres:5432` |
| `client` | new lightweight image with the test template at `/opt/antithesis/test/v1/automata/` | client | emits `setup_complete`, then sleeps; Antithesis runs `parallel_driver_*`, `eventually_*`, `finally_`, `anytime_` commands from the template (helpers under `helper_`) | -> `postgres:5432` (direct submit/read) and/or -> `automata` |

Justification: the SUT is a library, not a server — the smallest honest
deployment is one worker process. The client reaches the database directly
because callers and workers meet in the database by design; a client-facing
API would add a container the product does not require.

## Profile 2: PostgreSQL, two workers (cross-process ordering)

Same as Profile 1 plus a second `automata` replica (`automata-2`). Justified
only for properties requiring two claim loops racing on the same entities
(claim exclusivity across processes, lease takeover). Replica count 2: the
contract is per-entity mutual exclusion, not consensus quorum.

## Profile 3: SQLite (embedded)

| Container | Image | Role | Runs |
|---|---|---|---|
| `automata` | as above, SQLite connection string, notifications `Off` | service | worker + store in one process, WAL file on a volume |
| `client` | as above | client | test template writing/reading through its own SQLite connections |

No `postgres` container. Exercises single-writer `IMMEDIATE` gating,
claim-time lease reclamation, and busy-path behavior under faults.

## SDK selection

.NET SDK (SUT and client are F#/.NET 10). Client workload always carries
assertions; SUT carries SDK instrumentation at the points named in property
evidence files (finalize outcomes, lease-fence branches, poison-quarantine
path, boot defects).

## Fault requirements

- Network faults, congestion, throttling, thread pausing: defaults, fine.
- **Node termination (kill/stop) is required** for crash-recovery properties
  (finalize interrupted, lease takeover, action redelivery after worker death)
  and is commonly disabled by default — must be enabled for the tenant.
- Clock jitter is useful for lease-deadline and `received_at` behavior;
  low priority otherwise.

## Open Questions

- Are node-termination faults enabled for this tenant? `(needs human input)`
- Volume durability semantics for the SQLite profile under container kill
  (restart may drop non-durable filesystem state) — verify WAL + `synchronous`
  settings survive the harness's restart model before relying on Profile 3
  crash properties. `(partial: default SQLite synchronous=NORMAL in WAL survives process crash, container filesystem semantics unverified)`
