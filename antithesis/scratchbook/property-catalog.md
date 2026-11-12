---
sut_path: /home/mbenevides/Code/Personal/byzantine-systems/automata
commit: 9bdc555187e735cdf31431de3515832d3d0fc9d3
updated: 2026-11-12
external_references: []
---

# Property Catalog

21 properties. Categories follow the system's architecture. Every property
names a deterministic counterpart where one already exists (FsCheck law or
integration regression) — Antithesis adds the timing, concurrency and partial-
failure dimension those cannot reach. No property below is instrumented yet
(see `existing-assertions.md`).

## Category: Command lifecycle and ordering (both providers)

Covers submit, claim, head-of-line ordering and terminal release — the
contract every provider must satisfy identically (C0 suite territory).

### entity-head-single-claimable — One claimable head per entity

| | |
|---|---|
| **Type** | Safety |
| **Property** | At any instant, at most one non-terminal command per entity is claimable, and it is the entity's lowest unresolved `seq`. |
| **Invariant** | `Always`: after any workload step, a claim over all workers returns no two commands for one entity and never a successor while its predecessor is non-terminal. Matches the contract model's `HeadClaimable/Held/Hidden` classification (`tests/StorageContractProperties.fs`). |
| **Antithesis Angle** | Race submits, claims and finalizes across workers with thread pausing and connection faults inside the claim/submit window; explores interleavings deterministic tests cannot schedule. |
| **Why It Matters** | Reordered commands mean payment confirmation can overtake initiation; two hosts can commit contradictory states. |

**Open Questions:**

- Does the advisory-lock window inside `submit_command` hold under asymmetric network faults between the two lock participants? `(partial: lock is a single-session statement; asymmetry should reduce to latency, not yet fault-tested)`

### claim-exclusive-no-duplicate — Claims never overlap

| | |
|---|---|
| **Type** | Safety |
| **Property** | Two concurrent claims never return the same command, for any batch sizes, any number of worker processes. |
| **Invariant** | `Always`: union of claimed `(commandId, token)` across all in-flight claims contains no duplicate command id. Deterministic counterpart: contract property "claim exclusivity" (C0). |
| **Antithesis Angle** | Two-replica profile (Profile 2) with node throttling staggering the claim statement; the UPDATE...RETURNING lease is the mechanism under test. |
| **Why It Matters** | Double processing is the classic at-least-once hazard for commands, which the contract promises to prevent at this layer. |

**Open Questions:**

- None specific; inherits the cross-process verification question from `entity-head-single-claimable`.

### arrival-order-monotonic — Arrival stamps follow sequence order

| | |
|---|---|
| **Type** | Safety |
| **Property** | Per entity, `received_at` is nondecreasing in `seq`; a caller-supplied earlier arrival is refused (`ArrivedBeforePredecessor`) and stored nowhere. |
| **Invariant** | `Always`: for every entity, reading commands by `seq` yields nondecreasing `received_at`. Deterministic counterpart: C2 regression and shared law (commit 76d6e8f). |
| **Antithesis Angle** | Clock jitter plus racing submissions through the advisory-lock window; the clamp (`GREATEST` over predecessor) must hold under backward wall-clock steps. |
| **Why It Matters** | Arrival is business time (`EffectiveAt`); a regression desynchronizes replay from the original decisions (E2's dead-letter loop). |

**Open Questions:**

- Does the clamp hold under database clock steps larger than one submission interval? `(partial: GREATEST clamps to predecessor; multi-step regression untested)`

### submit-idempotent-by-request-identity — Key honesty

| | |
|---|---|
| **Type** | Safety |
| **Property** | Same key + same request digest returns the original command id (`AlreadySubmitted`), never a second row; same key + different digest is `Refused(KeyReused)` with no write. |
| **Invariant** | `Always`: after any sequence of submits under one key, exactly one command row exists for that key and the returned id is stable. Deterministic counterpart: C5 shared law (commit b2dee03). |
| **Antithesis Angle** | Retries racing the original submission under connection loss (client never saw the first answer); digest computed in-database normalizes jsonb reordering. |
| **Why It Matters** | Duplicate charges/approvals on retry; silent acceptance of a different request under a reused key. |

**Open Questions:**

- None open; the digest mechanism was mutation-validated locally (C5).

### finalize-atomic-single-fact — Commit is all-or-nothing

| | |
|---|---|
| **Type** | Safety |
| **Property** | A successful commit observably appends exactly one transition, advances the snapshot exactly one epoch, enqueues every recorded action on the registered queue, and closes the command — or none of these. |
| **Invariant** | `Always`: no interleaving of checks observes a commit partially applied, including from a second connection reading mid-finalize. Deterministic counterpart: C0 equations (history visibility, action enqueue) and finalize laws. |
| **Antithesis Angle** | The sweet spot: kill the worker inside the finalize transaction, throttle the connection mid-commit, partition after send-before-ack. Transaction atomicity must absorb every timing. |
| **Why It Matters** | A state change without its queued action (or vice versa) breaks every downstream business expectation. |

**Open Questions:**

- Is there any client-observable read path that bypasses the transaction (e.g. pgmq visibility before commit)? `(partial: enqueue happens inside the same transaction per finalize.sql; pgmq's own read_committed visibility assumed aligned)`

### finalize-fenced-by-lease-and-epoch — Stale writers write nothing

| | |
|---|---|
| **Type** | Safety |
| **Property** | Commit with an expired/superseded lease token or a wrong expected epoch answers fenced/refused and leaves log, snapshot, actions and command status unchanged. |
| **Invariant** | `Always`: after any fenced finalize attempt, durable state equals the state before it. Deterministic counterpart: contract "fencing conservation" and "epoch atomicity". |
| **Antithesis Angle** | Lease expiry racing the finalize statement (clock jitter while a throttled worker holds the fence); takeover worker claiming while the old one commits. |
| **Why It Matters** | A stalled worker overwriting a newer result silently corrupts business state. |

**Open Questions:**

- None; mechanism (`LeaseToken` + epoch check in `finalize_command`) is pinned by law.

### finalize-retry-reports-stored-resolution — Finalize retries are honest

| | |
|---|---|
| **Type** | Safety |
| **Property** | A finalize retry whose first attempt committed but whose answer was lost returns `AlreadyFinalized` carrying the stored epoch and status. |
| **Invariant** | `Always`: reported resolution equals the durable record. Deterministic counterpart: C6 law (commit d4b0a0d). |
| **Antithesis Angle** | Connection dropped after commit before acknowledgement; retry arrives with a different resolution in hand. |
| **Why It Matters** | Without it a caller cannot distinguish its outcome from the stored one and may compensate wrongly. |

**Open Questions:**

- None.

### terminal-command-releases-entity — Dead letters unblock the entity

| | |
|---|---|
| **Type** | Liveness |
| **Property** | When a command reaches any terminal outcome (committed, rejected, dead-lettered), its entity's successor becomes claimable. |
| **Invariant** | `Sometimes(cond)`: after a terminal outcome and a subsequent claim, the successor is returned — must become true in every timeline that keeps processing. Deterministic counterpart: contract "dead-letter release". |
| **Antithesis Angle** | Terminal write interrupted by worker kill; maintenance reaper involved for unreleased leases; needs node termination faults. |
| **Why It Matters** | An unreleased failing command blocks its entity forever — the docs call dead-lettering essential for exactly this. |

**Open Questions:**

- Does the maintenance reaper's lease-reclaim path also release the entity head, or only the lease row? `(partial: reaper reschedules expired leases; entity-head interaction covered by model, not fault-tested)`

### poison-row-quarantined-batch-survives — One bad row never wedges a batch

| | |
|---|---|
| **Type** | Safety |
| **Property** | A claim batch containing an undecodable row returns every decodable row, and the poison row is durably quarantined (dead letter / durable record) fenced by the token it received. |
| **Invariant** | `Always`: no interleaving leaves healthy rows leased-but-unreturned, and the poison row never returns to a claim. Deterministic counterpart: C3 poison-position laws (commit 46ae6ed). |
| **Antithesis Angle** | Quarantine follow-up write failing while the batch is processed; reclaimed poison row retried after lease expiry. |
| **Why It Matters** | E6's original failure mode: one schema-drifted payload stalling an entire queue forever. |

**Open Questions:**

- None.

## Category: Action delivery (both providers)

Covers the at-least-once side-effect channel and its fencing.

### committed-action-durable-until-terminal — Actions are never lost

| | |
|---|---|
| **Type** | Safety |
| **Property** | From the moment a commit records actions until they are completed or durably abandoned, at least one claimable copy exists in the queue. |
| **Invariant** | `Always`: after any crash/kill/retry interleaving, a committed action is either delivered or recorded as abandoned — never silently gone. |
| **Antithesis Angle** | Worker killed between external effect and acknowledgement (redelivery expected, loss forbidden); queue maintenance racing retries. |
| **Why It Matters** | Lost payment reservations and lost customer notices are the highest-impact business failures. |

**Open Questions:**

- Does the pgmq archive path keep abandoned actions queryable for reconciliation? `(partial: action_dead_letter table exists per C3; archive interaction untested)`

### action-ack-lease-fenced — Acknowledgements respect the fence

| | |
|---|---|
| **Type** | Safety |
| **Property** | `Complete`/`Reschedule`/`Abandon` with an expired or superseded lease answers `LeaseLost` and changes nothing about the row another worker now holds. |
| **Invariant** | `Always`: a fenced acknowledgement leaves the action's ownership and status exactly as the current holder left them. |
| **Antithesis Angle** | Slow handler (throttled container) whose lease expires mid-effect; second worker claims; first worker's ack arrives last. |
| **Why It Matters** | A stale ack can delete or requeue a delivery another worker owns — duplicated or lost effects. |

**Open Questions:**

- None.

### action-routed-by-boot-registration — Routing belongs to the database

| | |
|---|---|
| **Type** | Safety |
| **Property** | Actions are enqueued on the queue registered at boot; a machine with actions but no registered queue refuses the whole commit (no partial write); `Discard` records transition actions without delivery. |
| **Invariant** | `Always`: no commit interleaving produces actions on an unregistered queue or a committed transition with missing/extra delivery. Deterministic counterpart: C4 regressions (commit fbf059f). |
| **Antithesis Angle** | Two boots racing registration; commit racing a re-boot that changes registration. |
| **Why It Matters** | E7's silent-drop and wrong-queue failures misroute business effects. |

**Open Questions:**

- None.

## Category: Runtime and concurrency (process-level)

Covers the loops, cancellation and notification paths shared by both providers.

### poll-loop-bounded-stack-and-memory — Loops do not grow

| | |
|---|---|
| **Type** | Safety |
| **Property** | Sustained polling with a synchronously-completing store neither overflows the stack nor retains per-iteration state indefinitely. |
| **Invariant** | `AlwaysOrUnreachable`: whenever a loop drains >= N iterations, stack depth and retained memory stay bounded (tolerance-based check). Known **defect today** (E41 recursive `task` loops); property validates the B10 rewrite. |
| **Antithesis Angle** | CPU modulation and throttling stretch iterations so growth accumulates across fault windows; a pure deterministic test would need the rewritten loop anyway. |
| **Why It Matters** | Every long-running host leaks until it dies; stack overflow cannot be caught. |

**Open Questions:**

- What N and memory tolerance make the check robust inside a 15-minute timeline? `(partial: plan targets 100k iterations vs baseline; tolerance TBD when instrumented)`

### cancellation-stops-promptly — Stop is observed

| | |
|---|---|
| **Type** | Liveness |
| **Property** | After cancellation is requested, no new poll/delivery/wait cycle begins and `RunAsync` completes promptly with its totals. |
| **Invariant** | `Sometimes(cond)`: in every timeline where cancellation is requested, the loop's completion marker is reached without starting another claim. |
| **Antithesis Angle** | Cancellation racing a full-batch drain (the loop's fast path skips the wait step); graceful shutdown under throttling. |
| **Why It Matters** | Hosts that ignore stop drag k8s/systemd into kill territory, converting clean shutdowns into crashes. |

**Open Questions:**

- None.

### notification-loss-latency-not-correctness — Hints are only hints

| | |
|---|---|
| **Type** | Safety |
| **Property** | With LISTEN/NOTIFY entirely absent (listener `Off`, connection partitioned), every submitted command still reaches a terminal outcome via polling. |
| **Invariant** | `Always`: outcomes converge; only latency differs. The claim is stated at `Storage/Maintenance.fs:58-70` and never fault-tested. |
| **Antithesis Angle** | Partition the listener connection for the whole run; polling interval becomes the only wake-up. |
| **Why It Matters** | If notifications were load-bearing anywhere, polling deployments would silently lose work. |

**Open Questions:**

- None.

### send-settles-when-outcome-known — send terminates

| | |
|---|---|
| **Type** | Liveness |
| **Property** | `Machine.send` returns once the durable outcome is known (or errors); it never polls forever. Known **defect today** for purged commands (E44: `Ok None` loops). |
| **Invariant** | `Sometimes(cond)`: every `send` started in a timeline where processing continues reaches a return. Property lands with the E44 fix. |
| **Antithesis Angle** | Retention purge racing an in-flight `send` (the exact E44 window); clock jitter stretching `ResultPollInterval`. |
| **Why It Matters** | A caller blocked forever on `send` is an availability hole the caller cannot route around. |

**Open Questions:**

- Should a purged-while-awaited command answer with a typed "purged" outcome rather than erroring through the poll loop? `(needs human input)`

## Category: Temporal and corrections (PostgreSQL)

Covers bitemporal belief rewriting and replay — PostgreSQL-only capability.

### correction-atomic-belief-rewrite — Corrections are transactional rewrites

| | |
|---|---|
| **Type** | Safety |
| **Property** | A correction archives superseded beliefs and writes the corrected timeline in one transaction; a crashed correction leaves either the old or the new belief set, never a partial rewrite and never "no live belief". |
| **Invariant** | `Always`: after any interruption, exactly one live belief per corrected entity exists and archived rows are append-only. |
| **Antithesis Angle** | Kill inside `correct_beliefs`; partition between archive and rewrite statements; empty-belief-list edge (E16, fixed by type). |
| **Why It Matters** | False audit history; epoch regression breaking the next commit (E16's PK collision). |

**Open Questions:**

- None.

### correction-fenced-replay-bounded — Replay is fenced, bounded and honest

| | |
|---|---|
| **Type** | Safety |
| **Property** | Correction finalization is lease-fenced; replay respects its budget and uses original chart versions; replay over purged history is refused with a typed error, never silently wrong. |
| **Invariant** | `Always`: no replay interleaving produces a committed belief derived from incomplete history. Deterministic counterparts: correction-finalization laws (commits dc6f462, 8adbda0, 90e5ed3, ede7999). |
| **Antithesis Angle** | Purge racing the replay plan (watermark moves under the reader); lease expiry mid-replay-commit. |
| **Why It Matters** | Corrections exist because history was wrong; a correction that silently tolerates holes recreates the problem it fixes. |

**Open Questions:**

- None.

## Category: Schema and maintenance (PostgreSQL)

Covers retention, purge and boot — the operator-facing safety net.

### purge-removes-all-command-dependents — Retention never wedges

| | |
|---|---|
| **Type** | Safety |
| **Property** | Purging commands past the retention horizon deletes every FK-dependent row of each purged command, and maintenance keeps succeeding no matter how old the data. |
| **Invariant** | `Always`: after any purge run with any data age distribution, a subsequent `run_maintenance` succeeds. Deterministic counterpart: C1 regression (90e5ed3); the planned generic catalog guard (every FK into `command` has a purge target) is still missing and is part of C1's finish. |
| **Antithesis Angle** | Corrections aged past the horizon (E1's exact trigger); purge racing new corrections. |
| **Why It Matters** | E1 wedged every machine's maintenance forever once any correction aged out. |

**Open Questions:**

- None (guard design already planned in C1 finish).

### retention-watermark-exact — Suffix honesty

| | |
|---|---|
| **Type** | Safety |
| **Property** | `Suffix` reports `Incomplete` exactly when history at or below the requested `from` instant has been purged (via the `entity_purge` watermark), never by heuristic. |
| **Invariant** | `Always`: `Incomplete` iff `max_purged_effective_at >= from` for that entity. Deterministic counterpart: `ReplayRetentionProperties.fs` (C7, 90e5ed3). |
| **Antithesis Angle** | Purge and suffix query interleaving across connections (watermark write vs read). |
| **Why It Matters** | Replay that trusts a heuristic over holes silently reconstructs wrong history. |

**Open Questions:**

- None.

### boot-refusal-leaves-store-unchanged — Boot fails loudly and safely

| | |
|---|---|
| **Type** | Safety |
| **Property** | Whenever boot reports defects (schema missing/behind/ahead, prerequisites missing, misconfigured queue ownership), it changes no durable state and no worker starts. |
| **Invariant** | `AlwaysOrUnreachable`: if any `BootDefect` is observed, the store's pre-boot state is intact and no processing marker is reached. Reachable defect outcomes are themselves worth `Reachable` guidance. |
| **Antithesis Angle** | Boot racing an operator's parallel migration (journal read mid-upgrade); Phase D's queue-ownership refusal (second schema claiming a queue) lands here too. |
| **Why It Matters** | A boot that half-configures (queue created, registration skipped) leaves a store that silently misroutes. |

**Open Questions:**

- Does boot's idempotent configuration (queue creation, maintenance registration) leave partial effects when the boot check itself fails midway? `(partial: boot check precedes configuration per docs; mid-configuration fault path untested)`

## Category: Supervision and resilience control plane

Added by targeted gap-fill discovery (user decision, 2026-11-12): the
Erlang-style supervisor, escalation handling, and the resilience pipeline.

### restart-intensity-escalates — Crash loops terminate in escalation

| | |
|---|---|
| **Type** | Safety |
| **Property** | When child restarts exceed the configured intensity within the sliding period window, the supervisor escalates: every child is stopped and the supervisor faults `SupervisorEscalated` — it never restarts forever. |
| **Invariant** | `Always`: after any sequence of child exits, the number of restarts within any `Period` window never exceeds `Intensity` without an escalation. Rationale: an exact invariant on every state transition; `Always` matches, and the sliding-window boundary is precisely the timing-sensitive surface. |
| **Antithesis Angle** | A crash-looping child under CPU throttling and restart-delay jitter straddles the window boundary: restarts clustering inside the window must escalate; the same count spread outside it must not. A deterministic test fixes the clock; Antithesis explores the boundary. |
| **Why It Matters** | An unbounded restart loop burns a core forever and hides the defect that caused it; the intensity window is the system's only circuit breaker for rarely-restarting children (a real breaker cannot express it, `Supervisor.fs:73-84`). |

**Open Questions:**

- Does the sliding window count correctly under `OneForAll` cascades (many children exiting near-simultaneously)? `(partial: decisions are serialized through one agent mailbox, so the count is atomic; cascade clustering untested)`

### escalated-worker-restarts-and-resumes — Escalation recovers, others progress

| | |
|---|---|
| **Type** | Liveness |
| **Property** | After a processor escalation crashes its supervised child, the child restarts (default `Permanent`/`OneForOne`), the escalated command is eventually dead-lettered unresolved rather than reprocessed forever, and commands for other entities keep completing. |
| **Invariant** | `Sometimes(cond)`: in every timeline where the database stays reachable, post-escalation processing reaches new completions for other entities. |
| **Antithesis Angle** | Escalation racing the escalated command's lease expiry; restart delay versus lease deadline decides whether the restarted worker re-claims the escalated command first. |
| **Why It Matters** | One poisoned command must cost one restart plus one dead letter, not the machine's throughput (`HostedServices.fs:198-200` raises; `CommandProcessor.fs:384-395` dead-letters unresolved over-claimed commands). |

**Open Questions:**

- None.

### supervision-audit-bounded — The in-memory audit store has a bound

| | |
|---|---|
| **Type** | Safety |
| **Property** | The in-memory supervision audit store retains at most its configured bound of events, evicting oldest-first. Known **defect today** (E49: unbounded); the property lands with the DI-package fix. |
| **Invariant** | `AlwaysOrUnreachable`: whenever the in-memory store is active, its retained event count never exceeds the bound. |
| **Antithesis Angle** | Sustained crash-looping (restart events per crash) under a long timeline grows the store; only a fault-stretched run observes the leak meaningfully. |
| **Why It Matters** | Every supervision event retained forever is a slow leak in exactly the hosts that are already unhealthy. |

**Open Questions:**

- None.

### breaker-fails-fast-then-recovers — The circuit opens and half-opens honestly

| | |
|---|---|
| **Type** | Liveness |
| **Property** | With a breaker configured, sustained store failures open the circuit so subsequent calls fail fast as `CircuitOpen` (not as attempt timeouts), and after the break duration with the dependency recovered, a call succeeds again. |
| **Invariant** | `Sometimes(cond)`: within a fault run, both states are observed — a fast-fail `CircuitOpen` outcome during the break, and a success after the break elapses and the database returns. |
| **Antithesis Angle** | Partition the database just long enough to trip the breaker, restore during the break duration: the half-open probe's fate is a pure timing race between recovery and the next call. |
| **Why It Matters** | A breaker that never opens turns every call into a timeout cascade; one that never closes keeps a healthy system down after recovery. `TransientPolicy.Breaker` is `None` by default — the workload must configure it. |

**Open Questions:**

- None.

## Priorities

Assigned by evaluation synthesis; rationale in `evaluation/synthesis.md` and
the fit lens. High = dedicated search budget; medium = asserted from the
workload on runs that exercise them; low = deterministic counterpart is the
primary oracle, asserted opportunistically.

| Slug | Priority |
|---|---|
| entity-head-single-claimable | high |
| claim-exclusive-no-duplicate | high |
| arrival-order-monotonic | medium |
| submit-idempotent-by-request-identity | medium |
| finalize-atomic-single-fact | high |
| finalize-fenced-by-lease-and-epoch | high |
| finalize-retry-reports-stored-resolution | low |
| terminal-command-releases-entity | medium |
| poison-row-quarantined-batch-survives | high |
| committed-action-durable-until-terminal | high |
| action-ack-lease-fenced | high |
| action-routed-by-boot-registration | high |
| poll-loop-bounded-stack-and-memory | low |
| cancellation-stops-promptly | medium |
| notification-loss-latency-not-correctness | high |
| send-settles-when-outcome-known | medium |
| correction-atomic-belief-rewrite | medium |
| correction-fenced-replay-bounded | medium |
| purge-removes-all-command-dependents | medium |
| retention-watermark-exact | low |
| boot-refusal-leaves-store-unchanged | medium |
| restart-intensity-escalates | high |
| escalated-worker-restarts-and-resumes | medium |
| supervision-audit-bounded | low |
| breaker-fails-fast-then-recovers | medium |

## Catalog-wide open questions

- What outcome shape should a purged-while-awaited command give `Machine.send`
  (typed "purged/unknown" outcome vs error)? `(needs human input)` — blocks
  only the fix design for `send-settles-when-outcome-known`, not the rest of
  the catalog.
