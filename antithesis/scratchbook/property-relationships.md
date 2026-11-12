---
sut_path: /home/mbenevides/Code/Personal/byzantine-systems/automata
commit: 4ddd56f070aeb0a1d03b4c01bf620f4da3fb1933
updated: 2026-11-12
external_references: []
---

# Property Relationships

Clusters, suspected connections and dominance noted during synthesis. Slugs
are canonical IDs from `property-catalog.md`.

## Cluster: entity serialization

`entity-head-single-claimable`, `claim-exclusive-no-duplicate`,
`arrival-order-monotonic`, `terminal-command-releases-entity`,
`poison-row-quarantined-batch-survives`.

The head invariant dominates: if the head is single and correctly ordered,
claim exclusivity is its cross-process face and terminal release is its
liveness complement. `arrival-order-monotonic` is independent (timestamp
plumbing, not mutual exclusion) but shares the submit advisory-lock window and
fault exposure. `poison-row-quarantined-batch-survives` attaches here because
the claim path is where quarantine runs — it protects head processing from
undecodable rows rather than adding a new invariant.

## Cluster: finalize integrity

`finalize-atomic-single-fact`, `finalize-fenced-by-lease-and-epoch`,
`finalize-retry-reports-stored-resolution`, `submit-idempotent-by-request-identity`.

Atomicity is the core; fencing guards it against stale writers; retry-honesty
and submit idempotency are its client-visible recovery faces. A violation of
fencing usually surfaces *through* atomicity observations (partial writes), so
falsifications here often credit both — keep assertions distinct anyway.

## Cluster: action channel

`committed-action-durable-until-terminal`, `action-ack-lease-fenced`,
`action-routed-by-boot-registration`.

Durability is the channel's safety core; ack-fencing is its stale-worker face
(dominates: a fenced-ack violation can *cause* apparent loss);
boot-registration routing is a configuration precondition — violating it makes
durability observations vacuous (actions "delivered" to the wrong place).

## Cluster: runtime loops

`poll-loop-bounded-stack-and-memory`, `cancellation-stops-promptly`,
`notification-loss-latency-not-correctness`, `send-settles-when-outcome-known`,
`pre-canceled-call-does-no-work`, `task-completion-context-independent`.

All six live in the same runtime boundary (processor, dispatcher, observer drain,
`Recurring.repeat`/`awaitSettled`). The B10 rewrite (E41) is the shared
mechanism for the first two; the E44 fix is the mechanism for the fourth.
Notification-loss is only coupled by sharing the wake-up machinery.
`pre-canceled-call-does-no-work` is cancellation's API-entry complement to
loop shutdown, while `task-completion-context-independent` protects every
continuation in the same call paths from caller-context deadlock.

## Cluster: history integrity (PostgreSQL)

`correction-atomic-belief-rewrite`, `correction-fenced-replay-bounded`,
`retention-watermark-exact`, `purge-removes-all-command-dependents`.

Purge/watermark are the retention floor the correction pair stands on: a
watermark lie makes replay-boundedness unfalsifiable in the affected window
(dominance: `retention-watermark-exact` dominates
`correction-fenced-replay-bounded` for purged-window cases). E1's purge wedge
is the failure that makes every other maintenance observation moot.

## Cluster: control plane

`restart-intensity-escalates`, `escalated-worker-restarts-and-resumes`,
`supervision-audit-bounded`, `breaker-fails-fast-then-recovers`.

The supervisor's intensity window dominates the cluster: escalation
(`restart-intensity-escalates`) is the terminal event whose recovery is
`escalated-worker-restarts-and-resumes`'s subject. The audit bound is an
observability side-condition on the same events (a leak in the record of the
incident, not in the incident handling). The breaker belongs here as the
per-call fast-fail tier beneath the supervisor's per-child tier — the two
were explicitly separated in `TransientPolicy.fs:67-81` (durable retry is
neither) and should not dominate each other.

## Standalone

`boot-refusal-leaves-store-unchanged` — a lifecycle gate rather than a steady-
state invariant; Phase D's queue-ownership and schema checks will route their
refusals through it.

## Cross-cluster suspicion

`finalize-atomic-single-fact` x `committed-action-durable-until-terminal`:
the enqueue is *inside* the finalize transaction, so any atomicity violation
with actions present should trip both properties; distinct failure modes
(post-ack loss) belong to the action side alone.
