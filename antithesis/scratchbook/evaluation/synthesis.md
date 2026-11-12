---
sut_path: /home/mbenevides/Code/Personal/byzantine-systems/automata
commit: 9bdc555187e735cdf31431de3515832d3d0fc9d3
updated: 2026-11-12
external_references: []
---

# Evaluation Synthesis

Findings from the four lenses, categorized. Refinements applied directly;
gaps recorded; biases escalated below.

## Refinements (applied)

1. `poll-loop-bounded-stack-and-memory`: Antithesis priority demoted to low;
   the deterministic B10 stress test is the primary oracle and lands first
   (already reflected in the evidence file and the B10 slice plan).
2. `notification-loss-latency-not-correctness`: priority raised to high.
3. `retention-watermark-exact`, `finalize-retry-reports-stored-resolution`:
   priority low; assert opportunistically from the workload.
4. `finalize-atomic-single-fact`: evidence file gains the wildcard lens's
   additional workload observation — after `send` returns, a
   `Machine.state` readback must reflect the committed state (dominance: a
   consequence of the same transaction, not a separate property).

## Gaps

- ~~Supervision/resilience control plane: deferred per product-context
  ranking.~~ **Filled 2026-11-12** by targeted discovery after the user
  explicitly chose "Add control-plane now" over accepting the deferral:
  four new properties (`restart-intensity-escalates`,
  `escalated-worker-restarts-and-resumes`, `supervision-audit-bounded`,
  `breaker-fails-fast-then-recovers`) in a new catalog category with a
  relationships cluster.
- SQLite-specific concurrency surface (WAL busy, WriteGate keying): still
  deferred; revisit after the C0 conformance library runs the shared suite
  against SQLite.
- Cross-machine interference (payload routing, lock namespaces): still
  deferred to the Phase D research pass, where schema isolation retests the
  area.

## Biases (escalated to the human)

1. **Storage-correctness orientation.** The catalog concentrates on durable
   business outcomes (ordering, atomicity, fencing, delivery) and covers the
   control plane (supervision, breaker behavior) not at all. Evidence:
   coverage-balance lens; product-context analysis ranks business-state
   corruption as the principal risk. Judgment needed: accept this
   orientation, or redirect some budget to the control plane now?
2. **Fault-enablement dependency.** Several high-value properties assume
   node-termination faults are enabled for the tenant. If they are not, the
   kill-window set becomes outstanding until enabled (fallback: partial
   coverage via workload-driven connection aborts, per the wildcard lens).
   Confirmation needed from the operator.

## Ordering constraints (from implementability)

- `send-settles-when-outcome-known` asserts only after the E44 fix lands
  (B-phase slice).
- `poll-loop-bounded-stack-and-memory` tolerances derive from the B10 stress
  measurements.

## Catalog state after evaluation

25 properties after the control-plane gap fill: 17 `Always` /
5 `Sometimes` / 3 `AlwaysOrUnreachable`. Priorities are recorded per property
in the catalog's Priorities table: high = entity-head-single-claimable,
claim-exclusive-no-duplicate, finalize-atomic-single-fact,
finalize-fenced-by-lease-and-epoch, poison-row-quarantined-batch-survives,
committed-action-durable-until-terminal, action-ack-lease-fenced,
action-routed-by-boot-registration, notification-loss-latency-not-correctness,
restart-intensity-escalates; low = finalize-retry-reports-stored-resolution,
retention-watermark-exact, poll-loop-bounded-stack-and-memory,
supervision-audit-bounded; medium = the remaining eleven. No properties
invalidated.

## Re-evaluation: control-plane gap fill (2026-11-12)

The gap fill added a new category, which per the evaluation method warrants
an integration check across the lenses:

- **Fit:** `restart-intensity-escalates` is a strong fit (sliding-window
  boundary under restart-delay jitter is exactly the timing surface
  deterministic tests cannot explore). `breaker-fails-fast-then-recovers`
  fits (half-open probe races recovery). `escalated-worker-restarts-and-resumes`
  fits (restart delay versus lease deadline). `supervision-audit-bounded`
  is deterministic-primary (like `poll-loop-bounded-stack-and-memory`) and is
  priced accordingly at low.
- **Coverage balance:** the control plane now has proportionate
  representation; type balance moved to 17/5/3, still invariant-heavy, which
  matches a system whose product risk is business-state corruption. No
  over-investment signal.
- **Implementability:** no new topology requirements (the escalating
  scenario is inducible from the workload via a poison/failing command);
  `breaker-fails-fast-then-recovers` requires the workload to configure a
  breaker (default is `None`) with timeline-scaled durations.

Conclusion: the new category integrates; no further evaluation pass needed.
