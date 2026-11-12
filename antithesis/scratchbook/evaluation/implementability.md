---
sut_path: /home/mbenevides/Code/Personal/byzantine-systems/automata
commit: 9bdc555187e735cdf31431de3515832d3d0fc9d3
updated: 2026-11-12
external_references: []
---

# Evaluation Lens 3: Implementability

## Findings

- **Property/Properties:** `claim-exclusive-no-duplicate`,
  `entity-head-single-claimable` (cross-process face) — Concern: require
  Profile 2 (two SUT replicas), which is the catalog's only topology-cost
  multiplier. — Scope: property-specific — Suggested action: confirm the
  two-replica profile when the harness is built; single-replica runs still
  cover the entity-gated window.
- **Property/Properties:** `terminal-command-releases-entity`,
  `committed-action-durable-until-terminal`,
  `finalize-atomic-single-fact` (kill windows) — Concern: depend on node-
  termination faults, which are commonly disabled by default. — Scope:
  property-specific — Suggested action: confirm tenant fault availability
  before workload planning; if unavailable, approximate with workload-driven
  connection aborts (see wildcard lens) and mark the kill-window properties
  outstanding.
- **Property/Properties:** `send-settles-when-outcome-known` — Concern: the
  E44 defect is open, so the property would legitimately fail today. —
  Scope: property-specific — Suggested action: order the property after the
  B-phase E44 fix lands; record the ordering constraint in `synthesis.md`.
- **Property/Properties:** `poll-loop-bounded-stack-and-memory` — Concern: N
  and memory tolerance are unresolved inside a 15-minute timeline. — Scope:
  property-specific — Suggested action: derive both from the deterministic
  B10 stress test's measurements when it lands.

## Passes

- All workload-side observations use public interfaces or direct database
  reads the client container already has.
- Every deterministic counterpart named in the catalog exists or is already
  mandated by a planned slice (C0 suite, B10 stress, C1 guard).

## Uncertainties

- Tenant fault configuration (also recorded in `deployment-topology.md`).
