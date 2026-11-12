---
sut_path: /home/mbenevides/Code/Personal/byzantine-systems/automata
commit: 9bdc555187e735cdf31431de3515832d3d0fc9d3
updated: 2026-11-12
external_references: []
---

# Evaluation Lens 2: Coverage Balance

## Findings

- **Gap (deferred with rationale):** SQLite-specific concurrency (WAL busy
  behavior under concurrent writers, `WriteGate` keying E22, claim-time lease
  reclamation) has no dedicated property; the shared contract properties
  (1-12) exercise SQLite only through the common interfaces. — Scope:
  catalog-wide — Suggested action: record as a deliberate scope cut for this
  pass; revisit after the C0 conformance library lands (it will run the whole
  shared suite against SQLite, closing most of the gap).
- **Gap (deferred with rationale):** supervision/resilience control plane
  (`Supervisor` restart strategies, `InMemorySupervisionStore` bound E49,
  breaker behavior) has no properties. — Scope: catalog-wide — Suggested
  action: defer; product-context analysis ranks control-plane failures below
  business-state corruption, and the catalog is already at 21.
- **Type balance:** 16 `Always`, 3 `Sometimes`, 2 `AlwaysOrUnreachable`;
  `Reachable` appears only as suggested guidance markers inside evidence
  files (quarantine branch, BootDefect branches). Acceptable — no standalone
  reachability goal carries product weight on its own.
- **Component distribution:** properties distribute across both providers
  (shared), runtime loops, temporal module and maintenance. No single-service
  concentration. PostgreSQL-only capability (temporal) has proportionate
  coverage.

## Passes

- Every high-risk area named in `sut-analysis.md` (ordering, atomicity,
  fencing, action channel, poison rows, retention floor, boot gate, loop
  growth) maps to at least one property.
- Bug-history hotspots (C1-C7 fix cluster, E41/E44 open defects) all have
  regression-target properties.

## Uncertainties

- None blocking.
