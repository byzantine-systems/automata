---
sut_path: /home/mbenevides/Code/Personal/byzantine-systems/automata
commit: 9bdc555187e735cdf31431de3515832d3d0fc9d3
updated: 2026-11-12
external_references: []
---

# Evaluation Lens 1: Antithesis Fit

## Findings

- **Property/Properties:** `poll-loop-bounded-stack-and-memory` — Concern: the
  decisive oracle is the deterministic B10 stress test (100k iterations, stack
  and memory checkpoints); Antithesis adds only fault-stretched iteration
  growth on top. — Scope: property-specific — Evidence: E41 mechanism is
  pure F# task semantics, no interleaving required to violate it. —
  Suggested action: keep, but demote Antithesis priority to low and land the
  deterministic test first (it is the B10 slice's gate anyway).
- **Property/Properties:** `retention-watermark-exact`,
  `finalize-retry-reports-stored-resolution` — Concern: both are already
  pinned by exhaustive deterministic laws (`ReplayRetentionProperties.fs`,
  C6 law); the Antithesis angle is a narrow read/write race. — Scope:
  property-specific — Suggested action: keep at low priority; assert from the
  workload opportunistically rather than dedicating search budget.
- **Property/Properties:** `notification-loss-latency-not-correctness` —
  Concern (inverse): underestimated. A whole-run partition of the listener is
  trivial for Antithesis, impossible in the deterministic suite, and the
  claim has never been fault-tested. — Suggested action: raise priority to
  high.
- **Property/Properties:** `finalize-atomic-single-fact`,
  `finalize-fenced-by-lease-and-epoch`, `committed-action-durable-until-terminal` —
  fit is exactly Antithesis's sweet spot (kill inside statement, throttle
  mid-commit, expiry racing finalize). No change.

## Passes

- The remaining 15 properties each require timing, concurrency or partial
  failure that deterministic tests cannot schedule; assertion types match
  semantics (`Always` for invariants, `Sometimes` for the three liveness
  properties, `AlwaysOrUnreachable` for optional-path invariants).
- No `Sometimes(true)`-shaped conditions found.

## Uncertainties

- Whether pgmq's visibility rules create an observable window during
  partition (noted in `finalize-atomic-single-fact` open questions).
