---
sut_path: /home/mbenevides/Code/Personal/byzantine-systems/automata
commit: 9bdc555187e735cdf31431de3515832d3d0fc9d3
updated: 2026-11-12
external_references: []
---

# Evaluation Lens 4: Wildcard

Other lenses: Fit (Antithesis sweet spot vs deterministic territory),
Coverage (portfolio vs SUT risk map), Implementability (observability,
topology, fault feasibility). This pass looks where they do not.

## Findings

- **Cross-lens (Fit x Implementability):** the kill-window properties need
  node termination, which may be disabled. A workload-level approximation
  exists and is cheap: the client holds its own database connections, so it
  can abort a connection mid-finalize and emulate the lost-ack half of the
  window (not process death, but the ambiguous-completion interleaving). —
  Suggested action: record as fallback in the workload plan.
- **Question the frame:** the catalog implicitly treats "send returns" and
  "state is visible" as one observation. They are distinguishable:
  `Machine.state` reads the snapshot advanced by the same finalize
  transaction, so after `send` returns, a state readback must reflect the
  committed state. That is a cheap, client-observable consequence of
  `finalize-atomic-single-fact` — record it as an *additional workload
  observation* on that property rather than a 22nd property (dominance).
- **Missing angle (reported, not added):** cross-machine interference (two
  machines sharing one database: notification payload routing by machine id,
  per-machine advisory-lock namespaces). Today's payload-matched routing
  (`Listener.route`) looks sound, and Phase D's schema isolation will retest
  the whole area. Defer to the Phase D research pass where it belongs.
- **Odd report:** `instance_state` keeps one closed valid-time row per commit
  with no retention (E3, documented) — unbounded growth is *accepted* by the
  plan. Not a property; a capacity observation worth a note in workload
  design (long timelines will grow the table; do not mistake the growth for
  a violation).

## Passes

- No property requires observing internal process state that the SUT-side
  instrumentation points do not already name.

## Uncertainties

- Whether connection-abort emulation covers enough of the kill window to be
  worth asserting before node-termination availability is confirmed.
