# arrival-order-monotonic

## Evidence trail

- Identified from: E2 register entry and C2 fix (commit 76d6e8f):
  `received_at` stamped after the entity lock and clamped with `GREATEST` over
  the predecessor's `received_at`; explicit earlier arrivals refused
  (`SubmissionOutcome.Refused(ArrivedBeforePredecessor)`).
- Code paths: `R__command_routines.sql` submit path;
  `Runtime/CommandProcessor.fs:370-375` (ReceivedAt becomes EffectiveAt);
  SQLite write-session `Now` clamp; shared law in
  `tests/StorageContractProperties.fs`.
- Failure scenario: clock jitter steps the database clock backwards between
  two submissions of one entity; or a caller-supplied earlier arrival slips
  past refusal under a race with the predecessor insert.
- Key observation: regression tests pin the single-threaded behavior
  including the deterministic blocked-lock race test.

## Instrumentation

- Workload-side: read-back ordering assertion after each submit. SUT-side:
  none; missing.

## Open Questions

- Multi-step backwards clock regression (larger than one interval): partial —
  clamp is per-predecessor; chained regression untested.

### Investigation Log

#### Multi-step backwards clock regression (larger than one interval)

- Examined: the `GREATEST(predecessor.received_at, ...)` clamp in
  `R__command_routines.sql`, the SQLite write-session `Now` clamp, and the C2
  shared law plus blocked-lock regression (commit 76d6e8f).
- Found: each submission clamps only against its immediate predecessor; a
  single backwards step cannot reorder an entity, and monotonicity is
  inductive over the chain as long as every insert passes through the clamp.
- Not found: a test or proof covering repeated multi-interval backwards steps
  interleaved with submissions; clock-jitter fault runs do not exist yet.
- Conclusion: `(partial: single-step clamp verified; chained regression untested)`.
