# action-ack-lease-fenced

## Evidence trail

- Identified from: `Storage/Action.fs` `IActionQueue` lease contract
  (`Complete`/`Reschedule`/`Abandon` answer `LeaseUpdateOutcome`); the
  dispatcher's `fenced` mapping in `Runtime/ActionDispatcher.fs:150-164`.
- Code paths: `action/complete.sql`, `action/abandon.sql`,
  `action/reschedule.sql` (token-gated updates); SQLite equivalents.
- Failure scenario: throttled handler's lease expires mid-effect; second
  worker claims and delivers; the first worker's `Complete` arrives and must
  answer `LeaseLost` without disturbing the second worker's ownership.
- Key observation: mirrors command-side fencing but on the effect channel;
  no deterministic cross-worker test exists today.

## Instrumentation

- Workload-side: ownership-unchanged assertion after stale ack. SUT-side:
  none; missing.

## Open Questions

- None.
