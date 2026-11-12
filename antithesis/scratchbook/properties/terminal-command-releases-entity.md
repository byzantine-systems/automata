# terminal-command-releases-entity

## Evidence trail

- Identified from: C0 "dead-letter release"; `docs/public-api.md:41-45`
  (dead-lettering is essential or a failing command blocks the entity
  forever); unresolved-claim dead-letter in
  `Runtime/CommandProcessor.fs:384-395` (DeliveryCount > MaxAttempts).
- Code paths: dead-letter/reject statements close the command head;
  maintenance lease reaper reschedules expired leases (`run_maintenance`);
  SQLite reclaims expired leases at claim time.
- Failure scenario: terminal write itself interrupted (worker killed between
  external decision and the dead-letter statement); the reaper must still
  eventually release the entity.
- Key observation: needs node-termination faults for the interrupted-terminal
  window; deterministic counterpart exists in the contract model.

## Instrumentation

- Workload-side: successor claim succeeds within a bound after terminal
  outcome. SUT-side: none; missing.

## Open Questions

- Reaper entity-head interaction: partial — reaper reschedules leases; the
  head-release path is covered by the model but not fault-tested.

### Investigation Log

#### Reaper entity-head interaction

- Examined: `run_maintenance` lease-reaping branch
  (`R__maintenance_routines.sql`), claimability rules in `claim_commands`
  (head-gating by predecessor status), the contract model's terminal-release
  transition, SQLite claim-time reclamation in `SqliteCommandInbox`.
- Found: claimability depends on the predecessor's terminal status, not on
  lease state, so a rescheduled expired lease eventually reaches a terminal
  outcome through normal processing and the head opens; the model asserts
  this end-to-end deterministically.
- Not found: a fault run where the terminal write itself is destroyed by a
  kill and only the reaper path remains.
- Conclusion: `(partial: model covers the path; kill-window untested)`.
