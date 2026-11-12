# entity-head-single-claimable

## Evidence trail

- Identified from: `README.md:29` (per-entity ordering claim), C0 equation
  list in `NEW_PLAN.md:394-399`, and the shared contract model's
  `HeadClaimable/Held/Hidden/Ambiguous` classification in
  `tests/StorageContractProperties.fs:252-256`.
- Code paths: PostgreSQL `submit_command` / `claim_commands`
  (`R__command_routines.sql`; per-entity advisory lock
  `fsm.command/<machine>/<entity>` at line 119); SQLite `submit.sql` /
  `claim.sql` under the `IMMEDIATE` write session; unique index
  `command_unique_seq`.
- Failure scenario: two hosts' claims interleave such that a successor is
  returned while its predecessor is non-terminal, or two commands for one
  entity appear in one batch union.
- Key observations: the deterministic contract model already asserts this
  single-process; the cross-process and fault-injected window is untested.

## Instrumentation

- Workload-side assertion over claim results (union check per entity). SUT-side:
  none exists; a `Reachable` marker on the claim's successor-skip branch would
  help guide search. Missing (no SDK in codebase).

## Open Questions

- Advisory-lock window under asymmetric network faults between participants:
  partial — the lock is one session's statement; asymmetry should degrade to
  latency. Not fault-tested.

### Investigation Log

#### Advisory-lock window under asymmetric network faults between participants

- Examined: `R__command_routines.sql` submit path (lock acquisition and insert
  in one session), PostgreSQL session-level advisory-lock semantics, the C2
  blocked-lock race regression in `CommandInboxTests.fs`.
- Found: the advisory lock is taken and released inside a single
  `submit_command` call on one session; nothing depends on a second
  participant's liveness, so one-sided network faults reduce to latency for
  the lock holder.
- Not found: any fault-injected run demonstrating the degradation under
  asymmetric faults; pgmq/queue visibility is out of scope here.
- Conclusion: `(partial: confirmed single-session lock; fault-behavior untested)` —
  the question stays open until the harness can partition mid-submit.
