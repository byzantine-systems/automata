# committed-action-durable-until-terminal

## Evidence trail

- Identified from: `Storage/Action.fs:39-78` (at-least-once queue contract)
  and the Product Context pass: action loss is the severest delivery failure.
- Code paths: pgmq enqueue inside `finalize_command`; `action/claim.sql`,
  `action/complete.sql`, `action/reschedule.sql`, `action/abandon.sql`;
  SQLite machine-keyed action table; dead-letter records for undeliverable
  actions (`action_dead_letter` per C3).
- Failure scenario: worker dies between the external effect and
  `CompleteAsync` (redelivery, fine); or between claim and effect with the lease
  expiring into a gap where no row is claimable (loss, forbidden).
- Key observation: at-least-once means duplicates are *not* violations here;
  only disappearance is.

## Instrumentation

- Workload-side: delivered-or-abandoned ledger reconciliation at the end of
  the timeline. SUT-side: none; missing.

## Open Questions

- Abandoned-action archive queryability: partial — dead-letter table exists;
  archive interaction untested.

### Investigation Log

#### Abandoned-action archive queryability

- Examined: `action/abandon.sql`, the `action_dead_letter` write added by C3,
  pgmq archive semantics as used by `ensure_action_queue`, maintenance
  retention registration.
- Found: undeliverable actions leave a durable dead-letter row, so
  reconciliation has a record even after the queue row is retired.
- Not found: any test or routine that joins dead letters to pgmq archive
  tables, or documents their retention relationship.
- Conclusion: `(partial: dead-letter record confirmed; archive join untested)`.
