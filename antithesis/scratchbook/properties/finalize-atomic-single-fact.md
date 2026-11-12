# finalize-atomic-single-fact

## Evidence trail

- Identified from: `docs/index.md:9-12` ("one fact") and `docs/schema-evolution.md:38-40`;
  finalize transaction in `sql/command/finalize.sql` and
  `R__finalize_routines.sql` (transition append + belief advance + pgmq
  enqueue + command close in one statement/transaction).
- Code paths: `PostgresCommandProcessorStore.Commit`;
  `SqliteCommandProcessorStore` (enqueue skipped for `Discard`, still
  transactional); `ICommandProcessorStore` contract
  (`Storage/Processing.fs:215-253`).
- Failure scenario: worker killed inside the finalize statement; connection
  dropped after server-side commit before the client sees the ack; a second
  connection reading the entity mid-finalize observes a partial commit.
- Key observation: C4 moved action routing into the same transaction
  (registered queue read from `machine_maintenance.action_queue`), closing
  the wrong-queue hole.

## Instrumentation

- Workload-side: joint observation (snapshot epoch, history count, queue
  depth, command status) after recovery; must be consistent. SUT-side: an
  `Always` inside the store after finalize's transaction would anchor the
  invariant at the source; missing.
- Additional workload observation (added by evaluation synthesis, wildcard
  lens): after `Machine.send` returns, a `Machine.state` readback must
  reflect the committed state — the snapshot advances in the same finalize
  transaction, so this is a cheap client-visible consequence of the same
  invariant, not a separate property.

## Open Questions

- pgmq visibility before commit: partial — enqueue is inside the transaction;
  pgmq read visibility assumed transaction-aligned, not fault-tested.

### Investigation Log

#### pgmq visibility before commit

- Examined: `R__finalize_routines.sql` (pgmq `send` inside the finalize
  transaction), `sql/action/claim.sql` (visibility of queued rows to claims),
  the pgmq usage notes in `docs/schema-evolution.md`.
- Found: the enqueue statement executes within the finalizing transaction, so
  an uncommitted action row cannot be seen by another session's claim under
  read-committed semantics.
- Not found: a fault-injected claim attempted exactly between enqueue and
  commit; pgmq-internal behavior under share-lock edge cases.
- Conclusion: `(partial: transaction containment confirmed; fault-window untested)`.
