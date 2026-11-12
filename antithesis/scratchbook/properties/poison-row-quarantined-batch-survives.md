# poison-row-quarantined-batch-survives

## Evidence trail

- Identified from: E6 and C3 fix (commit 46ae6ed): `ClaimRecovery.collect`
  decodes per row; poison rows quarantined (dead letter for commands, durable
  abandon for actions) fenced by the token they just received; healthy rows
  returned; failed quarantine leaves the row leased for reclamation.
- Code paths: `Storage/ClaimRecovery.fs`; `PostgresCommandInbox.Claim`,
  `SqliteCommandInbox.Claim`, both action queues; generated poison-position
  laws (commands and actions, both providers).
- Failure scenario: quarantine follow-up write fails exactly when the batch
  is being processed; the poison row returns after lease expiry and must be
  quarantined again, never returned to a worker.

## Instrumentation

- Workload-side: batch-returned check plus quarantine-record check. SUT-side:
  a `Reachable` marker on the quarantine branch would guide search into the
  rare path; missing.

## Open Questions

- None.
