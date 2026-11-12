# finalize-retry-reports-stored-resolution

## Evidence trail

- Identified from: E9 and C6 fix (commit d4b0a0d): `AlreadyFinalized` now
  carries `stored: Epoch * status: CommandStatus`; both providers report it;
  shared finalization laws cover it.
- Code paths: `finalize_command` result mapping in
  `PostgresCommandProcessorStore` and `SqliteCommandProcessorStore`;
  generated finalization laws in the shared contract file.
- Failure scenario: commit's answer lost to a connection drop; the retry
  arrives proposing a different resolution (reject instead of commit) and
  must be told what was actually stored.

## Instrumentation

- Workload-side: retry answer equals durable record. SUT-side: none; missing.

## Open Questions

- None.
