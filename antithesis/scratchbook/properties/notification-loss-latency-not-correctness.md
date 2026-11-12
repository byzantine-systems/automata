# notification-loss-latency-not-correctness

## Evidence trail

- Identified from: `Storage/Maintenance.fs:58-70`, which states notifications
  are hints and polling is always underneath — a claimed guarantee with no
  fault test anywhere in the suite.
- Code paths: `PostgresWorkNotifications` (LISTEN session, `ListenerConnection.Off`
  option); the polling interval fallback in every `waitForWork`; keep-alive
  ping (`Listener.waitOrPing`).
- Failure scenario: listener connection partitioned or `Off` for the entire
  run while submits continue; every command must still complete via the
  polling interval.
- Key observation: this is the system's own "what if the hint infrastructure
  is entirely gone" claim; Antithesis partitions it for free.

## Instrumentation

- Workload-side: all-submitted-commands-resolved assertion at timeline end.
  SUT-side: none; missing.

## Open Questions

- None.
