# retention-watermark-exact

## Evidence trail

- Identified from: E5 (Suffix heuristic assumed epoch order equals
  `effective_at` order, broken by corrections) fixed in C7/90e5ed3 via the
  `entity_purge(machine_id, entity_id, max_purged_effective_at)` watermark
  with a `'-infinity'` sentinel; `Incomplete = max_purged_effective_at >= from`.
- Code paths: `transition/purge_watermark.sql`, `transition/suffix.sql`,
  `run_maintenance` watermark maintenance;
  `tests/.../ReplayRetentionProperties.fs` (extensive generated coverage).
- Failure scenario: purge advancing the watermark concurrently with a suffix
  read on another connection.

## Instrumentation

- Workload-side: suffix-vs-purge-log reconciliation. SUT-side: none; missing.

## Open Questions

- None.
