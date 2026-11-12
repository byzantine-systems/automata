# supervision-audit-bounded

## Evidence trail

- Identified from: E49 register entry — `InMemorySupervisionStore`
  (`DependencyInjection/SupervisionAudit.fs:22-24`) is unbounded; the plan
  re-homes the fix to the DI package ("bounded in place"). Known defect
  today; the property lands with that fix.
- Code paths: `InMemorySupervisionStore` (default when no
  `PostgresSupervisionStore` is registered, `SupervisionAudit.fs:15`);
  `ISupervision.Events` snapshots; audit recording per supervision event
  (`HostedServices.fs:324-332`).
- Failure scenario: a crash-looping child generates a `Restarted` event per
  crash; nothing evicts, so the store grows with the incident it is meant to
  record.
- Key observations: deterministic bound check (fill past the bound, count)
  is the primary oracle; Antithesis adds only the sustained-incident shape.

## Instrumentation

- Workload-side: event-count-at-or-below-bound assertion after an induced
  incident. SUT-side: none; missing.

## Open Questions

- None.
