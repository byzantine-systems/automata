# task-completion-context-independent

## Evidence trail

- Identified from: E54 and B11's TAP guidance. The source currently contains
  handwritten `task` and `taskResult` computation expressions in the complete
  `Machine.send` path and other library boundaries.
- Failure scenario: a caller installs a single-threaded synchronization
  context, invokes `Machine.send(...).Result`, and an asynchronously completing
  store posts the continuation back to that blocked context. Neither side can
  progress.
- Contract: supported library tasks use context-free builders; completion does
  not require pumping the invoking context.

## Instrumentation

- Deterministic primary oracle: a non-pumping-context child thread blocks on
  `Machine.send(...).Result` while its fake dependency completes from the
  thread pool; the call must complete within a bounded deadline.
- Workload-side Antithesis observation: mark dependency completion and public
  task completion; throttling may delay the second but cannot make it depend on
  caller-context progress.
- SUT-side assertion: none required.

## Open Questions

- None. The two SqlHydra-generated context factories are unsupported public
  surface and remain exact source-guard exceptions rather than handwritten
  B11 boundaries.
