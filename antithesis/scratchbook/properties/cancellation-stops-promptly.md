# cancellation-stops-promptly

## Evidence trail

- Identified from: the loop-entry `ct.IsCancellationRequested` checks and
  `waitForWork` cancellation handling in `CommandProcessor.fs:495-543` and
  `ActionDispatcher.fs:267-309`; `Machine.TryAcceptWork` gating `send`;
  hosted-service `StopAsync` wiring in `DependencyInjection/HostedServices.fs`.
- Failure scenario: cancellation lands exactly when a full batch completed
  synchronously and the loop is about to recurse without waiting (the fast
  path that skips the signal wait).
- Key observation: after B10's rewrite this becomes a property of the
  `while` loop's exit condition; today it is entangled with E41.

## Instrumentation

- Workload-side: shutdown marker plus no-claim-after-cancel assertion.
  SUT-side: none; missing.

## Open Questions

- None.
