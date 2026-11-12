# pre-canceled-call-does-no-work

## Evidence trail

- Identified from: E48 and B11's TAP contract. Provider database boundaries
  normalize cancellation after work reaches them, but most public store
  members currently encode arguments and build programs before that boundary.
- Failure scenario: a caller cancels before invocation; entity or payload
  encoding runs, a callback fires, or a connection opens before the provider
  notices. The caller observes cancellation although work started.
- Contract: an already-canceled token returns `Task.FromCanceled` carrying the
  same token before encoding, callbacks, connection acquisition, or writes.

## Instrumentation

- Deterministic primary oracle: invoke every provider capability with a
  pre-canceled token, assert exact `TaskStatus.Canceled`, matching token, and an
  empty recording trace.
- Workload-side Antithesis observation: count API invocation attempts and the
  first provider-side effect; assert no effect follows a pre-canceled attempt.
- SUT-side assertion: none required.

## Open Questions

- None. B11's chosen null contract also rejects null generic entity IDs, but
  null validation is independent of the no-work cancellation trace.
