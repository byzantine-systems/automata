# send-settles-when-outcome-known

## Evidence trail

- Identified from: E44 register entry — `awaitSettled`
  (`Runtime/Machine.fs:399-421`) treats `Ok None` (command gone, e.g. purged
  by retention) as "keep polling", so `send` polls forever on a purged
  command.
- Code paths: `Machine.send` -> `awaitSettled`; retention purge removing
  terminal commands (`purge_commands`); `ResultPollInterval` on
  `TimeProvider`.
- Failure scenario: retention horizon crosses a command while a caller is
  awaiting it (long GC pause, throttled caller, short retention).
- Key observations: known open defect; the property lands with the fix. The
  fix shape (typed "purged/unknown" outcome vs error) is a human decision.

## Instrumentation

- Workload-side: send-return assertion with a purge racing the await.
  SUT-side: none; missing.

## Open Questions

- Outcome shape for purged-while-awaited commands: `(needs human input)`
  (typed outcome vs error).

### Investigation Log

#### Outcome shape for purged-while-awaited commands

- Examined: `Machine.awaitSettled` (`Runtime/Machine.fs:399-421`) and its
  `Ok None` arm; `TryGetResultAsync` semantics in `Storage/Processing.fs`;
  `purge_commands` retention behavior; `CommandResult` union shape;
  `docs/public-api.md` send/commandResult contract text.
- Found: the poll loop has no exit for a command row that no longer exists;
  every candidate outcome shape (typed `Unknown/Purged` case, mapped error,
  timeout) changes public API surface (`CommandResult` or `MachineError`),
  and the docs commit to none.
- Not found: any stated preference in docs, plan, or tests between a typed
  purged outcome and an error mapping; the choice is a product-contract
  decision, not derivable from code.
- Conclusion: `(needs human input)` — owner must pick the outcome shape
  before the E44 fix is designed.
