# poll-loop-bounded-stack-and-memory

## Evidence trail

- Identified from: E41 register entry; the recursive `task` loops at
  `Runtime/CommandProcessor.fs:506-543`, `Runtime/ActionDispatcher.fs:277-309`,
  `ObserverDispatcher.drain`, `Core/Boundary.fs:96 Recurring.repeat`, and
  `Runtime/Machine.fs:399 awaitSettled`. F# `task` has no tail calls: one
  pending state machine per iteration; synchronous full-batch drains
  (`return! loop totals` when `Claimed = Batch`) also recurse on the stack.
- Failure scenario: a warm SQLite file or fast store makes every poll
  complete synchronously; 100k iterations leak state machines and eventually
  raise uncatchable `StackOverflowException`.
- Key observations: diagnosis is from F# semantics, not yet from a failing
  test. NEW_PLAN B10 mandates writing the failing test first (100k
  iterations, flat `GC.GetTotalMemory` between checkpoints), then rewriting
  as `while` loops with `Task.Yield()` on the synchronous full-batch path.
  Property validates the *rewritten* loop; it is a known defect today.

## Instrumentation

- Workload-side: iteration-count and memory-delta assertion inside the driver
  (or a dedicated long-running loop harness). SUT-side: none; missing.
  Deterministic counterpart (the B10 stress test) is the primary oracle;
  Antithesis adds sustained-growth under fault-stretched iterations.

## Open Questions

- Robust N and memory tolerance inside a 15-minute timeline: partial — plan
  targets 100k iterations vs baseline; final tolerance TBD when instrumented.

### Investigation Log

#### Robust N and memory tolerance inside a 15-minute timeline

- Examined: NEW_PLAN B10 (test-first mandate: 100k iterations, flat
  `GC.GetTotalMemory(true)` between 10k and 100k checkpoints), the four loop
  sites, prior experience with Expecto suite runtime in this repo
  (PostgreSQL integration suite: ~16s for 149 tests).
- Found: 100k synchronous iterations complete in seconds against a fake
  store, so the deterministic stress test fits comfortably; no timeline
  budget conflict for the local oracle.
- Not found: measurements of the same loop inside an Antithesis timeline with
  fault-stretched iterations; the tolerance for that context cannot be set
  until the harness exists.
- Conclusion: `(partial: deterministic N=100k chosen; fault-context tolerance TBD at instrumentation)`.
