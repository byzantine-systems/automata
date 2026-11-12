# breaker-fails-fast-then-recovers

## Evidence trail

- Identified from: `Resilience/TransientPolicy.fs:175-200` — pipeline order
  total timeout -> retry -> breaker -> attempt timeout; only
  `isTransient` failures open the breaker; `Breaker` is `None` in defaults
  (lines 96-105), so hosts opt in.
- Code paths: `TransientPolicy.toPipeline`; `Db.classify` mapping driver
  failures; `MachineError.CircuitOpen` surfaced to the runtime loops
  (`CommandProcessor.fs:534`, `ActionDispatcher.fs:295` log it as
  "circuit-open"); `BreakerConfig` (FailureRatio, MinimumThroughput,
  SamplingDuration, BreakDuration).
- Failure scenario: database partitioned long enough to trip the breaker;
  restored during `BreakDuration`; the half-open probe races the next call —
  success closes the circuit, another failure re-opens it.
- Key observations: both directions matter (never-open = timeout cascade;
  never-close = permanent outage after recovery). Workload must configure a
  breaker with a short `BreakDuration` and `SamplingDuration` to fit a
  timeline.

## Instrumentation

- Workload-side: observe a `CircuitOpen`-classified failure during the break
  and a success after recovery. SUT-side: none; missing.

## Open Questions

- None.
