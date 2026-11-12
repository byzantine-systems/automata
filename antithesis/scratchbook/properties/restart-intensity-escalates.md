# restart-intensity-escalates

## Evidence trail

- Identified from: `Resilience/Supervisor.fs:73-84` — restart intensity is an
  exact sliding window the supervisor maintains itself, "across all
  children"; exceeding it escalates (stop everything, fault
  `SupervisorEscalated`), explicitly because a circuit breaker cannot express
  rarely-restarting children.
- Code paths: `SupervisorSpec` (Intensity, Period); the restart decision path
  (`afterNaturalExit`, `requestRestart`, window bookkeeping in the agent's
  `handleMessage`); escalation via `SupervisorEscalated`
  (`Supervisor.fs:108-113`); hosted defaults `OneForOne`/`Permanent`/1s
  restart delay (`HostedServices.fs:78-82`).
- Failure scenario: a child crash-loops with restart delays comparable to the
  window length; depending on jitter, the same number of restarts either
  clusters inside `Period` (must escalate) or spreads outside it (must not).
- Key observations: the supervisor is a single MailboxProcessor agent —
  decisions serialize, so the window count is atomic per decision; the
  supervisor's own loop is `async` (trampolined), so E41 does not apply.

## Instrumentation

- Workload-side: supervision-event log assertion (Started/Restarted counts
  vs configured intensity; Escalated observed when exceeded). SUT-side: a
  `Reachable` marker on the escalate branch would anchor the boundary;
  missing.

## Open Questions

- OneForAll cascade counting: partial — decisions serialize through one
  mailbox, so the count is atomic; near-simultaneous cascade clustering is
  untested.

### Investigation Log

#### OneForAll cascade counting under near-simultaneous exits

- Examined: `Supervisor.fs` agent loop (`handleMessage`, single
  `inbox.Receive()` at lines 944-952), restart-intensity bookkeeping,
  `RestartStrategy` handling.
- Found: all child-exit messages are processed serially by one agent, so
  window increments and escalation checks cannot interleave; `OneForAll`
  cascades arrive as a burst of serialized `ChildExited` messages.
- Not found: any test clustering many exits inside the window boundary, or a
  fault run exercising the cascade timing.
- Conclusion: `(partial: atomicity confirmed by design; cascade-boundary timing untested)`.
