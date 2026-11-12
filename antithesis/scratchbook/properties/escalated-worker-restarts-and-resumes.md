# escalated-worker-restarts-and-resumes

## Evidence trail

- Identified from: `HostedServices.fs:198-200` — the hosted processor raises
  `AutomataEscalationException` on `ProcessorStop.Escalated`, i.e. escalation
  is a child crash from the supervisor's point of view; supervisor defaults
  restart it (`Permanent`, `OneForOne`, `HostedServices.fs:78-82`).
- Code paths: `CommandProcessor.RunAsync` escalation return
  (`CommandProcessor.fs:515-516`); the unresolved-claim dead-letter path
  (`CommandProcessor.fs:384-395`: `DeliveryCount > MaxAttempts` dead-letters
  without another attempt); supervision audit recording
  (`HostedServices.fs:324-332`).
- Failure scenario: escalation leaves the escalated command claimed; the
  restarted worker (or the lease-expiry path) must resolve it as a dead
  letter rather than re-attempting it forever, while other entities'
  commands keep completing.
- Key observations: composes `terminal-command-releases-entity` (dead letter
  opens the head) with supervisor restart; the interesting race is restart
  delay versus the escalated command's lease deadline.

## Instrumentation

- Workload-side: post-escalation completions for other entities observed;
  escalated command ends `DeadLettered`. SUT-side: none; missing.

## Open Questions

- None.
