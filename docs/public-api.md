# Public API guide

This guide summarizes the implemented public surface. The source XML comments remain the detailed contract for individual fields, union cases, and members.

## Core

`ByzantineSystems.Automata.Core` defines validated `StateId`, `MachineId`, and phantom-typed `EntityId<'entity>` identifiers with `create` and `value` functions. The auto-open helpers `stateId`, `machineId`, and `entityId` are shorthand constructors.

A `Chart<'State,'Event,'Action,'Err>` can be created directly with `Chart.create` or with the `statechart` computation expression. The expression supports:

- Top-level `root` and `classify` declarations.
- `state` and `compound` nodes, with `initial` and `terminal` markers.
- `onEntry` and `onExit` actions.
- `on`, `attempt`, `goto`, and `internalOn` transition rules.

Both construction paths validate the hierarchy and return `Result<Chart<_,_,_,_>, ChartError list>`. A `goto` naming a node the chart does not declare is refused there, as `UnknownGotoTarget`, rather than on the first event that reaches it. `Chart.resolve` is pure and returns either a `Resolution` or a typed `TransitionError`. Query functions expose the root, classifier, nodes, children, and terminal or compound status.

A `state` or `compound` block is a `NodeDraft` value, so fragments are reusable: yield one wherever it is needed, splice a list with `yield!` (in a chart or inside a compound), and give a second copy of the same fragment its own ids with `Fragment.prefix`, which renames every node the fragment declares along with the initial children and goto targets that name them. The classifier is the host's code and must return the prefixed ids; see [chart fragments](fragments.md).

The lower-level `Rule` module provides `transition`, `attempt`, `goto`, `internalOn`, `guarded`, and `invoke`. `Rule.kind` reports the `RuleKind` that built a rule (`Transition`, `Attempt`, `Goto of StateId`, `Internal`, or `Guarded of reason * inner`), which is the part of a rule that is visible without calling it. `Codec<'T>` and its module functions provide typed encode/decode boundaries for stores.

`Chart.fingerprint` returns a `ChartFingerprint`, a SHA-256 digest of the chart's structure: the root, and for every node its id, parent, initial child, terminal flag and the kind of each rule. Nodes are hashed in id order, because sibling declaration order changes nothing at resolution time; rules keep declaration order, because first-match-wins makes their order part of what the chart does.

**The fingerprint is a tripwire, not a proof of equivalence.** Guards, transforms, entry and exit actions and the classifier are all closures, and nothing can see inside them. It catches a chart whose shape was edited without bumping its version; it cannot catch a behaviour change that leaves the shape alone. Replay correctness still rests on the developer bumping `ChartVersion`.

## Runtime

The `machine` computation expression creates a `Machine<'EntityId,'State,'Event,'Action,'Err>` from a machine id and declarations. A chart, a declared `chartVersion`, an initial state and an `IMachineStore` are required; `processor`, `onTransition`, `logger` and `timeProvider` are optional, and the builder returns every `MachineConfigError` at once rather than the first. The action queue is not declared here: the store owns it, so it is named once, where it is created.

A machine no longer processes anything. `Machine.enqueue` records an event in the durable inbox and returns a `SubmissionOutcome`; some worker, here or on another host, claims it and finishes it. `Machine.commandResult` reads what became of a command. `Machine.send` is `enqueue` plus a wait for the durable outcome, and remains the headline call because it is what most callers want: the documentation states plainly that it now costs a round trip through the database, and points throughput-sensitive callers at `enqueue` and `commandResult`. `Machine.state` reads the current snapshot.

`Machine.startAsync` takes an `IChartRegistry`, boots the store when it offers `IStoreBoot`, then registers the chart's structure against its declared version before any work begins. It answers `Startup.Started` with the registration, or `Startup.Refused` with the `BootDefect`s that stopped it, in which case nothing was registered or started. `Registered` and `Matched` proceed; `Mismatched` is reported as data, because whether it is fatal is the host's decision. When the store offers `IWorkNotifications`, the machine also listens for work announced by other processes, and a failed listener is retried on the polling interval rather than faulting anything.

`Machine.processor` builds the `CommandProcessor` that drains the inbox: claim a batch, resolve each command against the chart with pure `Chart.resolve`, and finalize it. At most one non-terminal command per entity is claimable, so a claim already excludes that entity across every process; the processor never serialises anything itself and never needs to know which entities exist. A batch therefore cannot contain two commands for one entity, which is why processing it concurrently cannot reorder anything. `PollAsync` does one round and reports a `PollReport`; `RunAsync` loops and answers `ProcessorStop.Drained` or `ProcessorStop.Escalated`, so a supervisor can tell an orderly shutdown from a worker that stopped because something is wrong.

`ProcessorPolicy` carries the batch size, lease, renewal point, polling interval, concurrency, attempt limit, backoff envelope, and `Classify`: the one decision the library cannot make for an application, which is what a failure means for the command that caused it. `ProcessorPolicy.defaultClassify` retries transient infrastructure failures, records a chart's own refusal as a domain error, and dead-letters everything else with a machine-level reason rather than retrying it, because an event the chart has no rule for will not acquire one by waiting. Dead-lettering releases the entity, which matters more here than anywhere else: a command that keeps failing would otherwise hold up every command behind it for that entity, forever.

`Machine.dispatcher` builds the `ActionDispatcher` that delivers what a commit queued, through an `ActionHandler` that receives the whole `LeasedAction` so a destination can deduplicate on `(CommandId, Ordinal)`. A handler that throws is logged with its exception; one that reports an `Error` is logged as having failed, without the value, which is the application's own.

Both refuse **poison**: work delivered more often than `MaxAttempts` is dead-lettered or abandoned without being run. The ordinary retry path cannot get there, because a reported failure raises the attempt count along with the delivery count; only deliveries that ended with no outcome at all, a handler that took its process down or ran past its lease, push past it, and nothing else would ever stop them.

## Resilience

`TransientPolicy` describes short-horizon retry, per-attempt and total timeouts, and optional circuit breaking, for the I/O one machine performs. Durable retry is not here: a command that fails is rescheduled in the inbox, with a backoff the database computes, and keeps its place in its entity's order while it waits.

`TransientPolicy.toPipeline` takes the policy, a name for telemetry, a predicate saying which of a driver's exceptions are worth repeating, and an event sink. The predicate is a parameter because only a store knows which of its exceptions mean "again in a moment" and which mean "never"; this assembly has no reference to any driver. The pipeline it returns is applied **inside** the store adapter, around the driver call, because Polly reads exceptions and everything above that line reads `Result`. A pipeline wrapped outside the conversion would retry nothing.

`SupervisorSpec` and the `Supervisor` module model and validate supervision policy. `Supervisor.start` returns an `ISupervisor` whose `Completion` observes every child, and whose `Events`/`EventsAsync` expose the ordered supervision event log for audit persistence.

## DependencyInjection

`AddAutomata` registers a `BackgroundService` from typed `AutomataOptions`. `AutomataSupervisorOptions.defaults name` fills in the supervisor, and `Actions` says whether this host delivers the machine's actions: `ActionDelivery.registered<'EntityId, 'Action, 'EffectError>` resolves the registered `IActionHandler` per action, and `ActionDelivery.Elsewhere` leaves delivery to another fleet. It is a union rather than a flag because the handler's type is what fixes `'EffectError`; with nothing mentioning it, F# would infer `obj` and the host would ask for a handler nobody registered. A generation's workers start only after its machine has started, so none polls a queue its boot has not created yet. The root supervisor owns machine generations, each generation runs a `CommandProcessor` and optionally an `ActionDispatcher`, and an `ISupervisionEventStore` receives ordered audit records. A generation that reports `ProcessorStop.Escalated` faults, which is what a supervisor restarts on. `AutomataSupervisorOptions` carries the restart strategy, kind, intensity, period and shutdown budget. `IActionHandler` is the scoped delivery contract, resolved from a fresh scope per action. `InMemorySupervisionStore` keeps one process's restart history for a host that has not registered a durable writer; `PostgresSupervisionStore` is the durable one.

No resilience pipeline is registered here. A host configures short-horizon retry when it builds its `PostgresContext` and durable retry through the machine's processor policy.

A store that refuses to boot faults the generation with `AutomataBootRefusedException`, the same way a fingerprint mismatch does.

`AddAutomataMaintenance` registers `MaintenanceService` once per database: announcements every `NotifyEvery`, and a pass every `RunEvery` that reaps leases, applies retention and reports drift. `MaintenanceScheduler.InDatabase` asks pg_cron to run both ticks and falls back to `InProcess` when it cannot, because maintenance is not optional; while the database runs them, the service still checks drift so it reaches `ILogger`. Drift is repaired only with `RepairDrift = true`. A failed tick is logged and waited out; a loop that faults with something not understood stops its sibling and faults the service.

## Storage

### Required

`ByzantineSystems.Automata.Storage` is four required interfaces. `ICommandInbox` submits commands idempotently, leases runnable ones a batch at a time, and finishes a lease through fenced writes that require the `LeaseToken<CommandWork>` the claim issued; at most one non-terminal command per entity is claimable, so a claim excludes that entity across every process rather than only within one. `IStateReader` reads the current `Snapshot` and pages the transition log. A `Page` carries an exclusive `Epoch` cursor rather than an offset, which is what makes a page deterministic: an offset shifts as rows arrive, so paging a growing log with one silently skips or repeats, while a page pinned to an epoch returns the same rows however much has been committed since. `ICommandProcessorStore` is the atomic end of processing a command. `IActionQueue` is the consumer side of delivery. `IMachineStore` aggregates the four.

Supporting types: `CommandId`, `ChartVersion`, `CommandRecord`, `Leased<'kind,'Work>`, `SubmissionOutcome`, `LeaseUpdateOutcome`, `Backoff`, `AuditContext`, and `EventEnvelope`, which pairs an event with its idempotency key, audit context and optional visibility and arrival instants.

`ICommandProcessorStore.Commit` appends a transition, advances the belief, queues the actions and closes the inbox row; `Reject` and `DeadLetter` record the failure instead and leave the state alone. All three release the entity, and all three are **idempotent on the command**: a worker whose connection dropped mid-write can repeat the call and be told `AlreadyFinalized` with the epoch it wrote, rather than that its lease is gone. That is what makes an ambiguous failure safe to retry.

`CommandFailure` is `Domain of 'Err` or `Machine of reason`. Two cases because there are two kinds of answer and conflating them loses the more useful one: a chart that refuses an event says so in the application's own vocabulary, while a row that will not decode or an event with no rule is a fact about the machine, and no `'Err` exists to express it. A processor cannot invent one.

`TransitionDraft` is what pure chart resolution produces. `CommittedTransition` adds what only the database can decide: the authoritative `Epoch`, the `CommandId`, the `ChartVersion` and `CommittedAt`. `FinalizeOutcome` is `Finalized`, `AlreadyFinalized`, `Conflict` or `LeaseLost`.

### Optional capabilities

Two capabilities sit outside the required four, discovered by `Store.tryTemporal` and `Store.tryCorrections` rather than declared on `IMachineStore`. A type test is the only shape where adding a capability breaks nobody: members returning `option` would force every provider to write `None` for capabilities it has never heard of, and declaring one separately when a machine is wired would let a caller hand over a reader belonging to a different store than the one it configured. `Machine.temporal` and `Machine.corrections` return the options; `Machine.history` is always available because history is required.

`ITemporalReader` reads the past. `ValidAt` gives today's opinion about an instant of business time; `AsOf` gives the opinion held at a second instant, about the first. Holding one still and moving the other is how a change of mind becomes visible as a change of mind. Both answer with a `Belief`, which is a `Snapshot` placed on two time axes plus the command and chart version that produced it, so "what is it now" and "what was it then" yield values that compare directly. An open bound crosses as `None` rather than as a sentinel instant.

`ICorrectionStore` changes it. `Correct` supersedes an entity's belief timeline from an instant onward with a supplied list, in one transaction; superseded beliefs are archived rather than overwritten, so the previous opinion stays answerable through `AsOf`. The beliefs must be ascending and at or after the corrected instant, each runs until the next begins, and the last is left open, so a gap or an overlap is not expressible. An empty list asserts the entity had no belief from that instant onward.

**It carries no policy, deliberately.** Which chart would have decided a state, whether replaying a command against it still resolves, and what to do when it does not are the caller's questions. `CorrectionPolicy` and `ReplayError` are not part of this release; keeping them out of the contract is what let the contract ship.

Reading the past and changing it are two interfaces rather than one because they are two separate rights: a store may offer time travel without offering back-dating, and a deployment may want the reader everywhere while the correction path is reachable only where an operator has a reason.

`IStoreBoot` checks a store's preconditions and performs the idempotent writes it needs before serving, and writes nothing when a check fails. `BootReport` is `Ready` or `Refused of BootDefect list`, where a `BootDefect` is `SchemaMissing`, `SchemaBehind`, `SchemaAhead`, `MissingPrerequisite` or `Misconfigured`. `IWorkNotifications` calls back when a store says work may be waiting; a callback is a hint, and losing every one costs the polling interval and never a command. Both are discovered by `Store.tryBoot` and `Store.tryNotifications`.

`IDatabaseMaintenance` maintains a whole database rather than one machine: `NotifyPending`, `Run`, `DetectDrift`, `RepairDrift`, `Schedule` and `Unschedule`. `RetentionPolicy` says how long terminal commands, superseded beliefs and delivered actions are kept, each as `Retain.Forever` or `Retain.For span`; `RetentionPolicy.keepEverything` is the default, because deleting audit data is never a default.

`IChartRegistry` is the durable record of which chart structure each declared version belongs to. `Register` answers `Registered`, `Matched`, or `Mismatched of stored`. The first sighting of a version claims it and nothing ever overwrites that, so a process whose chart no longer matches its declared version finds out before it writes anything. Every command pins a chart version and a foreign key holds it to one that a chart declared: submitting under an unregistered version returns `StoreError.NotFound`.

## Storage.Postgres

`MachineStoreOptions.forEntityId<'Tag, 'State, 'Event, 'Action, 'Err> context queue` fills in every default a host would otherwise write out: JSON codecs, `EntityId` keys, keeping everything, reaping leases five minutes after they expire, and listening on the store's own data source. Override any of it with `{ … with }`. Its boot refuses a queue name outside `^[a-z_][a-z0-9_]*$` as `Misconfigured`, before the database is asked anything.

`PostgresMachineStore` implements all four required interfaces from one `MachineStoreOptions`, composing `PostgresCommandInbox`, `PostgresCommandProcessorStore` and `PostgresActionQueue`. Its boot checks the extensions, the schema and the migration journal against the migrations this build embeds, then creates the machine's action queue and registers the machine's `Retention` and `ReapAfter` for maintenance. The queue has to exist before any command commits, because a commit enqueues into it inside its own transaction; `EnsureQueueAsync` remains for hosts that start a store without a machine.

`Listener` says where the machine listens for announcements. `ListenerConnection.SameDataSource` holds one connection from the store's data source; `Direct` opens a dedicated unpooled one, for a data source behind a transaction-mode pooler, where `LISTEN` does not survive; `Off` relies on polling alone. The listener never joins an ambient transaction, pings on a keepalive so a silently dead connection fails rather than hangs, and fires both callbacks whenever it subscribes, since anything announced while it was away is gone.

`PostgresMaintenance` implements `IDatabaseMaintenance` over a `PostgresContext`, one `fsm.*` routine per member, which are the same routines pg_cron runs.

`PostgresContext` pairs a pooled `NpgsqlDataSource` with the resilience every call through it runs under, so one database has one pool and one circuit. `DataSource.create` enables automatic statement preparation; `DataSource.resilience` builds the pipeline with the driver-failure classifier this assembly owns. `Serialization.systemTextJson` creates F#-aware JSON codecs and `Serialization.listOf` derives a list codec from an element codec, which is what keeps the action list in `fsm.transition` agreeing element for element with the messages in the queue. `EntityKey.forEntityId` provides the common projection pair, whose decode returns a `Result` so a corrupt row becomes `StoreError.Serialization` rather than an exception.

`PostgresTemporalStore` implements both optional capabilities and is composed into `PostgresMachineStore`, so both discoveries answer `Some` for the PostgreSQL store.

`Migrator.migrate` applies the embedded migrations; see [schema evolution](schema-evolution.md) for how the schema and its statements are organised, and for why nothing in F# calls `pgmq.archive` or `pgmq.set_vt` directly.
