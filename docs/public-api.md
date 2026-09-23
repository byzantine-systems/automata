# Public API guide

This guide summarizes the implemented public surface. The source XML comments remain the detailed contract for individual fields, union cases, and members.

## Core

`ByzantineSystems.Automata.Core` defines validated `StateId`, `MachineId`, and phantom-typed `EntityId<'entity>` identifiers with `create` and `value` functions. The auto-open helpers `stateId`, `machineId`, and `entityId` are shorthand constructors.

A `Chart<'State,'Event,'Action,'Err>` can be created directly with `Chart.create` or with the `statechart` computation expression. The expression supports:

- Top-level `root` and `classify` declarations.
- `state` and `compound` nodes, with `initial` and `terminal` markers.
- `onEntry` and `onExit` actions.
- `on`, `attempt`, `goto`, and `internalOn` transition rules.

Both construction paths validate the hierarchy and return `Result<Chart<_,_,_,_>, ChartError list>`. `Chart.resolve` is pure and returns either a `Resolution` or a typed `TransitionError`. Query functions expose the root, classifier, nodes, children, and terminal or compound status.

The lower-level `Rule` module provides `transition`, `attempt`, `goto`, `internalOn`, `guarded`, and `invoke`. `Rule.kind` reports the `RuleKind` that built a rule (`Transition`, `Attempt`, `Goto of StateId`, `Internal`, or `Guarded of reason * inner`), which is the part of a rule that is visible without calling it. `Codec<'T>` and its module functions provide typed encode/decode boundaries for stores.

`Chart.fingerprint` returns a `ChartFingerprint`, a SHA-256 digest of the chart's structure: the root, and for every node its id, parent, initial child, terminal flag and the kind of each rule. Nodes are hashed in id order, because sibling declaration order changes nothing at resolution time; rules keep declaration order, because first-match-wins makes their order part of what the chart does.

**The fingerprint is a tripwire, not a proof of equivalence.** Guards, transforms, entry and exit actions and the classifier are all closures, and nothing can see inside them. It catches a chart whose shape was edited without bumping its version; it cannot catch a behaviour change that leaves the shape alone. Replay correctness still rests on the developer bumping `ChartVersion`.

## Runtime

The `machine` computation expression creates a `Machine<'EntityId,'State,'Event,'Action,'Err>` from a machine id and declarations. A chart, a declared `chartVersion`, an initial state, an `IMachineStore` and an `actionQueue` name are required; `processor`, `onTransition`, `logger` and `timeProvider` are optional, and the builder returns every `MachineConfigError` at once rather than the first.

A machine no longer processes anything. `Machine.enqueue` records an event in the durable inbox and returns a `SubmissionOutcome`; some worker, here or on another host, claims it and finishes it. `Machine.commandResult` reads what became of a command. `Machine.send` is `enqueue` plus a wait for the durable outcome, and remains the headline call because it is what most callers want: the documentation states plainly that it now costs a round trip through the database, and points throughput-sensitive callers at `enqueue` and `commandResult`. `Machine.state` reads the current snapshot.

`Machine.startAsync` takes an `IChartRegistry` and registers the chart's structure against its declared version before any work begins. `Registered` and `Matched` proceed; `Mismatched` is reported as data, because whether it is fatal is the host's decision.

`Machine.processor` builds the `CommandProcessor` that drains the inbox: claim a batch, resolve each command against the chart with pure `Chart.resolve`, and finalize it. At most one non-terminal command per entity is claimable, so a claim already excludes that entity across every process; the processor never serialises anything itself and never needs to know which entities exist. A batch therefore cannot contain two commands for one entity, which is why processing it concurrently cannot reorder anything. `PollAsync` does one round and reports a `PollReport`; `RunAsync` loops and answers `ProcessorStop.Drained` or `ProcessorStop.Escalated`, so a supervisor can tell an orderly shutdown from a worker that stopped because something is wrong.

`ProcessorPolicy` carries the batch size, lease, renewal point, polling interval, concurrency, attempt limit, backoff envelope, and `Classify`: the one decision the library cannot make for an application, which is what a failure means for the command that caused it. `ProcessorPolicy.defaultClassify` retries transient infrastructure failures, records a chart's own refusal as a domain error, and dead-letters everything else with a machine-level reason rather than retrying it, because an event the chart has no rule for will not acquire one by waiting. Dead-lettering releases the entity, which matters more here than anywhere else: a command that keeps failing would otherwise hold up every command behind it for that entity, forever.

`Machine.dispatcher` builds the `ActionDispatcher` that delivers what a commit queued, through an `ActionHandler` that receives the whole `LeasedAction` so a destination can deduplicate on `(CommandId, Ordinal)`.

## Resilience

`TransientPolicy` describes short-horizon retry, per-attempt and total timeouts, and optional circuit breaking, for the I/O one machine performs. Durable retry is not here: a command that fails is rescheduled in the inbox, with a backoff the database computes, and keeps its place in its entity's order while it waits.

`TransientPolicy.toPipeline` takes the policy, a name for telemetry, a predicate saying which of a driver's exceptions are worth repeating, and an event sink. The predicate is a parameter because only a store knows which of its exceptions mean "again in a moment" and which mean "never"; this assembly has no reference to any driver. The pipeline it returns is applied **inside** the store adapter, around the driver call, because Polly reads exceptions and everything above that line reads `Result`. A pipeline wrapped outside the conversion would retry nothing.

`SupervisorSpec` and the `Supervisor` module model and validate supervision policy. `Supervisor.start` returns an `ISupervisor` whose `Completion` observes every child, and whose `Events`/`EventsAsync` expose the ordered supervision event log for audit persistence.

## DependencyInjection

`AddAutomata` registers a `BackgroundService` from typed `AutomataOptions`: the root supervisor owns machine generations, each generation runs a `CommandProcessor` and optionally an `ActionDispatcher`, and an `ISupervisionEventStore` receives ordered audit records. A generation that reports `ProcessorStop.Escalated` faults, which is what a supervisor restarts on. `AutomataSupervisorOptions` carries the restart strategy, kind, intensity, period and shutdown budget. `IActionHandler` is the scoped delivery contract, resolved from a fresh scope per action. `InMemorySupervisionStore` keeps one process's restart history for a host that has not registered a durable writer; `PostgresSupervisionStore` is the durable one.

No resilience pipeline is registered here. A host configures short-horizon retry when it builds its `PostgresContext` and durable retry through the machine's processor policy.

## Storage

`ByzantineSystems.Automata.Storage` is four required interfaces. `ICommandInbox` submits commands idempotently, leases runnable ones a batch at a time, and finishes a lease through fenced writes that require the `LeaseToken<CommandWork>` the claim issued; at most one non-terminal command per entity is claimable, so a claim excludes that entity across every process rather than only within one. `IStateReader` reads the current `Snapshot`. `ICommandProcessorStore` is the atomic end of processing a command. `IActionQueue` is the consumer side of delivery. `IMachineStore` aggregates the four.

Supporting types: `CommandId`, `ChartVersion`, `CommandRecord`, `Leased<'kind,'Work>`, `SubmissionOutcome`, `LeaseUpdateOutcome`, `Backoff`, `AuditContext`, and `EventEnvelope`, which pairs an event with its idempotency key, audit context and optional visibility and arrival instants.

`ICommandProcessorStore.Commit` appends a transition, advances the belief, queues the actions and closes the inbox row; `Reject` and `DeadLetter` record the failure instead and leave the state alone. All three release the entity, and all three are **idempotent on the command**: a worker whose connection dropped mid-write can repeat the call and be told `AlreadyFinalized` with the epoch it wrote, rather than that its lease is gone. That is what makes an ambiguous failure safe to retry.

`CommandFailure` is `Domain of 'Err` or `Machine of reason`. Two cases because there are two kinds of answer and conflating them loses the more useful one: a chart that refuses an event says so in the application's own vocabulary, while a row that will not decode or an event with no rule is a fact about the machine, and no `'Err` exists to express it. A processor cannot invent one.

`TransitionDraft` is what pure chart resolution produces. `CommittedTransition` adds what only the database can decide: the authoritative `Epoch`, the `CommandId`, the `ChartVersion` and `CommittedAt`. `FinalizeOutcome` is `Finalized`, `AlreadyFinalized`, `Conflict` or `LeaseLost`.

`IChartRegistry` is the durable record of which chart structure each declared version belongs to. `Register` answers `Registered`, `Matched`, or `Mismatched of stored`. The first sighting of a version claims it and nothing ever overwrites that, so a process whose chart no longer matches its declared version finds out before it writes anything. Every command pins a chart version and a foreign key holds it to one that a chart declared: submitting under an unregistered version returns `StoreError.NotFound`.

## Storage.Postgres

`PostgresMachineStore` implements all four required interfaces from one `MachineStoreOptions`, composing `PostgresCommandInbox`, `PostgresCommandProcessorStore` and `PostgresActionQueue`. `EnsureQueueAsync` creates the machine's action queue and must run before any command commits, because a commit enqueues into it inside its own transaction.

`PostgresContext` pairs a pooled `NpgsqlDataSource` with the resilience every call through it runs under, so one database has one pool and one circuit. `DataSource.create` enables automatic statement preparation; `DataSource.resilience` builds the pipeline with the driver-failure classifier this assembly owns. `Serialization.systemTextJson` creates F#-aware JSON codecs and `Serialization.listOf` derives a list codec from an element codec, which is what keeps the action list in `fsm.transition` agreeing element for element with the messages in the queue. `EntityKey.forEntityId` provides the common projection pair, whose decode returns a `Result` so a corrupt row becomes `StoreError.Serialization` rather than an exception.

`Migrator.migrate` applies the embedded migrations; see [schema evolution](schema-evolution.md) for how the schema and its statements are organised, and for why nothing in F# calls `pgmq.archive` or `pgmq.set_vt` directly.
