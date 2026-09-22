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

The lower-level `Rule` module provides `transition`, `attempt`, `goto`, `internalOn`, `guarded`, and `invoke`. `Codec<'T>` and its module functions provide typed encode/decode boundaries for stores.

## Runtime

The `machine` computation expression creates a `Machine<'EntityId,'State,'Event,'Action,'Err>` from a machine id and declarations. A chart, initial state, combined `IMachineStore`, and either a `RetryConfig` or a pre-built `resiliencePipeline` are required. The builder also accepts `retryPolicy`, `onTransition`, `mailboxCapacity`, `idleTimeout`, and `timeProvider`, and returns all `MachineConfigError` values when configuration is invalid.

Use `EventEnvelope.create` to pair an event with an idempotency key. `Machine.send` returns `Committed`, `AlreadyApplied`, `Deferred`, or `Ignored`, or a typed `MachineError`. `Machine.state` reads the latest snapshot. `Machine.machineId` and `Machine.stopAsync` expose identity and orderly shutdown.

`RetryPump` processes durable retry work. `ActionDispatcher` leases and delivers action-outbox items through an application-provided handler. Both provide one-shot `PollAsync` and continuous `RunAsync` members.

## Resilience

`RetryConfig<'Err>` describes Polly retry, per-attempt and total timeouts, optional circuit breaking, and the final error classifier. `RetryConfig.defaults`, `RetryConfig.validate`, and `RetryConfig.toPipeline` provide the standard lifecycle. The classifier chooses one of `Reject`, `Defer`, `DeadLetter`, `Ignore`, or `Escalate` after short-horizon resilience has completed.

`SupervisorSpec` and the `Supervisor` module model and validate supervision policy. `Supervisor.start` returns an `ISupervisor` whose `Completion` observes every child, and whose `Events`/`EventsAsync` expose the ordered supervision event log for audit persistence.

## DependencyInjection

`ByzantineSystems.Automata.DependencyInjection` hosts a machine under supervision. `AddAutomata` registers a `BackgroundService` from typed `AutomataOptions<'EntityId,'State,'Event,'Action,'Err,'EffectError>`: the keyed Polly pipeline is built once via `AddResiliencePipeline`, the root supervisor owns machine generations, an optional `DispatchActions` runs scoped `IActionHandler` deliveries for the transactional outbox, and an `ISupervisionEventStore` receives ordered audit records. `AutomataSupervisorOptions` carries the restart strategy, kind, intensity, period, and shutdown budget. `IActionHandler` is the scoped delivery contract; `ISupervisionEventStore` is implemented by `InMemorySupervisionStore` and `PostgresSupervisionStore`.

## Storage

`ByzantineSystems.Automata.Storage` separates persistence into `IStateStore`, `IRetryQueue`, `IDeadLetterStore`, and `IActionOutbox`. `IMachineStore` combines all four interfaces. The contracts use cancellation-aware tasks and typed `StoreError` results.

`InMemoryStore` is a thread-safe implementation of `IMachineStore`. It also exposes test-facing views of pending retries, pending outbox actions, and dead letters.

`ICommandInbox` is the durable command inbox, and the unit of ordering for the PostgreSQL authority being built alongside those contracts. It submits commands idempotently, leases runnable ones a batch at a time, and finishes a lease through fenced writes that require the `LeaseToken<CommandWork>` the claim issued. At most one non-terminal command per entity is claimable, so a claim excludes that entity across every process rather than only within one. Supporting types: `CommandId`, `ChartVersion`, `CommandRecord`, `Leased<'kind,'Work>`, `SubmissionOutcome`, `LeaseUpdateOutcome`, `TerminalStatus`, `Backoff`, and `AuditContext`.

`PostgresCommandInbox` implements it from `CommandInboxOptions`: a pooled `NpgsqlDataSource`, an event codec, and entity-key projections whose decode returns a `Result` so a corrupt row becomes `StoreError.Serialization` rather than an exception. `DataSource.create` builds the data source with automatic statement preparation enabled. `Serialization.systemTextJson` and `Serialization.systemTextJsonList` create F#-aware JSON codecs, and `EntityKey.forEntityId` provides the common projection pair. `Migrator.migrate` applies the embedded PostgreSQL migrations; see [schema evolution](schema-evolution.md) for how the schema and its statements are organised.

The PostgreSQL implementation of `IMachineStore` was removed with the v1 schema and returns, against the new contracts, once the command processor lands.
