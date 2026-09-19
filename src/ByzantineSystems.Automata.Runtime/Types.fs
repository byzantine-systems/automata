namespace ByzantineSystems.Automata.Runtime

open System
open System.Threading
open System.Threading.Tasks
open Polly
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Resilience
open ByzantineSystems.Automata.Storage

/// <summary>
/// The observable result of a <see cref="M:ByzantineSystems.Automata.Runtime.Machine.send" />.
/// <c>Deferred</c> means the event was durably queued for later retry, not applied.
/// </summary>
type SendOutcome =
    /// <summary>The transition committed: state, history, and outbox advanced.</summary>
    | Committed

    /// <summary>The idempotency key was already applied; the original receipt stands.</summary>
    | AlreadyApplied

    /// <summary>The event is parked in the durable retry queue under this id.</summary>
    | Deferred of RetryId

    /// <summary>Policy said to ignore the event; nothing durable happened.</summary>
    | Ignored

/// <summary>
/// Raised when a send's disposition is <see cref="F:ByzantineSystems.Automata.Resilience.Disposition.Escalate" />.
/// The runtime has no supervisor of its own; an owner wires actors to
/// <c>ByzantineSystems.Automata.Resilience.Supervisor</c> and lets this fault a child.
/// </summary>
exception EscalatedSend of reason: string

/// <summary>Durable, long-horizon retry policy. Polly governs short retries; this governs the queue.</summary>
type RetryPolicy =
    {
        /// <summary>Total lease attempts before an item is dead-lettered.</summary>
        MaxAttempts: int

        /// <summary>Delay before a failed event may be claimed again.</summary>
        Delay: TimeSpan

        /// <summary>Lease length granted to a claiming worker.</summary>
        Lease: TimeSpan

        /// <summary>Number of items claimed per poll.</summary>
        BatchSize: int

        /// <summary>Polling cadence; a dropped wake-up hint is recovered by polling.</summary>
        PollingInterval: TimeSpan

        /// <summary>Upper bound on concurrently processed items.</summary>
        Concurrency: int
    }

/// <summary>Structural defects in a <see cref="T:ByzantineSystems.Automata.Runtime.RetryPolicy" />.</summary>
type RetryPolicyError =
    | MaxAttemptsBelowOne of int
    | DelayNotPositive of TimeSpan
    | LeaseNotPositive of TimeSpan
    | BatchSizeBelowOne of int
    | PollingIntervalNotPositive of TimeSpan
    | ConcurrencyBelowOne of int

/// <summary>Construction and validation of <see cref="T:ByzantineSystems.Automata.Runtime.RetryPolicy" />.</summary>
[<RequireQualifiedAccess>]
module RetryPolicy =

    /// <summary>A safe default: five attempts, thirty-second delay and lease.</summary>
    let defaults: RetryPolicy =
        { MaxAttempts = 5
          Delay = TimeSpan.FromSeconds 30.
          Lease = TimeSpan.FromSeconds 30.
          BatchSize = 50
          PollingInterval = TimeSpan.FromSeconds 5.
          Concurrency = 4 }

    /// <summary>Accumulates every defect rather than stopping at the first.</summary>
    let validate (policy: RetryPolicy) : Result<RetryPolicy, RetryPolicyError list> =
        let errors =
            [ if policy.MaxAttempts < 1 then
                  MaxAttemptsBelowOne policy.MaxAttempts

              if policy.Delay <= TimeSpan.Zero then
                  DelayNotPositive policy.Delay

              if policy.Lease <= TimeSpan.Zero then
                  LeaseNotPositive policy.Lease

              if policy.BatchSize < 1 then
                  BatchSizeBelowOne policy.BatchSize

              if policy.PollingInterval <= TimeSpan.Zero then
                  PollingIntervalNotPositive policy.PollingInterval

              if policy.Concurrency < 1 then
                  ConcurrencyBelowOne policy.Concurrency ]

        match errors with
        | [] -> Ok policy
        | errors -> Error errors

/// <summary>Structural defects accumulated by the machine builder.</summary>
type MachineConfigError =
    | MissingChart
    | MissingInitialState
    | MissingStore
    | MissingRetry
    | InvalidRetry of ConfigError list
    | InvalidRetryPolicy of RetryPolicyError list
    | InvalidSupervision of SupervisorError list
    | InitialStateUnknown of StateId
    | MailboxCapacityBelowOne of capacity: int
    | IdleTimeoutNotPositive of TimeSpan

/// <summary>
/// A best-effort, post-commit observer: it cannot change an already committed send result.
/// Exceptions and dropped notifications are swallowed at the runtime boundary.
/// </summary>
type TransitionObserver<'EntityId, 'State, 'Event, 'Action> =
    Transition<'EntityId, 'State, 'Event, 'Action> -> CancellationToken -> Task<unit>

/// <summary>
/// Shared configuration for one machine: one chart, one store, one shared resilience
/// pipeline, and the scheduler and observer every entity actor inherits. Internal to the
/// runtime assembly.
/// </summary>
type internal RuntimeConfig<'EntityId, 'State, 'Event, 'Action, 'Err when 'EntityId: equality> =
    { MachineId: MachineId
      Chart: Chart<'State, 'Event, 'Action, 'Err>
      InitialState: 'State
      Store: IStateStore<'EntityId, 'State, 'Event, 'Action>
      RetryQueue: IRetryQueue<'EntityId, 'Event>
      DeadLetter: IDeadLetterStore<'EntityId, 'Event>
      Pipeline: ResiliencePipeline<PipelineResult<'Err>>
      Classify: MachineError<'Err> -> Disposition
      RetryPolicy: RetryPolicy
      Supervisor: SupervisorSpec option
      TimeProvider: TimeProvider
      MailboxCapacity: int
      IdleTimeout: TimeSpan }
