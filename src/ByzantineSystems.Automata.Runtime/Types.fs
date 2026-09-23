namespace ByzantineSystems.Automata.Runtime

open System
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage

/// <summary>
/// What the processor does with a command it could not commit.
///
/// This replaces the disposition the resilience policy used to carry. Short-horizon retry is
/// now the store's business and happens below this layer; what is left is the decision only the
/// application can make, which is what a failure means for the command that caused it.
/// </summary>
type CommandDisposition<'Err> =
    /// <summary>Put it back in the inbox for a later attempt, keeping its place in the entity's order.</summary>
    | Retry

    /// <summary>Record the refusal and release the entity. The state does not move.</summary>
    | Reject of refused: CommandFailure<'Err>

    /// <summary>Give up on it permanently, recording why, and release the entity.</summary>
    | DeadLetter of abandoned: CommandFailure<'Err>

    /// <summary>
    /// Treat it as a fault in this worker rather than in the command. The processor stops and
    /// its supervisor decides what happens next; the lease expires and the command is redelivered.
    /// </summary>
    | Escalate of reason: string

/// <summary>Structural defects in a <see cref="T:ByzantineSystems.Automata.Runtime.ProcessorPolicy`1" />.</summary>
type ProcessorPolicyError =
    | BatchBelowOne of int
    | LeaseNotPositive of TimeSpan
    | RenewAfterNotPositive of TimeSpan
    | RenewAfterNotBelowLease of renewAfter: TimeSpan * lease: TimeSpan
    | PollingIntervalNotPositive of TimeSpan
    | ConcurrencyBelowOne of int
    | MaxAttemptsBelowOne of int

/// <summary>
/// How one machine's processor claims and finishes work.
///
/// Nothing here governs ordering, which is the inbox's job and not negotiable from this side.
/// These are throughput and patience settings, plus the one decision the library cannot make
/// for an application: what a failure means.
/// </summary>
type ProcessorPolicy<'Err> =
    {
        /// <summary>How many commands one poll claims. Never two for the same entity.</summary>
        Batch: int

        /// <summary>How long a claim is held before the database considers it abandoned.</summary>
        Lease: TimeSpan

        /// <summary>
        /// How long into a lease the processor pushes the deadline out for work still running.
        /// Must be below <c>Lease</c>, or the extension arrives after the claim it was meant to
        /// protect has already lapsed.
        /// </summary>
        RenewAfter: TimeSpan

        /// <summary>
        /// How often to poll when no wake-up hint arrives. Correctness never depends on a hint,
        /// so this is the upper bound on how long committed work can sit unnoticed.
        /// </summary>
        PollingInterval: TimeSpan

        /// <summary>Upper bound on commands processed at once, across distinct entities.</summary>
        Concurrency: int

        /// <summary>
        /// Attempts before a retryable command is given up on. Counted by the inbox rather than
        /// in this process, so a command that moved between workers still counts its own history.
        /// </summary>
        MaxAttempts: int

        /// <summary>The envelope the database computes each retry's delay from.</summary>
        Backoff: Backoff

        /// <summary>
        /// What a failure means for the command that caused it. A domain rejection arrives here
        /// too, so an application can decide that some of its own errors are worth retrying.
        /// </summary>
        Classify: MachineError<'Err> -> CommandDisposition<'Err>
    }

/// <summary>A processor policy whose structural invariants have been checked.</summary>
type ValidatedProcessorPolicy<'Err> = private ValidatedProcessorPolicy of ProcessorPolicy<'Err>

/// <summary>Construction and validation of <see cref="T:ByzantineSystems.Automata.Runtime.ProcessorPolicy`1" />.</summary>
[<RequireQualifiedAccess>]
module ProcessorPolicy =

    /// <summary>
    /// The default classification.
    ///
    /// Transient infrastructure failures are worth another attempt. A chart that refused an event
    /// is a domain answer and is recorded as one. Everything else is dead-lettered with the
    /// reason rather than retried, because an event the chart has no rule for will not acquire
    /// one by waiting, and a command that cannot be decoded will not decode later. Dead-lettering
    /// releases the entity, which matters more here than anywhere else: a command that keeps
    /// failing would otherwise hold up every command behind it for that entity, forever.
    /// </summary>
    let defaultClassify (error: MachineError<'Err>) : CommandDisposition<'Err> =
        match error with
        | Store(Unavailable _)
        | Store(Concurrency _)
        | Timeout _
        | CircuitOpen _ -> Retry
        | Transition(TransitionError.Rejected domain) -> Reject(CommandFailure.Domain domain)
        | Transition(Unhandled(state, event)) ->
            DeadLetter(CommandFailure.Machine $"no rule handled %s{StateId.value state} for event %s{event}")
        | Transition(GuardFailed(state, reason)) ->
            Reject(CommandFailure.Machine $"a guard on %s{StateId.value state} refused the event: %s{reason}")
        | Transition(UnknownState state) ->
            DeadLetter(CommandFailure.Machine $"the classifier produced %s{StateId.value state}, which is not a leaf")
        | Transition(TargetMismatch(expected, actual)) ->
            DeadLetter(
                CommandFailure.Machine
                    $"the rule's next state resolved to %s{StateId.value actual}, not %s{StateId.value expected}"
            )
        | Store(Serialization(typeName, _)) ->
            DeadLetter(CommandFailure.Machine $"a stored %s{typeName} could not be read")
        | Store(NotFound entity) -> DeadLetter(CommandFailure.Machine $"%s{entity} does not exist")
        | Rejected(InstanceNotRunning status) ->
            Reject(CommandFailure.Machine $"the instance is %A{status} and accepts no further commands")
        | Rejected lifecycle -> Escalate $"the machine is %A{lifecycle}"

    /// <summary>
    /// A safe default: batches of 32, a thirty-second lease renewed at twenty, a five-second
    /// poll, eight concurrent commands and five attempts.
    /// </summary>
    let defaults<'Err> : ProcessorPolicy<'Err> =
        { Batch = 32
          Lease = TimeSpan.FromSeconds 30.
          RenewAfter = TimeSpan.FromSeconds 20.
          PollingInterval = TimeSpan.FromSeconds 5.
          Concurrency = 8
          MaxAttempts = 5
          Backoff = Backoff.defaults
          Classify = defaultClassify }

    /// <summary>Accumulates every defect rather than stopping at the first.</summary>
    let validate (policy: ProcessorPolicy<'Err>) : Result<ValidatedProcessorPolicy<'Err>, ProcessorPolicyError list> =
        let errors =
            [ if policy.Batch < 1 then
                  BatchBelowOne policy.Batch

              if policy.Lease <= TimeSpan.Zero then
                  LeaseNotPositive policy.Lease

              if policy.RenewAfter <= TimeSpan.Zero then
                  RenewAfterNotPositive policy.RenewAfter
              elif policy.RenewAfter >= policy.Lease then
                  RenewAfterNotBelowLease(policy.RenewAfter, policy.Lease)

              if policy.PollingInterval <= TimeSpan.Zero then
                  PollingIntervalNotPositive policy.PollingInterval

              if policy.Concurrency < 1 then
                  ConcurrencyBelowOne policy.Concurrency

              if policy.MaxAttempts < 1 then
                  MaxAttemptsBelowOne policy.MaxAttempts ]

        match errors with
        | [] -> Ok(ValidatedProcessorPolicy policy)
        | errors -> Error errors

/// <summary>Operations on a validated processor policy.</summary>
[<RequireQualifiedAccess>]
module ValidatedProcessorPolicy =

    let internal value (ValidatedProcessorPolicy policy) = policy

/// <summary>What one poll of the command processor did.</summary>
type PollSummary =
    { Claimed: int
      Committed: int
      Rejected: int
      Rescheduled: int
      DeadLettered: int
      /// <summary>Claims another worker had taken over before this one finished with them.</summary>
      LeaseLost: int }

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Runtime.PollSummary" />.</summary>
[<RequireQualifiedAccess>]
module PollSummary =

    /// <summary>A poll that found nothing.</summary>
    let empty: PollSummary =
        { Claimed = 0
          Committed = 0
          Rejected = 0
          Rescheduled = 0
          DeadLettered = 0
          LeaseLost = 0 }

    let internal add (left: PollSummary) (right: PollSummary) : PollSummary =
        { Claimed = left.Claimed + right.Claimed
          Committed = left.Committed + right.Committed
          Rejected = left.Rejected + right.Rejected
          Rescheduled = left.Rescheduled + right.Rescheduled
          DeadLettered = left.DeadLettered + right.DeadLettered
          LeaseLost = left.LeaseLost + right.LeaseLost }

/// <summary>
/// A best-effort, post-commit observer. It cannot change what has already been committed, and a
/// dropped notification costs a side channel rather than a fact: the transition log is the
/// record, and this is a convenience on top of it.
/// </summary>
type TransitionObserver<'EntityId, 'State, 'Event, 'Action> =
    CommittedTransition<'EntityId, 'State, 'Event, 'Action> -> CancellationToken -> Task<unit>

/// <summary>
/// A declaration that may occur at most once in a machine expression. Qualified access is not
/// optional here: half these names are also types in scope, and an unqualified <c>Chart</c> or
/// <c>Logger</c> would resolve to one of them.
/// </summary>
[<RequireQualifiedAccess>]
type MachineDeclaration =
    | Chart
    | ChartVersion
    | Initial
    | Store
    | Processor
    | ActionQueue
    | Observer
    | Logger
    | TimeProvider

/// <summary>Structural defects accumulated by the machine builder.</summary>
type MachineConfigError =
    | MissingChart
    | MissingChartVersion
    | MissingInitialState
    | MissingStore
    | MissingActionQueue
    | InvalidActionQueueName of name: string
    | InvalidProcessorPolicy of ProcessorPolicyError list
    | DuplicateDeclaration of MachineDeclaration
    | InitialStateUnknown of StateId

/// <summary>
/// Everything one machine's workers share. Internal to the runtime assembly: the public surface
/// is the <c>machine</c> expression that builds it and the functions that read it.
/// </summary>
type internal RuntimeConfig<'EntityId, 'State, 'Event, 'Action, 'Err when 'EntityId: equality> =
    { MachineId: MachineId
      Chart: Chart<'State, 'Event, 'Action, 'Err>
      ChartVersion: ChartVersion
      InitialState: 'State
      Store: IMachineStore<'EntityId, 'State, 'Event, 'Action, 'Err>
      Processor: ValidatedProcessorPolicy<'Err>
      ActionQueue: string
      Logger: ILogger
      TimeProvider: TimeProvider }
