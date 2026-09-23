namespace ByzantineSystems.Automata.Storage

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core

/// <summary>
/// What resolving a command against a chart decided, before anything durable has happened.
///
/// Pure output: every field here comes from the chart and the event. Nothing the database owns
/// appears on it, which is what stops a draft being mistaken for history.
/// </summary>
type TransitionDraft<'EntityId, 'State, 'Event, 'Action> =
    {
        MachineId: MachineId
        EntityId: 'EntityId
        Event: 'Event
        Actions: 'Action list
        FromState: 'State
        ToState: 'State
        /// <summary>Which node in the chain handled the event, which bubbling makes worth recording.</summary>
        HandledBy: StateId
        /// <summary>States exited on the way to the least common ancestor, innermost first.</summary>
        Exited: StateId list
        /// <summary>States entered from the least common ancestor down to the target, outermost first.</summary>
        Entered: StateId list
        /// <summary>The entity's lifecycle after this transition.</summary>
        Status: InstanceStatus
        /// <summary>
        /// When the event was effective in the world. Caller-supplied, because a back-dated
        /// correction is a legitimate thing to record and the system clock cannot know about it.
        /// </summary>
        EffectiveAt: DateTimeOffset
    }

/// <summary>
/// A draft that has been committed, together with everything the database decided about it. The
/// epoch, the command it came from, the chart version it was resolved under and the instant it
/// was written are all authoritative and none of them existed before the commit.
/// </summary>
type CommittedTransition<'EntityId, 'State, 'Event, 'Action> =
    {
        Draft: TransitionDraft<'EntityId, 'State, 'Event, 'Action>
        Epoch: Epoch
        CommandId: CommandId
        ChartVersion: ChartVersion
        /// <summary>When this database wrote the transition. Never caller-supplied.</summary>
        CommittedAt: DateTimeOffset
    }

/// <summary>What became of a submitted command.</summary>
[<RequireQualifiedAccess>]
type CommandResult<'EntityId, 'State, 'Event, 'Action, 'Err> =
    /// <summary>Still in the inbox: unclaimed, leased, or waiting behind an earlier sibling.</summary>
    | Pending

    /// <summary>Applied, with the transition it produced.</summary>
    | Committed of CommittedTransition<'EntityId, 'State, 'Event, 'Action>

    /// <summary>Refused by the chart, with the domain error that refused it.</summary>
    | Rejected of rejected: 'Err

    /// <summary>Given up on, with the error that ended it. Its entity has been released.</summary>
    | DeadLettered of abandoned: 'Err

/// <summary>
/// What finishing a command did.
///
/// <c>AlreadyFinalized</c> is the case the whole operation exists for: a worker whose connection
/// dropped does not know whether it committed, and this lets it ask again and be told what it
/// already did rather than that its lease is gone.
/// </summary>
type FinalizeOutcome =
    /// <summary>The command was finished by this call.</summary>
    | Finalized of committed: Epoch

    /// <summary>This lease had already finished it. The epoch is the one it wrote.</summary>
    | AlreadyFinalized of stored: Epoch

    /// <summary>The entity moved on without this caller. Both epochs are reported so a caller knows how far behind it is.</summary>
    | Conflict of expected: Epoch * actual: Epoch

    /// <summary>Another worker holds the command now; nothing was written.</summary>
    | LeaseLost

/// <summary>
/// Recognisers over the two fenced-write outcomes.
///
/// A lost lease arrives as a case of two different unions depending on which call reported it,
/// and every caller reacts the same way: stop, write nothing, and let whoever holds the work
/// now do it. One pattern spares each of them a two-line match on a union it otherwise has no
/// reason to name.
/// </summary>
[<AutoOpen>]
module LeaseOutcome =

    /// <summary>Matches a finalize that was fenced out.</summary>
    let (|FinalizeFenced|_|) (outcome: FinalizeOutcome) =
        match outcome with
        | FinalizeOutcome.LeaseLost -> Some()
        | _ -> None

    /// <summary>Matches a lease update that was fenced out.</summary>
    let (|LeaseFenced|_|) (outcome: LeaseUpdateOutcome) =
        match outcome with
        | LeaseUpdateOutcome.LeaseLost -> Some()
        | Updated -> None

/// <summary>
/// Reads an entity's current state.
///
/// A processor needs this before it can do anything: resolving a command means knowing the state
/// it starts from, and finishing one means knowing the epoch it expects. History reads arrive
/// with the rest of the read path.
/// </summary>
type IStateReader<'EntityId, 'State> =

    /// <summary>The entity's current belief, or <c>None</c> when it has never committed.</summary>
    abstract TryGetSnapshot:
        machineId: MachineId * entityId: 'EntityId * ct: CancellationToken ->
            Task<Result<Snapshot<'State> option, StoreError>>

/// <summary>
/// The atomic end of processing a command.
///
/// Each operation verifies the lease, records the outcome and releases the entity to its next
/// command, as one transaction. All three are idempotent on the command: repeating one with the
/// lease that already finished it reports what that lease did.
/// </summary>
type ICommandProcessorStore<'EntityId, 'State, 'Event, 'Action, 'Err> =

    /// <summary>
    /// Applies a resolved transition: appends it to the log, advances the belief, and closes the
    /// command. <paramref name="expected" /> is the epoch the caller resolved against; a
    /// different one means the entity moved on and nothing is written.
    /// </summary>
    abstract Commit:
        commandId: CommandId *
        token: LeaseToken<CommandWork> *
        expected: Epoch *
        draft: TransitionDraft<'EntityId, 'State, 'Event, 'Action> *
        ct: CancellationToken ->
            Task<Result<FinalizeOutcome, StoreError>>

    /// <summary>Records that the chart refused the command. No transition, and the state does not move.</summary>
    abstract Reject:
        commandId: CommandId * token: LeaseToken<CommandWork> * error: 'Err * ct: CancellationToken ->
            Task<Result<FinalizeOutcome, StoreError>>

    /// <summary>
    /// Gives up on a command. Like <c>Reject</c>, and it releases the entity for the same reason:
    /// a command nobody can process must not stop everything behind it.
    /// </summary>
    abstract DeadLetter:
        commandId: CommandId * token: LeaseToken<CommandWork> * error: 'Err * ct: CancellationToken ->
            Task<Result<FinalizeOutcome, StoreError>>

    /// <summary>Reads what became of a command, without waiting for it.</summary>
    abstract TryGetResult:
        commandId: CommandId * ct: CancellationToken ->
            Task<Result<CommandResult<'EntityId, 'State, 'Event, 'Action, 'Err> option, StoreError>>
