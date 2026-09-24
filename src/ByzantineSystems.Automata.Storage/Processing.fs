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

/// <summary>
/// Why a command did not commit.
///
/// Two cases, because there are two kinds of answer and conflating them loses the more useful
/// one. A chart that refuses an event says so in the application's own vocabulary, and that
/// belongs in the audit record as data the application can read back. A row that will not
/// decode, an event the chart has no rule for, a lease that expired: those are facts about the
/// machine, and no <c>'Err</c> exists to express them. A processor cannot invent one, and a
/// contract that demanded it would be answered with a lie.
/// </summary>
[<RequireQualifiedAccess>]
type CommandFailure<'Err> =
    /// <summary>The chart refused the event, with the error it refused it with.</summary>
    | Domain of 'Err

    /// <summary>The machine could not process the command, with a reason meant for a human.</summary>
    | Machine of reason: string

/// <summary>What became of a submitted command.</summary>
[<RequireQualifiedAccess>]
type CommandResult<'EntityId, 'State, 'Event, 'Action, 'Err> =
    /// <summary>Still in the inbox: unclaimed, leased, or waiting behind an earlier sibling.</summary>
    | Pending

    /// <summary>Applied, with the transition it produced.</summary>
    | Committed of CommittedTransition<'EntityId, 'State, 'Event, 'Action>

    /// <summary>Refused, with the reason. The state did not move.</summary>
    | Rejected of rejected: CommandFailure<'Err>

    /// <summary>Given up on, with the reason that ended it. Its entity has been released.</summary>
    | DeadLettered of abandoned: CommandFailure<'Err>

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
/// One ascending page of an entity's transition log, bounded by an exclusive epoch cursor and a
/// row limit.
///
/// The cursor is an epoch rather than an offset, and that is what makes a page deterministic. An
/// offset shifts as rows arrive; an epoch does not, so a page pinned to one returns the same rows
/// however much has been committed since.
/// </summary>
type Page =
    private
        { AfterEpoch: Epoch option
          Limit: int }

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Storage.Page" />.</summary>
[<RequireQualifiedAccess>]
module Page =

    /// <summary>The first page of a history query, with the given row limit.</summary>
    /// <exception cref="T:System.ArgumentException">The limit is below 1.</exception>
    let create (limit: int) : Page =
        if limit < 1 then
            invalidArg (nameof limit) "A page must request at least one row."

        { AfterEpoch = None; Limit = limit }

    /// <summary>A page continuing strictly after the given epoch.</summary>
    /// <exception cref="T:System.ArgumentException">The limit is below 1.</exception>
    let after (epoch: Epoch) (limit: int) : Page =
        if limit < 1 then
            invalidArg (nameof limit) "A page must request at least one row."

        { AfterEpoch = Some epoch
          Limit = limit }

    /// <summary>The exclusive lower epoch bound; <c>None</c> reads from the start of the log.</summary>
    let cursor (page: Page) : Epoch option = page.AfterEpoch

    /// <summary>The maximum number of rows the page requests.</summary>
    let limit (page: Page) : int = page.Limit

/// <summary>
/// Reads what an entity is and what it has been.
///
/// A processor needs the snapshot before it can do anything: resolving a command means knowing
/// the state it starts from, and finishing one means knowing the epoch it expects. The history
/// is the other half of the same question, and it is required rather than optional because an
/// append-only log is something any store can page.
/// </summary>
type IStateReader<'EntityId, 'State, 'Event, 'Action> =

    /// <summary>The entity's current belief, or <c>None</c> when it has never committed.</summary>
    abstract TryGetSnapshot:
        machineId: MachineId * entityId: 'EntityId * ct: CancellationToken ->
            Task<Result<Snapshot<'State> option, StoreError>>

    /// <summary>
    /// One ascending page of the entity's transition log.
    ///
    /// Ascending because this is a log rather than a feed: it is read to follow what happened in
    /// the order it happened, and the epoch that orders it is gapless.
    /// </summary>
    abstract History:
        machineId: MachineId * entityId: 'EntityId * paging: Page * ct: CancellationToken ->
            Task<Result<CommittedTransition<'EntityId, 'State, 'Event, 'Action> list, StoreError>>

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

    /// <summary>Records that the command was refused. No transition, and the state does not move.</summary>
    abstract Reject:
        commandId: CommandId * token: LeaseToken<CommandWork> * failure: CommandFailure<'Err> * ct: CancellationToken ->
            Task<Result<FinalizeOutcome, StoreError>>

    /// <summary>
    /// Gives up on a command. Like <c>Reject</c>, and it releases the entity for the same reason:
    /// a command nobody can process must not stop everything behind it.
    /// </summary>
    abstract DeadLetter:
        commandId: CommandId * token: LeaseToken<CommandWork> * failure: CommandFailure<'Err> * ct: CancellationToken ->
            Task<Result<FinalizeOutcome, StoreError>>

    /// <summary>Reads what became of a command, without waiting for it.</summary>
    abstract TryGetResult:
        commandId: CommandId * ct: CancellationToken ->
            Task<Result<CommandResult<'EntityId, 'State, 'Event, 'Action, 'Err> option, StoreError>>
