namespace ByzantineSystems.Automata.Storage

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core

/// <summary>
/// One belief about an entity, with both windows that place it.
///
/// A <see cref="T:ByzantineSystems.Automata.Core.Snapshot`1" /> answers "what is this entity
/// now". This answers "what did we think, and when did we think it", which needs the windows as
/// much as it needs the state: the same state asserted over two different valid-time intervals
/// is two different claims, and the same claim held over two different system-time intervals is
/// a claim that was superseded.
/// </summary>
type Belief<'EntityId, 'State> =
    {
        MachineId: MachineId
        EntityId: 'EntityId
        /// <summary>
        /// What was believed, in the same shape
        /// <see cref="M:ByzantineSystems.Automata.Runtime.Machine.state" /> answers with. A belief
        /// is a snapshot placed on two time axes, so "what is it now" and "what was it then"
        /// yield values that can be compared directly rather than field by field.
        /// </summary>
        Snapshot: Snapshot<'State>
        /// <summary>The command that produced this belief, and the chart it was decided by.</summary>
        CommandId: CommandId
        ChartVersion: ChartVersion
        /// <summary>When this belief started being true in the world.</summary>
        ValidFrom: DateTimeOffset
        /// <summary>When it stopped. <c>None</c> means it is still true.</summary>
        ValidTo: DateTimeOffset option
        /// <summary>When this database started holding it.</summary>
        KnownFrom: DateTimeOffset
        /// <summary>When it stopped holding it. <c>None</c> means nothing has superseded it.</summary>
        KnownTo: DateTimeOffset option
    }

/// <summary>
/// Reading the past.
///
/// Optional, and discovered by type test rather than required of every store. A provider that
/// keeps only the current state is still a complete provider; it simply cannot answer these, and
/// the type system says so rather than a runtime error saying it later.
/// </summary>
type ITemporalReader<'EntityId, 'State> =

    /// <summary>
    /// Today's opinion about an instant of business time. Equivalent to <c>AsOfAsync</c> with
    /// <c>knownAt</c> set to now, and offered separately because that is the question most
    /// callers actually have.
    /// </summary>
    abstract ValidAtAsync:
        machineId: MachineId * entityId: 'EntityId * validAt: DateTimeOffset * cancellationToken: CancellationToken ->
            Task<Result<Belief<'EntityId, 'State> option, StoreError>>

    /// <summary>
    /// The opinion held at <paramref name="knownAt" />, about <paramref name="validAt" />.
    ///
    /// Holding one still and moving the other is how a change of mind becomes visible as a change
    /// of mind: the same instant in the world, two different answers, and the difference is the
    /// explanation an operator is asking for.
    /// </summary>
    abstract AsOfAsync:
        machineId: MachineId *
        entityId: 'EntityId *
        validAt: DateTimeOffset *
        knownAt: DateTimeOffset *
        cancellationToken: CancellationToken ->
            Task<Result<Belief<'EntityId, 'State> option, StoreError>>

/// <summary>
/// One belief a correction asserts.
///
/// Deliberately free of policy. Which chart would have decided this state, whether replaying a
/// command against it still resolves, and what to do when it does not are all questions a caller
/// answers before it gets here. A storage contract that carried them would be guessing at
/// answers nobody has needed yet.
/// </summary>
type CorrectedBelief<'State> =
    {
        /// <summary>
        /// What is now asserted to have been true, including the epoch it is attributed to. A
        /// correction asserting a state that no single transition produced names the last one
        /// that did, so the belief still points at something in the log.
        ///
        /// Named for what it is rather than reusing <c>Snapshot</c>, which
        /// <see cref="T:ByzantineSystems.Automata.Storage.Belief`2" /> already carries: a bare
        /// <c>_.Snapshot</c> in a lambda would otherwise resolve to whichever record was
        /// declared last rather than to the one the caller meant.
        /// </summary>
        Asserted: Snapshot<'State>
        ValidFrom: DateTimeOffset
        CommandId: CommandId
        ChartVersion: ChartVersion
    }

/// <summary>What correcting a belief timeline did.</summary>
type CorrectionOutcome =
    /// <summary>The entity moved before the operator correction could acquire its fence.</summary>
    | Conflict of actual: Epoch
    /// <summary>
    /// The timeline was rewritten. <c>Superseded</c> counts the beliefs that were archived, which
    /// is what an operator checks against what they expected to change.
    /// </summary>
    | Corrected of superseded: int

    /// <summary>
    /// Nothing covered the corrected range, so nothing was superseded and the supplied beliefs
    /// are simply the entity's history from that instant. Not an error: correcting an entity that
    /// has no belief there is how one is created out of order.
    /// </summary>
    | NothingSuperseded

/// <summary>
/// Changing the past.
///
/// Optional, and separate from <see cref="T:ByzantineSystems.Automata.Storage.ITemporalReader`2" />
/// on purpose: reading what was believed and rewriting it are different rights. A store may offer
/// time travel without offering back-dating, and a deployment may want the reader everywhere
/// while the correction path is reachable only where an operator has a reason. One interface
/// would make that distinction unexpressible.
/// </summary>
type ICorrectionStore<'EntityId, 'State> =

    /// <summary>
    /// Supersedes an entity's belief timeline from <paramref name="validFrom" /> onward with
    /// <paramref name="beliefs" />, in one transaction.
    ///
    /// Superseded beliefs are archived rather than overwritten, so the previous opinion stays
    /// answerable through <c>AsOfAsync</c>. That is the whole point: a correction that erased what it
    /// replaced would leave an audit record claiming the new answer had always been the answer.
    ///
    /// The beliefs must be ordered by <c>ValidFrom</c>: when non-empty, the first starts exactly
    /// at <paramref name="validFrom" /> and each later one strictly after the previous. Each one
    /// runs until the next begins and the last is left open, so a gap or an overlap in the
    /// corrected timeline is not expressible.
    ///
    /// An empty list asserts that the entity had no belief from <paramref name="validFrom" />
    /// onward, which is how a correction undoes one rather than replacing it.
    /// </summary>
    abstract CorrectAsync:
        machineId: MachineId *
        entityId: 'EntityId *
        expected: Epoch *
        validFrom: DateTimeOffset *
        beliefs: CorrectedBelief<'State> list *
        cancellationToken: CancellationToken ->
            Task<Result<CorrectionOutcome, StoreError>>

/// <summary>The committed events a correction has to re-decide.</summary>
type ReplaySuffix<'EntityId, 'State, 'Event, 'Action> =
    {
        /// <summary>
        /// Every committed transition effective at or after the corrected instant, ordered by
        /// effective time and then epoch.
        /// </summary>
        Transitions: CommittedTransition<'EntityId, 'State, 'Event, 'Action> list
        /// <summary>
        /// Retention has purged transitions that may fall in that range, so the list cannot be
        /// trusted to be the whole story.
        /// </summary>
        Incomplete: bool
    }

/// <summary>What a replayed correction writes, all in one transaction.</summary>
type CorrectionCommit<'EntityId, 'State, 'Event, 'Action> =
    {
        /// <summary>
        /// The correction's own entry in the transition log: the missed event, from the entity's
        /// state before the correction to its state after it, with no actions.
        /// </summary>
        Transition: TransitionDraft<'EntityId, 'State, 'Event, 'Action>
        /// <summary>The instant the belief timeline is rewritten from.</summary>
        ValidFrom: DateTimeOffset
        /// <summary>
        /// The rewritten timeline, never empty: the correction's own assertion is always present.
        /// The core snapshot advances independently of this timeline and remains the epoch authority.
        /// </summary>
        Beliefs: CorrectedBelief<'State> * CorrectedBelief<'State> list
    }

/// <summary>
/// Reading a committed suffix and whether retention has made it incomplete.
/// This capability requires only a transition log and a purge watermark, not temporal tables.
/// </summary>
type ITransitionLog<'EntityId, 'State, 'Event, 'Action> =

    /// <summary>
    /// The committed transitions effective at or after <paramref name="from" />, at most
    /// <paramref name="limit" /> of them. Asking for one more than a budget allows is how a caller
    /// learns the budget would be exceeded.
    /// </summary>
    abstract SuffixAsync:
        machineId: MachineId *
        entityId: 'EntityId *
        from: DateTimeOffset *
        limit: int *
        cancellationToken: CancellationToken ->
            Task<Result<ReplaySuffix<'EntityId, 'State, 'Event, 'Action>, StoreError>>

/// <summary>The entity's state strictly before a business instant, independent of its representation.</summary>
type IPriorState<'EntityId, 'State> =
    /// <summary>Returns the state effective strictly before the supplied instant, or no prior state.</summary>
    abstract BeforeAsync:
        machineId: MachineId * entityId: 'EntityId * at: DateTimeOffset * cancellationToken: CancellationToken ->
            Task<Result<Snapshot<'State> option, StoreError>>

/// <summary>Atomic, fenced correction commits, independent of the representation of history.</summary>
type ICorrectionCommitter<'EntityId, 'State, 'Event, 'Action> =
    /// <summary>
    /// Commits a correction, fenced like any other commit: the lease must still be held and the
    /// entity must still be at <paramref name="expected" />, or nothing is written.
    /// </summary>
    abstract CommitCorrectionAsync:
        commandId: CommandId *
        token: LeaseToken<CommandWork> *
        expected: Epoch *
        commit: CorrectionCommit<'EntityId, 'State, 'Event, 'Action> *
        cancellationToken: CancellationToken ->
            Task<Result<FinalizeOutcome, StoreError>>

/// <summary>The complete set of operations required to plan and commit a correction.</summary>
type CorrectionCapability<'EntityId, 'State, 'Event, 'Action> =
    {
        /// <summary>The committed suffix and its retention watermark.</summary>
        Log: ITransitionLog<'EntityId, 'State, 'Event, 'Action>
        /// <summary>The state immediately preceding the correction.</summary>
        Prior: IPriorState<'EntityId, 'State>
        /// <summary>The atomic correction writer.</summary>
        Committer: ICorrectionCommitter<'EntityId, 'State, 'Event, 'Action>
    }
