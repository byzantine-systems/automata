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
    /// Today's opinion about an instant of business time. Equivalent to <c>AsOf</c> with
    /// <c>knownAt</c> set to now, and offered separately because that is the question most
    /// callers actually have.
    /// </summary>
    abstract ValidAt:
        machineId: MachineId * entityId: 'EntityId * validAt: DateTimeOffset * ct: CancellationToken ->
            Task<Result<Belief<'EntityId, 'State> option, StoreError>>

    /// <summary>
    /// The opinion held at <paramref name="knownAt" />, about <paramref name="validAt" />.
    ///
    /// Holding one still and moving the other is how a change of mind becomes visible as a change
    /// of mind: the same instant in the world, two different answers, and the difference is the
    /// explanation an operator is asking for.
    /// </summary>
    abstract AsOf:
        machineId: MachineId *
        entityId: 'EntityId *
        validAt: DateTimeOffset *
        knownAt: DateTimeOffset *
        ct: CancellationToken ->
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
    /// answerable through <c>AsOf</c>. That is the whole point: a correction that erased what it
    /// replaced would leave an audit record claiming the new answer had always been the answer.
    ///
    /// The beliefs must be ordered by <c>ValidFrom</c>, all at or after
    /// <paramref name="validFrom" />. Each one runs until the next begins and the last is left
    /// open, so a gap or an overlap in the corrected timeline is not expressible.
    ///
    /// An empty list asserts that the entity had no belief from <paramref name="validFrom" />
    /// onward, which is how a correction undoes one rather than replacing it.
    /// </summary>
    abstract Correct:
        machineId: MachineId *
        entityId: 'EntityId *
        validFrom: DateTimeOffset *
        beliefs: CorrectedBelief<'State> list *
        ct: CancellationToken ->
            Task<Result<CorrectionOutcome, StoreError>>
