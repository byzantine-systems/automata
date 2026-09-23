namespace ByzantineSystems.Automata.Storage

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core

/// <summary>
/// One side effect a committed transition asked for, as the queue holds it.
///
/// Identity is <c>(CommandId, Ordinal)</c> rather than the queue's own message id. A redelivery
/// carries a new message id but the same pair, so a destination that deduplicates on it stays
/// correct across a crash between the external call and the acknowledgement.
/// </summary>
type ActionRecord<'EntityId, 'Action> =
    {
        MachineId: MachineId
        EntityId: 'EntityId
        /// <summary>The command whose commit emitted this action.</summary>
        CommandId: CommandId
        /// <summary>The epoch that commit wrote, so a handler can order effects against history.</summary>
        Epoch: Epoch
        /// <summary>Position within that transition's action list, from zero.</summary>
        Ordinal: int
        Action: 'Action
    }

/// <summary>An action claimed for delivery.</summary>
type LeasedAction<'EntityId, 'Action> = Leased<ActionWork, ActionRecord<'EntityId, 'Action>>

/// <summary>
/// Delivery of the actions a commit produced.
///
/// Enqueueing is not on this interface: an action is enqueued by
/// <see cref="M:ByzantineSystems.Automata.Storage.ICommandProcessorStore`5.Commit" />, inside
/// the same transaction as the state change, which is what makes "the transition happened and
/// its effects were queued" a single fact. This interface is only the consumer side.
///
/// Delivery is at-least-once. A worker that crashes after the external call but before
/// completing redelivers, so handlers must tolerate a repeat or pass
/// <c>(CommandId, Ordinal)</c> to the destination.
/// </summary>
type IActionQueue<'EntityId, 'Action> =

    /// <summary>
    /// Leases up to <paramref name="batch" /> deliverable actions. Unordered by design: the
    /// per-entity order that matters was decided when the commands were committed, and holding
    /// a slow destination's actions in that order would stall every other entity behind it.
    /// </summary>
    abstract Claim:
        machineId: MachineId * batch: int * lease: TimeSpan * ct: CancellationToken ->
            Task<Result<LeasedAction<'EntityId, 'Action> list, StoreError>>

    /// <summary>Marks an action delivered. Fenced: a lease that was taken over cannot complete it.</summary>
    abstract Complete:
        action: LeasedAction<'EntityId, 'Action> * ct: CancellationToken -> Task<Result<LeaseUpdateOutcome, StoreError>>

    /// <summary>Returns a failed delivery to the queue after a backoff. Fenced.</summary>
    abstract Reschedule:
        action: LeasedAction<'EntityId, 'Action> * backoff: Backoff * ct: CancellationToken ->
            Task<Result<LeaseUpdateOutcome, StoreError>>

    /// <summary>
    /// Gives up on a delivery, recording it durably before removing it from the queue. Fenced.
    /// The queue's own archive is operational forensics; the record this writes is the business
    /// fact that an effect the machine asked for never happened.
    /// </summary>
    abstract Abandon:
        action: LeasedAction<'EntityId, 'Action> * reason: string * ct: CancellationToken ->
            Task<Result<LeaseUpdateOutcome, StoreError>>
