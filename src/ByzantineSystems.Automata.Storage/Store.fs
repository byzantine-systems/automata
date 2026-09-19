namespace ByzantineSystems.Automata.Storage

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core

/// <summary>
/// Durable per-instance state, history, and receipts. <c>Commit</c> is the atomicity
/// boundary: the snapshot advance, the history append, and the action outbox inserts happen
/// in one transaction, or not at all. A duplicate idempotency key returns the original
/// receipt without advancing the epoch or emitting duplicate outbox rows. A stale
/// <c>expected</c> epoch returns <see cref="F:ByzantineSystems.Automata.Core.StoreError.Concurrency" />.
/// New instances start from the first commit's <c>FromState</c> at epoch 0.
/// </summary>
type IStateStore<'EntityId, 'State, 'Event, 'Action> =

    /// <summary>Returns the current snapshot, or <c>None</c> when the entity has never committed.</summary>
    abstract TryGet:
        machineId: MachineId * entityId: 'EntityId * ct: CancellationToken ->
            Task<Result<Snapshot<'State> option, StoreError>>

    /// <summary>
    /// Looks up the receipt of an already-committed idempotency key. An optimisation:
    /// <c>Commit</c> enforces uniqueness itself, even when two processes race on the same key.
    /// </summary>
    abstract FindReceipt:
        machineId: MachineId * entityId: 'EntityId * idempotencyKey: string * ct: CancellationToken ->
            Task<Result<CommitReceipt option, StoreError>>

    /// <summary>
    /// Applies one transition against the expected epoch. Implementations must: reject a
    /// stale epoch with <see cref="F:ByzantineSystems.Automata.Core.StoreError.Concurrency" />;
    /// return the original receipt when the idempotency key already exists; create the
    /// instance on first commit; and derive outbox rows from <c>Actions</c> with stable
    /// <c>&lt;event key&gt;#&lt;ordinal&gt;</c> action keys.
    /// </summary>
    abstract Commit:
        transition: Transition<'EntityId, 'State, 'Event, 'Action> * expected: Epoch * ct: CancellationToken ->
            Task<Result<CommitReceipt, StoreError>>

    /// <summary>Returns one ascending page of the entity's transition log.</summary>
    abstract History:
        machineId: MachineId * entityId: 'EntityId * paging: Page * ct: CancellationToken ->
            Task<Result<Transition<'EntityId, 'State, 'Event, 'Action> list, StoreError>>

/// <summary>
/// The durable, long-horizon retry queue. Claims are leased: an item claimed by one worker
/// is invisible to others until the lease expires, so any number of pump instances may poll
/// concurrently without double-processing. Durable attempts have their own maximum and
/// schedule, owned by the pump's policy, not by the queue.
/// </summary>
type IRetryQueue<'EntityId, 'Event> =

    /// <summary>
    /// Enqueues an event for later retry. Re-enqueueing an idempotency key that is still
    /// pending refreshes its schedule without duplicating the item.
    /// </summary>
    abstract Enqueue: request: RetryRequest<'EntityId, 'Event> * ct: CancellationToken -> Task<Result<unit, StoreError>>

    /// <summary>
    /// Leases up to <c>batch</c> due items (next attempt reached, lease free), ordered by next
    /// attempt time. Claiming increments the attempt count and sets the lease deadline.
    /// </summary>
    abstract Claim:
        batch: int * lease: TimeSpan * ct: CancellationToken ->
            Task<Result<RetryItem<'EntityId, 'Event> list, StoreError>>

    /// <summary>
    /// Removes a processed item. Completing an unknown id succeeds: completion is
    /// idempotent from a pump's point of view.
    /// </summary>
    abstract Complete: retryId: RetryId * ct: CancellationToken -> Task<Result<unit, StoreError>>

    /// <summary>
    /// Records a recoverable failure: the item is unlocked and rescheduled to the given time.
    /// Failing an unknown id succeeds, symmetrically with <c>Complete</c>.
    /// </summary>
    abstract Fail:
        retryId: RetryId * nextAttemptAt: DateTimeOffset * error: string * ct: CancellationToken ->
            Task<Result<unit, StoreError>>

/// <summary>Append-only recording of events the machine has permanently abandoned.</summary>
type IDeadLetterStore<'EntityId, 'Event> =

    /// <summary>Records one dead letter. Recording is append-only and never reprocesses.</summary>
    abstract Record: deadLetter: DeadLetter<'EntityId, 'Event> * ct: CancellationToken -> Task<Result<unit, StoreError>>

/// <summary>
/// The consumer side of the transactional action outbox written by
/// <see cref="M:ByzantineSystems.Automata.Storage.IStateStore`4.Commit" />. Delivery is
/// at-least-once: a worker that crashes after the external call but before completing may
/// redeliver the same action key, so effect handlers must pass the key to the destination
/// or otherwise tolerate duplicates.
/// </summary>
type IActionOutbox<'EntityId, 'Action> =

    /// <summary>
    /// Leases up to <c>batch</c> due items, ordered by next attempt time. Claiming increments
    /// the attempt count and sets the lease deadline.
    /// </summary>
    abstract Claim:
        batch: int * lease: TimeSpan * ct: CancellationToken ->
            Task<Result<OutboxItem<'EntityId, 'Action> list, StoreError>>

    /// <summary>Marks an action as delivered. Completing an unknown key succeeds.</summary>
    abstract Complete: actionKey: string * ct: CancellationToken -> Task<Result<unit, StoreError>>

    /// <summary>
    /// Records a recoverable delivery failure: the item is unlocked and rescheduled to the
    /// given time. Failing an unknown key succeeds, symmetrically with <c>Complete</c>.
    /// </summary>
    abstract Fail:
        actionKey: string * nextAttemptAt: DateTimeOffset * ct: CancellationToken -> Task<Result<unit, StoreError>>
