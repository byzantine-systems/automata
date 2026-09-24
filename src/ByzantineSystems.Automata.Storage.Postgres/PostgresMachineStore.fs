namespace ByzantineSystems.Automata.Storage.Postgres

open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage

/// <summary>
/// Everything one machine's PostgreSQL store needs, in one place.
///
/// The codecs and the entity-key projections are shared across all four capabilities, so
/// declaring them once is not only convenience: two capabilities encoding an entity id
/// differently would write rows that cannot find each other.
/// </summary>
type MachineStoreOptions<'EntityId, 'State, 'Event, 'Action, 'Err> =
    {
        Context: PostgresContext
        /// <summary>The queue this machine's actions are delivered through, one per machine.</summary>
        ActionQueue: string
        StateCodec: Codec<'State>
        EventCodec: Codec<'Event>
        ActionCodec: Codec<'Action>
        ErrorCodec: Codec<'Err>
        EntityIdEncode: 'EntityId -> string
        /// <summary>Decoding returns a result, so a corrupt row is a typed failure rather than an exception.</summary>
        EntityIdDecode: string -> Result<'EntityId, string>
    }

/// <summary>
/// The complete PostgreSQL implementation of
/// <see cref="T:ByzantineSystems.Automata.Storage.IMachineStore`5" />.
///
/// It composes the four implementations rather than reimplementing them, because each is
/// separately useful: a host that only submits commands needs the inbox and nothing else, and
/// the inbox alone requires no extension at all.
///
/// The interface implementations are explicit, which F# requires, and that is a feature here.
/// <c>ICommandInbox</c> and <c>IActionQueue</c> both declare <c>Claim</c> and <c>Reschedule</c>
/// with the same shape, so a caller has to say which capability it means, and a line that says
/// so cannot mean the other one by accident.
/// </summary>
type PostgresMachineStore<'EntityId, 'State, 'Event, 'Action, 'Err>
    (options: MachineStoreOptions<'EntityId, 'State, 'Event, 'Action, 'Err>) =

    let inbox =
        PostgresCommandInbox<'EntityId, 'Event>(
            { Context = options.Context
              EventCodec = options.EventCodec
              EntityIdEncode = options.EntityIdEncode
              EntityIdDecode = options.EntityIdDecode }
        )
        :> ICommandInbox<'EntityId, 'Event>

    let processor =
        PostgresCommandProcessorStore<'EntityId, 'State, 'Event, 'Action, 'Err>(
            { Context = options.Context
              ActionQueue = options.ActionQueue
              StateCodec = options.StateCodec
              EventCodec = options.EventCodec
              // The transition row stores the whole action list as one document, while the
              // queue carries them one at a time. Both codecs are derived from the caller's
              // single action codec so the two encodings cannot drift.
              ActionCodec = Serialization.listOf options.ActionCodec
              ErrorCodec = options.ErrorCodec
              EntityIdEncode = options.EntityIdEncode
              EntityIdDecode = options.EntityIdDecode }
        )

    let actions =
        PostgresActionQueue<'EntityId, 'Action>(
            { Context = options.Context
              Queue = options.ActionQueue
              ActionCodec = options.ActionCodec
              EntityIdEncode = options.EntityIdEncode
              EntityIdDecode = options.EntityIdDecode }
        )

    let temporalStore =
        PostgresTemporalStore<'EntityId, 'State>(
            { Context = options.Context
              StateCodec = options.StateCodec
              EntityIdEncode = options.EntityIdEncode
              EntityIdDecode = options.EntityIdDecode }
        )

    let actionQueue = actions :> IActionQueue<'EntityId, 'Action>
    let reader = processor :> IStateReader<'EntityId, 'State, 'Event, 'Action>
    let temporal = temporalStore :> ITemporalReader<'EntityId, 'State>
    let corrections = temporalStore :> ICorrectionStore<'EntityId, 'State>

    let processorStore =
        processor :> ICommandProcessorStore<'EntityId, 'State, 'Event, 'Action, 'Err>

    /// <summary>
    /// Creates this machine's action queue if it does not exist yet. Call once at startup,
    /// before any command can commit: a commit enqueues into this queue inside its own
    /// transaction, and a missing queue would fail the commit rather than only the delivery.
    /// </summary>
    member _.EnsureQueueAsync(ct: CancellationToken) : Task<Result<bool, StoreError>> = actions.EnsureQueueAsync ct

    interface ICommandInbox<'EntityId, 'Event> with

        member _.Submit(submission, ct) = inbox.Submit(submission, ct)

        member _.Claim(machineId, batch, lease, ct) =
            inbox.Claim(machineId, batch, lease, ct)

        member _.Reschedule(commandId, token, backoff, ct) =
            inbox.Reschedule(commandId, token, backoff, ct)

        member _.ExtendLease(commandId, token, lease, ct) =
            inbox.ExtendLease(commandId, token, lease, ct)

        member _.TryGet(commandId, ct) = inbox.TryGet(commandId, ct)

        member _.TryFind(machineId, entityId, key, ct) =
            inbox.TryFind(machineId, entityId, key, ct)

    interface IStateReader<'EntityId, 'State, 'Event, 'Action> with

        member _.TryGetSnapshot(machineId, entityId, ct) =
            reader.TryGetSnapshot(machineId, entityId, ct)

        member _.History(machineId, entityId, paging, ct) =
            reader.History(machineId, entityId, paging, ct)

    interface ICommandProcessorStore<'EntityId, 'State, 'Event, 'Action, 'Err> with

        member _.Commit(commandId, token, expected, draft, ct) =
            processorStore.Commit(commandId, token, expected, draft, ct)

        member _.Reject(commandId, token, failure, ct) =
            processorStore.Reject(commandId, token, failure, ct)

        member _.DeadLetter(commandId, token, failure, ct) =
            processorStore.DeadLetter(commandId, token, failure, ct)

        member _.TryGetResult(commandId, ct) =
            processorStore.TryGetResult(commandId, ct)

    interface IActionQueue<'EntityId, 'Action> with

        member _.Claim(machineId, batch, lease, ct) =
            actionQueue.Claim(machineId, batch, lease, ct)

        member _.Complete(action, ct) = actionQueue.Complete(action, ct)

        member _.Reschedule(action, backoff, ct) =
            actionQueue.Reschedule(action, backoff, ct)

        member _.Abandon(action, reason, ct) = actionQueue.Abandon(action, reason, ct)

    // The two optional capabilities. Declaring them is what makes Store.tryTemporal and
    // Store.tryCorrections answer Some for this store, and a provider that omits them is still a
    // complete provider.
    interface ITemporalReader<'EntityId, 'State> with

        member _.ValidAt(machineId, entityId, validAt, ct) =
            temporal.ValidAt(machineId, entityId, validAt, ct)

        member _.AsOf(machineId, entityId, validAt, knownAt, ct) =
            temporal.AsOf(machineId, entityId, validAt, knownAt, ct)

    interface ICorrectionStore<'EntityId, 'State> with

        member _.Correct(machineId, entityId, validFrom, beliefs, ct) =
            corrections.Correct(machineId, entityId, validFrom, beliefs, ct)

    interface IMachineStore<'EntityId, 'State, 'Event, 'Action, 'Err>
