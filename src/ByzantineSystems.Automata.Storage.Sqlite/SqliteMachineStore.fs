namespace ByzantineSystems.Automata.Storage.Sqlite

open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open FsToolkit.ErrorHandling

/// <summary>
/// Everything one machine's SQLite store needs, in one place.
///
/// The codecs and the entity-key projections are shared across all four capabilities, so
/// declaring them once is not only convenience: two capabilities encoding an entity id
/// differently would write rows that cannot find each other.
///
/// There is no queue name, retention or listener here. Actions are rows keyed by machine, so
/// there is nothing to name or create; retention and notifications are capabilities this store
/// does not offer yet.
/// </summary>
type MachineStoreOptions<'EntityId, 'State, 'Event, 'Action, 'Err> =
    {
        Context: SqliteContext
        StateCodec: Codec<'State>
        EventCodec: Codec<'Event>
        ActionCodec: Codec<'Action>
        ErrorCodec: Codec<'Err>
        EntityIdEncode: 'EntityId -> string
        /// <summary>Decoding returns a result, so a corrupt row is a typed failure rather than an exception.</summary>
        EntityIdDecode: string -> Result<'EntityId, string>
    }

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Storage.Sqlite.MachineStoreOptions`5" />.</summary>
[<RequireQualifiedAccess>]
module MachineStoreOptions =

    /// <summary>
    /// Every default a host would otherwise write out: JSON codecs for state, events, actions and
    /// errors, and <c>EntityId</c> keys. Override any of it with <c>{ … with }</c>.
    /// </summary>
    /// <example>
    /// <code lang="fsharp">
    /// let store =
    ///     MachineStoreOptions.forEntityId&lt;Payment, PaymentState, PaymentEvent, PaymentAction, PaymentError&gt;
    ///         (SqliteContext.ofPath "payments.db")
    ///     |> SqliteMachineStore
    /// </code>
    /// </example>
    let forEntityId<'Tag, 'State, 'Event, 'Action, 'Err>
        (context: SqliteContext)
        : MachineStoreOptions<EntityId<'Tag>, 'State, 'Event, 'Action, 'Err> =
        let encode, decode = EntityKey.forEntityId<'Tag>

        { Context = context
          StateCodec = Serialization.systemTextJson<'State> ()
          EventCodec = Serialization.systemTextJson<'Event> ()
          ActionCodec = Serialization.systemTextJson<'Action> ()
          ErrorCodec = Serialization.systemTextJson<'Err> ()
          EntityIdEncode = encode
          EntityIdDecode = decode }

/// <summary>
/// The complete SQLite implementation of
/// <see cref="T:ByzantineSystems.Automata.Storage.IMachineStore`5" />: an embedded, single-file
/// store for one process or a few sharing a file.
///
/// It composes the four implementations rather than reimplementing them, as the PostgreSQL store
/// does. The interface implementations are explicit, so a caller has to say whether it means the
/// inbox's <c>Claim</c> or the action queue's.
///
/// The optional capabilities it leaves out are absent rather than stubbed, which is what the
/// runtime's discovery by type test expects. There is no temporal reader, correction store or
/// replay: the store keeps the current state rather than a bitemporal history, so a correction
/// command is dead-lettered and its entity released. There are no work notifications: workers
/// poll, which is the designed fallback. There is no maintenance: a claim already reclaims
/// expired leases.
/// </summary>
type SqliteMachineStore<'EntityId, 'State, 'Event, 'Action, 'Err>
    (options: MachineStoreOptions<'EntityId, 'State, 'Event, 'Action, 'Err>) =

    let inbox =
        SqliteCommandInbox<'EntityId, 'Event>(
            { Context = options.Context
              EventCodec = options.EventCodec
              EntityIdEncode = options.EntityIdEncode
              EntityIdDecode = options.EntityIdDecode }
        )
        :> ICommandInbox<'EntityId, 'Event>

    let processor =
        SqliteCommandProcessorStore<'EntityId, 'State, 'Event, 'Action, 'Err>(
            { Context = options.Context
              StateCodec = options.StateCodec
              EventCodec = options.EventCodec
              // The transition row stores the whole action list as one document, while the
              // queue carries them one at a time. Both encodings derive from the caller's single
              // action codec so they cannot drift.
              ActionCodec = Serialization.listOf options.ActionCodec
              ErrorCodec = options.ErrorCodec
              EntityIdEncode = options.EntityIdEncode
              EntityIdDecode = options.EntityIdDecode }
        )

    let actionQueue =
        SqliteActionQueue<'EntityId, 'Action>(
            { Context = options.Context
              ActionCodec = options.ActionCodec
              EntityIdEncode = options.EntityIdEncode
              EntityIdDecode = options.EntityIdDecode }
        )
        :> IActionQueue<'EntityId, 'Action>

    let reader = processor :> IStateReader<'EntityId, 'State, 'Event, 'Action>

    let processorStore =
        processor :> ICommandProcessorStore<'EntityId, 'State, 'Event, 'Action, 'Err>

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

    /// <summary>
    /// The boot check. It only reads: there is no queue to create and no maintenance to register,
    /// so a file that can serve needs nothing written before the first command.
    /// </summary>
    interface IStoreBoot with

        member _.Boot(_, ct) =
            Boot.inspect options.Context ct
            |> TaskResult.map (function
                | [] -> BootReport.Ready
                | defects -> BootReport.Refused defects)

    interface IMachineStore<'EntityId, 'State, 'Event, 'Action, 'Err>
