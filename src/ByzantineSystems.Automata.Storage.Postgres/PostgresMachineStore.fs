namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open FsToolkit.ErrorHandling
open Npgsql

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
        /// <summary>Delivery registered at boot, or explicit Discard.</summary>
        ActionDelivery: ActionDelivery
        StateCodec: Codec<'State>
        EventCodec: Codec<'Event>
        ActionCodec: Codec<'Action>
        ErrorCodec: Codec<'Err>
        EntityIdEncode: 'EntityId -> string
        /// <summary>Decoding returns a result, so a corrupt row is a typed failure rather than an exception.</summary>
        EntityIdDecode: string -> Result<'EntityId, string>
        /// <summary>
        /// How long this machine's history is kept. Written to the database on boot, so the
        /// database can apply it without the application present. Nothing is deleted unless this
        /// says so: <c>RetentionPolicy.keepEverything</c> keeps it all.
        /// </summary>
        Retention: RetentionPolicy
        /// <summary>
        /// How long past its expiry a lease is left alone before maintenance hands it back to
        /// ready. It must exceed the processor's lease renewal margin, because a lease that
        /// expired and was not reclaimed may still be extended by its holder, and reaping takes
        /// that away. Positive and provider-safe by construction; use minutes, not seconds.
        /// </summary>
        ReapAfter: LeaseDuration
        /// <summary>
        /// Where this machine listens for work announced by other processes.
        /// <c>SameDataSource</c> unless the data source goes through a transaction-mode pooler,
        /// where <c>LISTEN</c> does not survive.
        /// </summary>
        Listener: ListenerConnection
    }

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Storage.Postgres.MachineStoreOptions`5" />.</summary>
[<RequireQualifiedAccess>]
module MachineStoreOptions =

    /// <summary>
    /// Every default a host would otherwise write out: JSON codecs for state, events, actions and
    /// errors, <c>EntityId</c> keys, keeping everything, reaping leases five minutes after they
    /// expire, and listening on the store's own data source. Override any of it with
    /// <c>{ … with }</c>; nothing here is hidden behind a builder.
    /// </summary>
    /// <example>
    /// <code lang="fsharp">
    /// let store =
    ///     MachineStoreOptions.forEntityId&lt;Payment, PaymentState, PaymentEvent, PaymentAction, PaymentError&gt;
    ///         context (ActionDelivery.Deliver(ActionQueueName.create "payment_actions"))
    ///     |> PostgresMachineStore
    /// </code>
    /// </example>
    let forEntityId<'Tag, 'State, 'Event, 'Action, 'Err>
        (context: PostgresContext)
        (actionDelivery: ActionDelivery)
        : MachineStoreOptions<EntityId<'Tag>, 'State, 'Event, 'Action, 'Err> =
        let encode, decode = EntityKey.forEntityId<'Tag>

        { Context = context
          ActionDelivery = actionDelivery
          StateCodec = Serialization.systemTextJson<'State> ()
          EventCodec = Serialization.systemTextJson<'Event> ()
          ActionCodec = Serialization.systemTextJson<'Action> ()
          ErrorCodec = Serialization.systemTextJson<'Err> ()
          EntityIdEncode = encode
          EntityIdDecode = decode
          Retention = RetentionPolicy.keepEverything
          ReapAfter = LeaseDuration.create (TimeSpan.FromMinutes 5.)
          Listener = ListenerConnection.SameDataSource }

/// <summary>
/// The complete PostgreSQL implementation of
/// <see cref="T:ByzantineSystems.Automata.Storage.IMachineStore`5" />.
///
/// It composes the four implementations rather than reimplementing them, because each is
/// separately useful: a host that only submits commands needs the inbox and nothing else, and
/// the inbox alone requires no extension at all.
///
/// The interface implementations are explicit, which F# requires, and that is a feature here.
/// <c>ICommandInbox</c> and <c>IActionQueue</c> both declare <c>ClaimAsync</c> and <c>RescheduleAsync</c>
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

    let queueName, actions =
        match options.ActionDelivery with
        | ActionDelivery.Discard -> "", None
        | ActionDelivery.Deliver name ->
            let queue = ActionQueueName.value name

            queue,
            Some(
                PostgresActionQueue<'EntityId, 'Action>(
                    { Context = options.Context
                      Queue = queue
                      ActionCodec = options.ActionCodec
                      EntityIdEncode = options.EntityIdEncode
                      EntityIdDecode = options.EntityIdDecode }
                )
            )

    let ensureQueue ct =
        match actions with
        | Some actions -> actions.EnsureQueueAsync ct
        | None when ct.IsCancellationRequested -> Task.FromCanceled<Result<bool, StoreError>> ct
        | None -> Task.FromResult(Ok false)

    let queueOwnerDefect =
        function
        | StoreError.Unexpected(:? PostgresException as error) when error.ConstraintName = "automata_action_queue_owner" ->
            Some(BootDefect.Misconfigured error.MessageText)
        | _ -> None

    let temporalStore =
        PostgresTemporalStore<'EntityId, 'State>(
            { Context = options.Context
              TimeProvider = TimeProvider.System
              StateCodec = options.StateCodec
              EntityIdEncode = options.EntityIdEncode
              EntityIdDecode = options.EntityIdDecode }
        )

    let notifications =
        PostgresWorkNotifications(options.Context, queueName, options.Listener) :> IWorkNotifications

    let actionQueue =
        match actions with
        | Some actions -> actions :> IActionQueue<'EntityId, 'Action>
        | None -> ActionQueues.discard ()

    let reader = processor :> IStateReader<'EntityId, 'State, 'Event, 'Action>
    let temporal = temporalStore :> ITemporalReader<'EntityId, 'State>
    let corrections = temporalStore :> ICorrectionStore<'EntityId, 'State>

    let processorStore =
        processor :> ICommandProcessorStore<'EntityId, 'State, 'Event, 'Action, 'Err>

    /// Every precondition is read before anything is written, so a refused boot leaves the
    /// database as it found it; the two writes after it are idempotent, which is what makes
    /// booting every generation of a supervised machine safe.
    let boot machineId ct =
        backgroundTask {
            match! Boot.inspect options.Context ct with
            | Error error -> return Error error
            | Ok [] ->
                match! ensureQueue ct with
                | Error error ->
                    match queueOwnerDefect error with
                    | Some defect -> return Ok(BootReport.Refused [ defect ])
                    | None -> return Error error
                | Ok _ ->
                    match! Boot.register options.Context machineId queueName options.Retention options.ReapAfter ct with
                    | Ok() -> return Ok BootReport.Ready
                    | Error error -> return Error error
            | Ok defects -> return Ok(BootReport.Refused defects)
        }

    /// <summary>
    /// Creates this machine's action queue if it does not exist yet. Call once at startup,
    /// before any command can commit: a commit enqueues into this queue inside its own
    /// transaction, and a missing queue would fail the commit rather than only the delivery.
    /// </summary>
    member _.EnsureQueueAsync(cancellationToken: CancellationToken) : Task<Result<bool, StoreError>> =
        Tap.start cancellationToken (fun () -> ensureQueue cancellationToken)

    interface ICommandInbox<'EntityId, 'Event> with

        member _.SubmitAsync(submission, cancellationToken) =
            inbox.SubmitAsync(submission, cancellationToken)

        member _.ClaimAsync(machineId, batch, lease, cancellationToken) =
            inbox.ClaimAsync(machineId, batch, lease, cancellationToken)

        member _.RescheduleAsync(commandId, token, backoff, cancellationToken) =
            inbox.RescheduleAsync(commandId, token, backoff, cancellationToken)

        member _.ExtendLeaseAsync(commandId, token, lease, cancellationToken) =
            inbox.ExtendLeaseAsync(commandId, token, lease, cancellationToken)

        member _.TryGetAsync(commandId, cancellationToken) =
            inbox.TryGetAsync(commandId, cancellationToken)

        member _.TryFindAsync(machineId, entityId, key, cancellationToken) =
            inbox.TryFindAsync(machineId, entityId, key, cancellationToken)

    interface IStateReader<'EntityId, 'State, 'Event, 'Action> with

        member _.TryGetSnapshotAsync(machineId, entityId, cancellationToken) =
            reader.TryGetSnapshotAsync(machineId, entityId, cancellationToken)

        member _.HistoryAsync(machineId, entityId, paging, cancellationToken) =
            reader.HistoryAsync(machineId, entityId, paging, cancellationToken)

    interface ICommandProcessorStore<'EntityId, 'State, 'Event, 'Action, 'Err> with

        member _.CommitAsync(commandId, token, expected, draft, cancellationToken) =
            processorStore.CommitAsync(commandId, token, expected, draft, cancellationToken)

        member _.RejectAsync(commandId, token, failure, cancellationToken) =
            processorStore.RejectAsync(commandId, token, failure, cancellationToken)

        member _.DeadLetterAsync(commandId, token, failure, cancellationToken) =
            processorStore.DeadLetterAsync(commandId, token, failure, cancellationToken)

        member _.TryGetResultAsync(commandId, cancellationToken) =
            processorStore.TryGetResultAsync(commandId, cancellationToken)

    interface IActionQueue<'EntityId, 'Action> with

        member _.ClaimAsync(machineId, batch, lease, cancellationToken) =
            actionQueue.ClaimAsync(machineId, batch, lease, cancellationToken)

        member _.CompleteAsync(action, cancellationToken) =
            actionQueue.CompleteAsync(action, cancellationToken)

        member _.RescheduleAsync(action, backoff, cancellationToken) =
            actionQueue.RescheduleAsync(action, backoff, cancellationToken)

        member _.AbandonAsync(action, reason, cancellationToken) =
            actionQueue.AbandonAsync(action, reason, cancellationToken)

    // The two optional capabilities. Declaring them is what makes Store.tryTemporal and
    // Store.tryCorrections answer Some for this store, and a provider that omits them is still a
    // complete provider.
    interface ITemporalReader<'EntityId, 'State> with

        member _.ValidAtAsync(machineId, entityId, validAt, cancellationToken) =
            temporal.ValidAtAsync(machineId, entityId, validAt, cancellationToken)

        member _.AsOfAsync(machineId, entityId, validAt, knownAt, cancellationToken) =
            temporal.AsOfAsync(machineId, entityId, validAt, knownAt, cancellationToken)

    interface ICorrectionStore<'EntityId, 'State> with

        member _.CorrectAsync(machineId, entityId, validFrom, beliefs, cancellationToken) =
            corrections.CorrectAsync(machineId, entityId, validFrom, beliefs, cancellationToken)

    /// The boot check. Delivery names are validated before the options can be constructed.
    interface IStoreBoot with

        member _.BootAsync(machineId, cancellationToken) =
            Tap.requireMachineId (nameof machineId) machineId

            Tap.start cancellationToken (fun () -> boot machineId cancellationToken)

    interface IWorkNotifications with

        member _.ListenAsync(machineId, onCommands, onActions, cancellationToken) =
            Tap.requireMachineId (nameof machineId) machineId
            Tap.requireArg (nameof onCommands) onCommands
            Tap.requireArg (nameof onActions) onActions

            Tap.start cancellationToken (fun () ->
                notifications.ListenAsync(machineId, onCommands, onActions, cancellationToken))

    interface IReplayStore<'EntityId, 'State, 'Event, 'Action> with

        member _.SuffixAsync(machineId, entityId, from, limit, cancellationToken) =
            (processor :> IReplayStore<'EntityId, 'State, 'Event, 'Action>)
                .SuffixAsync(machineId, entityId, from, limit, cancellationToken)

        member _.CommitCorrectionAsync(commandId, token, expected, commit, cancellationToken) =
            (processor :> IReplayStore<'EntityId, 'State, 'Event, 'Action>)
                .CommitCorrectionAsync(commandId, token, expected, commit, cancellationToken)

    interface IMachineStore<'EntityId, 'State, 'Event, 'Action, 'Err>
