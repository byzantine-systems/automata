namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Internal
open FsToolkit.ErrorHandling

/// <summary>
/// What the action queue needs to bridge one machine's actions to a pgmq queue.
/// </summary>
type ActionQueueOptions<'EntityId, 'Action> =
    {
        Context: PostgresContext
        /// <summary>
        /// The queue this machine's actions are delivered through, one per machine. Sharing a
        /// queue between machines would need a predicate on the claim, and pgmq has no way to
        /// express one without scanning the queue.
        /// </summary>
        Queue: string
        ActionCodec: Codec<'Action>
        EntityIdEncode: 'EntityId -> string
        /// <summary>Decoding returns a result, so a corrupt message is a typed failure rather than an exception.</summary>
        EntityIdDecode: string -> Result<'EntityId, string>
    }

/// <summary>
/// The PostgreSQL end of action delivery.
///
/// pgmq supplies the delivery semantics and none of the fencing, so every write here goes
/// through an <c>fsm.*</c> routine that compares <c>read_ct</c> first. Nothing in this type
/// calls <c>pgmq.archive</c> or <c>pgmq.set_vt</c>, and that is deliberate: those two ignore
/// <c>read_ct</c>, which is the one thing that distinguishes the lease holder from a worker
/// that stalled past its visibility timeout.
/// </summary>
type PostgresActionQueue<'EntityId, 'Action>(options: ActionQueueOptions<'EntityId, 'Action>) =

    let context = options.Context

    let ensureQueue = Statement.load "action" "ensure_queue"
    let claimActions = Statement.load "action" "claim"
    let completeAction = Statement.load "action" "complete"
    let rescheduleAction = Statement.load "action" "reschedule"
    let abandonAction = Statement.load "action" "abandon"

    let queue = [ "queue", Param.Text options.Queue ]

    let outcomeFromString (value: string) : Result<LeaseUpdateOutcome, StoreError> =
        match value with
        | "updated" -> Ok Updated
        | "lease_lost" -> Ok LeaseUpdateOutcome.LeaseLost
        | other -> Error(Db.decodeFailure (nameof LeaseUpdateOutcome) $"unknown lease outcome {other}")

    /// The message body finalize_command wrote. Every field is required: a message missing one
    /// was not written by this library's finalize, and guessing what it meant would be worse
    /// than refusing it.
    let readMessage (json: string) : Result<ActionRecord<'EntityId, 'Action>, StoreError> =
        use document = JsonDocument.Parse json
        let root = document.RootElement

        let field (name: string) : Result<JsonElement, StoreError> =
            match root.TryGetProperty name with
            | true, value -> Ok value
            | _ -> Error(Db.decodeFailure (nameof ActionRecord) $"an action message carried no {name}")

        let text (name: string) : Result<string, StoreError> =
            field name
            |> Result.bind (fun value ->
                match value.ValueKind with
                | JsonValueKind.String -> Ok(value.GetString())
                | _ -> Error(Db.decodeFailure (nameof ActionRecord) $"{name} was not a string"))

        let number (name: string) : Result<int64, StoreError> =
            field name
            |> Result.bind (fun value ->
                match value.ValueKind with
                | JsonValueKind.Number -> Ok(value.GetInt64())
                | _ -> Error(Db.decodeFailure (nameof ActionRecord) $"{name} was not a number"))

        let entityId =
            text "entity_id"
            |> Result.bind (options.EntityIdDecode >> Result.mapError (Db.decodeFailure "EntityId"))

        let action =
            field "action"
            |> Result.bind (fun value ->
                value.GetRawText()
                |> options.ActionCodec.Decode
                |> Result.mapError Db.toStoreError)

        match text "machine_id", entityId, number "command_id", number "epoch", number "ordinal", action with
        | Ok machineId, Ok entityId, Ok commandId, Ok epoch, Ok ordinal, Ok action ->
            Ok
                { MachineId = MachineId.create machineId
                  EntityId = entityId
                  CommandId = CommandId.ofInt64 commandId
                  Epoch = Epoch.ofUInt64 (uint64 epoch)
                  Ordinal = int ordinal
                  Action = action }
        | Error error, _, _, _, _, _
        | _, Error error, _, _, _, _
        | _, _, Error error, _, _, _
        | _, _, _, Error error, _, _
        | _, _, _, _, Error error, _
        | _, _, _, _, _, Error error -> Error error

    /// <summary>
    /// The two halves of a pgmq claim, both carried on the lease.
    ///
    /// <c>msg_id</c> says which message and <c>read_ct</c> says which claim of it. pgmq's own
    /// archive and set_vt take only the first, which is precisely why they cannot fence: they
    /// cannot tell this worker's claim from the one that replaced it after a stall.
    /// </summary>
    let messageIdOf (action: LeasedAction<'EntityId, 'Action>) = LeaseToken.value action.Token

    /// The message and the claim that fences it, as every fenced routine binds them.
    let fence (action: LeasedAction<'EntityId, 'Action>) =
        queue
        @ [ "msg_id", Param.Bigint(messageIdOf action)
            "read_ct", Param.Int action.DeliveryCount ]

    /// One fenced call: the routine answers whether the caller still held the lease.
    let fenced (statement: Statement) (values: (string * Param) list) (ct: CancellationToken) =
        Sql.run
            context
            (postgres {
                let! outcome =
                    Sql.one (nameof LeaseUpdateOutcome) statement values (fun reader -> Row.string reader "outcome")

                return! outcomeFromString outcome
            })
            ct

    /// <summary>
    /// Creates this machine's queue if it does not exist yet. Call once at startup: it builds
    /// tables and indexes, which is not a question a hot path should be asking.
    /// </summary>
    member _.EnsureQueueAsync(ct: CancellationToken) : Task<Result<bool, StoreError>> =
        Sql.run context (Sql.one "ActionQueue" ensureQueue queue (fun reader -> Row.bool reader "created")) ct

    interface IActionQueue<'EntityId, 'Action> with

        member _.Claim(_, batch, lease, ct) =
            if batch < 1 then
                invalidArg (nameof batch) "A claim batch size must be positive."

            if lease <= TimeSpan.Zero then
                invalidArg (nameof lease) "A lease must be a positive duration."

            // Not idempotent, like the command claim: a claim whose reply was lost has already
            // leased the messages, and repeating it leases more while the first batch stays
            // invisible. The dispatcher polls again shortly, which recovers faster.
            let claim =
                Sql.rows
                    claimActions
                    (queue @ [ "batch", Param.Int batch; "lease", Param.Interval lease ])
                    (fun reader ->
                        Row.string reader "message",
                        Row.int64 reader "msg_id",
                        Row.int32 reader "read_ct",
                        Row.timestamp reader "vt")

            backgroundTaskResult {
                let! claimed = Sql.runOnce context claim ct

                // Decoded once the claim has returned. The first message that does not decode
                // fails the claim, as before; its lease lapses and it is redelivered.
                return!
                    claimed
                    |> List.traverseResultM (fun (message, messageId, deliveries, visibleUntil) ->
                        readMessage message
                        |> Result.map (fun record ->
                            { Work = record
                              // The message's id identifies the claim; the delivery count fences
                              // it. Every write below needs both.
                              Token = LeaseToken.ofInt64 messageId
                              DeliveryCount = deliveries
                              ExpiresAt = visibleUntil }))
            }

        member _.Complete(action, ct) = fenced completeAction (fence action) ct

        member _.Reschedule(action, backoff, ct) =
            fenced
                rescheduleAction
                (fence action
                 @ [ "base_ms", Param.Int(int (Backoff.baseDelay backoff).TotalMilliseconds)
                     "cap_ms", Param.Int(int (Backoff.ceiling backoff).TotalMilliseconds) ])
                ct

        member _.Abandon(action, reason, ct) =
            fenced abandonAction (fence action @ [ "reason", Param.Text reason ]) ct
