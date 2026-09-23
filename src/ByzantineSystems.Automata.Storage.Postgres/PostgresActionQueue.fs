namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open Npgsql

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

    let dataSource = options.Context.DataSource
    let protect work ct = Db.protect options.Context.Resilience work ct

    let addText (name: string) (value: string) (cmd: NpgsqlCommand) =
        cmd.Parameters.AddWithValue(name, value) |> ignore

    let addInt (name: string) (value: int) (cmd: NpgsqlCommand) =
        cmd.Parameters.AddWithValue(name, value) |> ignore

    let addBigint (name: string) (value: int64) (cmd: NpgsqlCommand) =
        cmd.Parameters.AddWithValue(name, value) |> ignore

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
                value.GetRawText() |> options.ActionCodec.Decode |> Result.mapError Db.toStoreError)

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

    /// One fenced call, for the two writes that differ only in which routine they name.
    let fencedWrite (operation: string) (bind: NpgsqlCommand -> unit) (ct: CancellationToken) =
        protect
            (fun token ->
                task {
                    use! conn = dataSource.OpenConnectionAsync(token).AsTask()
                    use cmd = new NpgsqlCommand(SqlResources.get "action" operation, conn)
                    cmd |> addText "queue" options.Queue
                    bind cmd
                    use! reader = cmd.ExecuteReaderAsync token
                    let! hasRow = reader.ReadAsync token

                    if not hasRow then
                        return Error(Db.decodeFailure (nameof LeaseUpdateOutcome) $"{operation} returned no row")
                    else
                        return outcomeFromString (Row.string reader "outcome")
                })
            ct

    /// <summary>
    /// Creates this machine's queue if it does not exist yet. Call once at startup: it builds
    /// tables and indexes, which is not a question a hot path should be asking.
    /// </summary>
    member _.EnsureQueueAsync(ct: CancellationToken) : Task<Result<bool, StoreError>> =
        protect
            (fun token ->
                task {
                    use! conn = dataSource.OpenConnectionAsync(token).AsTask()
                    use cmd = new NpgsqlCommand(SqlResources.get "action" "ensure_queue", conn)
                    cmd |> addText "queue" options.Queue
                    use! reader = cmd.ExecuteReaderAsync token
                    let! hasRow = reader.ReadAsync token

                    if not hasRow then
                        return Error(Db.decodeFailure "ActionQueue" "ensure_action_queue returned no row")
                    else
                        return Ok(Row.bool reader "created")
                })
            ct

    interface IActionQueue<'EntityId, 'Action> with

        member _.Claim(_, batch, lease, ct) =
            if batch < 1 then
                invalidArg (nameof batch) "A claim batch size must be positive."

            if lease <= TimeSpan.Zero then
                invalidArg (nameof lease) "A lease must be a positive duration."

            // Not idempotent, like the command claim: a claim whose reply was lost has already
            // leased the messages, and repeating it leases more while the first batch stays
            // invisible. The dispatcher polls again shortly, which recovers faster.
            Db.protectOnce
                (fun token ->
                    task {
                        use! conn = dataSource.OpenConnectionAsync(token).AsTask()
                        use cmd = new NpgsqlCommand(SqlResources.get "action" "claim", conn)
                        cmd |> addText "queue" options.Queue
                        cmd |> addInt "batch" batch
                        cmd.Parameters.AddWithValue("lease", lease) |> ignore
                        use! reader = cmd.ExecuteReaderAsync token

                        let claimed = ResizeArray()
                        let mutable failure = None
                        let mutable reading = true

                        while reading do
                            let! hasRow = reader.ReadAsync token

                            if not hasRow then
                                reading <- false
                            else
                                match readMessage (Row.string reader "message") with
                                | Error error ->
                                    failure <- Some error
                                    reading <- false
                                | Ok record ->
                                    claimed.Add
                                        { Work = record
                                          // The message's id identifies the claim; the delivery
                                          // count fences it. Every write below needs both.
                                          Token = LeaseToken.ofInt64 (Row.int64 reader "msg_id")
                                          DeliveryCount = Row.int32 reader "read_ct"
                                          ExpiresAt = Row.timestamp reader "vt" }

                        match failure with
                        | Some error -> return Error error
                        | None -> return Ok(List.ofSeq claimed)
                    })
                ct

        member _.Complete(action, ct) =
            fencedWrite
                "complete"
                (fun cmd ->
                    cmd |> addBigint "msg_id" (messageIdOf action)
                    cmd |> addInt "read_ct" action.DeliveryCount)
                ct

        member _.Reschedule(action, backoff, ct) =
            protect
                (fun token ->
                    task {
                        use! conn = dataSource.OpenConnectionAsync(token).AsTask()
                        use cmd = new NpgsqlCommand(SqlResources.get "action" "reschedule", conn)
                        cmd |> addText "queue" options.Queue
                        cmd |> addBigint "msg_id" (messageIdOf action)
                        cmd |> addInt "read_ct" action.DeliveryCount
                        cmd |> addInt "base_ms" (int (Backoff.baseDelay backoff).TotalMilliseconds)
                        cmd |> addInt "cap_ms" (int (Backoff.ceiling backoff).TotalMilliseconds)
                        use! reader = cmd.ExecuteReaderAsync token
                        let! hasRow = reader.ReadAsync token

                        if not hasRow then
                            return Error(Db.decodeFailure (nameof LeaseUpdateOutcome) "reschedule_action returned no row")
                        else
                            return outcomeFromString (Row.string reader "outcome")
                    })
                ct

        member _.Abandon(action, reason, ct) =
            fencedWrite
                "abandon"
                (fun cmd ->
                    cmd |> addBigint "msg_id" (messageIdOf action)
                    cmd |> addInt "read_ct" action.DeliveryCount
                    cmd |> addText "reason" reason)
                ct
