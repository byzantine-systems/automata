namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open Npgsql
open NpgsqlTypes

/// <summary>
/// Everything the inbox needs to bridge the generic domain types to the columns of
/// <c>fsm.command</c>.
/// </summary>
type CommandInboxOptions<'EntityId, 'Event> =
    {
        DataSource: NpgsqlDataSource
        EventCodec: Codec<'Event>
        EntityIdEncode: 'EntityId -> string
        /// <summary>
        /// Decoding returns a result rather than raising. A row whose entity id no longer parses
        /// is a corrupt row, and the contract promises that as a
        /// <c>StoreError.Serialization</c>, not as an exception escaping the store.
        /// </summary>
        EntityIdDecode: string -> Result<'EntityId, string>
    }

/// <summary>Translations between the inbox's domain types and their column representations.</summary>
[<RequireQualifiedAccess>]
module private CommandMapping =

    let statusToString (status: CommandStatus) : string =
        match status with
        | CommandStatus.Ready -> "ready"
        | CommandStatus.Leased -> "leased"
        | CommandStatus.Succeeded -> "succeeded"
        | CommandStatus.Rejected -> "rejected"
        | CommandStatus.DeadLettered -> "dead_letter"

    let statusFromString (value: string) : Result<CommandStatus, StoreError> =
        match value with
        | "ready" -> Ok CommandStatus.Ready
        | "leased" -> Ok CommandStatus.Leased
        | "succeeded" -> Ok CommandStatus.Succeeded
        | "rejected" -> Ok CommandStatus.Rejected
        | "dead_letter" -> Ok CommandStatus.DeadLettered
        | other -> Error(Db.decodeFailure (nameof CommandStatus) $"unknown command status {other}")

    let outcomeFromString (value: string) : Result<LeaseUpdateOutcome, StoreError> =
        match value with
        | "updated" -> Ok Updated
        | "lease_lost" -> Ok LeaseLost
        | other -> Error(Db.decodeFailure (nameof LeaseUpdateOutcome) $"unknown lease outcome {other}")

    /// The schema stores the empty string for an unsupplied audit field.
    let audit (value: string option) : string = defaultArg value ""

/// <summary>
/// The PostgreSQL command inbox.
///
/// Per-entity ordering here is durable and cross-process: the database holds at most one
/// claimable command per entity, so two workers in two processes cannot both be advancing the
/// same entity. Every write that finishes a claim carries the lease token, so a worker that
/// stalled past its lease changes nothing.
/// </summary>
type PostgresCommandInbox<'EntityId, 'Event>(options: CommandInboxOptions<'EntityId, 'Event>) =

    let dataSource = options.DataSource

    let command (statement: string) (conn: NpgsqlConnection) = new NpgsqlCommand(statement, conn)

    let addText (name: string) (value: string) (cmd: NpgsqlCommand) =
        cmd.Parameters.AddWithValue(name, value) |> ignore

    let addInt (name: string) (value: int) (cmd: NpgsqlCommand) =
        cmd.Parameters.AddWithValue(name, value) |> ignore

    let addBigint (name: string) (value: int64) (cmd: NpgsqlCommand) =
        cmd.Parameters.AddWithValue(name, value) |> ignore

    let addInterval (name: string) (value: TimeSpan) (cmd: NpgsqlCommand) =
        cmd.Parameters.AddWithValue(name, value) |> ignore

    /// An absent instant is sent as a typed NULL. The routine substitutes its own default, which
    /// is the database clock, and the explicit type keeps the server from having to guess.
    let addOptionalTimestamp (name: string) (value: DateTimeOffset option) (cmd: NpgsqlCommand) =
        let parameter = NpgsqlParameter(name, NpgsqlDbType.TimestampTz)

        parameter.Value <-
            match value with
            | Some instant -> box (Db.timestamp instant)
            | None -> box DBNull.Value

        cmd.Parameters.Add parameter |> ignore

    let readAudit (reader: NpgsqlDataReader) : AuditContext =
        { Tenant = Row.optionalString reader "tenant"
          Principal = Row.optionalString reader "principal"
          Source = Row.optionalString reader "source"
          CorrelationId = Row.optionalString reader "correlation_id"
          CausationId = Row.optionalString reader "causation_id" }

    /// Rebuilds one command from its row. Four decodes can fail independently, and the first
    /// failure wins; none of them is expected, and all of them mean the row is not what this
    /// version of the code was compiled against.
    let readRecord (reader: NpgsqlDataReader) : Result<CommandRecord<'EntityId, 'Event>, StoreError> =
        let entityId =
            Row.string reader "entity_id"
            |> options.EntityIdDecode
            |> Result.mapError (Db.decodeFailure "EntityId")

        let chartVersion =
            Row.int32 reader "chart_version"
            |> ChartVersion.tryCreate
            |> Result.mapError (Db.decodeFailure "ChartVersion")

        let event =
            Row.string reader "event"
            |> options.EventCodec.Decode
            |> Result.mapError Db.toStoreError

        let status = Row.string reader "status" |> CommandMapping.statusFromString

        match entityId, chartVersion, event, status with
        | Ok entityId, Ok chartVersion, Ok event, Ok status ->
            Ok
                { CommandId = Row.int64 reader "command_id" |> CommandId.ofInt64
                  MachineId = Row.string reader "machine_id" |> MachineId.create
                  EntityId = entityId
                  Sequence = Row.int64 reader "seq"
                  IdempotencyKey = Row.string reader "idempotency_key"
                  ChartVersion = chartVersion
                  Event = event
                  Status = status
                  Blocked = Row.bool reader "blocked"
                  VisibleAt = Row.timestamp reader "visible_at"
                  Attempts = Row.int32 reader "attempts"
                  ReceivedAt = Row.timestamp reader "received_at"
                  Audit = readAudit reader }
        | Error error, _, _, _
        | _, Error error, _, _
        | _, _, Error error, _
        | _, _, _, Error error -> Error error

    /// A claimed row carries its own lease: the token that fences it, the delivery count, and the
    /// deadline the database assigned. visible_at means the lease deadline while a command is
    /// leased, which is the one place that dual reading of the column surfaces in F#.
    let readLeased (reader: NpgsqlDataReader) : Result<LeasedCommand<'EntityId, 'Event>, StoreError> =
        readRecord reader
        |> Result.map (fun record ->
            { Work = record
              Token = Row.int64 reader "lease_token" |> LeaseToken.ofInt64
              DeliveryCount = Row.int32 reader "read_ct"
              ExpiresAt = record.VisibleAt })

    /// Reads at most one row through a projection that may itself fail.
    let readOptional
        (project: NpgsqlDataReader -> Result<'T, StoreError>)
        (reader: NpgsqlDataReader)
        (ct: CancellationToken)
        : Task<Result<'T option, StoreError>> =
        task {
            let! hasRow = reader.ReadAsync ct

            if not hasRow then
                return Ok None
            else
                return project reader |> Result.map Some
        }

    let readOutcome (cmd: NpgsqlCommand) (ct: CancellationToken) : Task<Result<LeaseUpdateOutcome, StoreError>> =
        task {
            let! value = cmd.ExecuteScalarAsync ct

            match value with
            | :? string as outcome -> return CommandMapping.outcomeFromString outcome
            | other ->
                return
                    Error(
                        Db.decodeFailure (nameof LeaseUpdateOutcome) $"expected a lease outcome, got {other |> string}"
                    )
        }

    let findExisting
        (machineId: MachineId)
        (entityId: string)
        (idempotencyKey: string)
        (ct: CancellationToken)
        : Task<Result<CommandRecord<'EntityId, 'Event> option, StoreError>> =
        task {
            use! conn = dataSource.OpenConnectionAsync(ct).AsTask()
            use cmd = command (SqlResources.get "command" "by_idempotency_key") conn
            cmd |> addText "machine_id" (MachineId.value machineId)
            cmd |> addText "entity_id" entityId
            cmd |> addText "idempotency_key" idempotencyKey
            use! reader = cmd.ExecuteReaderAsync ct
            return! readOptional readRecord reader ct
        }

    let submitOnce
        (submission: CommandSubmission<'EntityId, 'Event>)
        (encodedEvent: string)
        (entityId: string)
        (ct: CancellationToken)
        : Task<Result<SubmissionOutcome, StoreError>> =
        task {
            use! conn = dataSource.OpenConnectionAsync(ct).AsTask()
            use cmd = command (SqlResources.get "command" "submit") conn
            cmd |> addText "machine_id" (MachineId.value submission.MachineId)
            cmd |> addText "entity_id" entityId
            cmd |> addText "idempotency_key" submission.IdempotencyKey
            cmd |> addInt "chart_version" (ChartVersion.value submission.ChartVersion)
            cmd |> addText "event" encodedEvent
            cmd |> addOptionalTimestamp "visible_at" submission.VisibleAt
            cmd |> addOptionalTimestamp "received_at" submission.ReceivedAt
            cmd |> addText "tenant" (CommandMapping.audit submission.Audit.Tenant)
            cmd |> addText "principal" (CommandMapping.audit submission.Audit.Principal)
            cmd |> addText "source" (CommandMapping.audit submission.Audit.Source)

            cmd
            |> addText "correlation_id" (CommandMapping.audit submission.Audit.CorrelationId)

            cmd
            |> addText "causation_id" (CommandMapping.audit submission.Audit.CausationId)

            use! reader = cmd.ExecuteReaderAsync ct
            let! hasRow = reader.ReadAsync ct

            if not hasRow then
                return Error(Db.decodeFailure (nameof SubmissionOutcome) "submit_command returned no row")
            else
                let commandId = Row.int64 reader "command_id" |> CommandId.ofInt64

                return
                    if Row.bool reader "accepted" then
                        Ok(Accepted commandId)
                    else
                        Ok(AlreadySubmitted commandId)
        }

    interface ICommandInbox<'EntityId, 'Event> with

        member _.Submit(submission, ct) =
            Db.protect
                (fun token ->
                    task {
                        let entityId = options.EntityIdEncode submission.EntityId

                        match options.EventCodec.Encode submission.Event with
                        | Error error -> return Error(Db.toStoreError error)
                        | Ok encodedEvent ->
                            try
                                return! submitOnce submission encodedEvent entityId token
                            with
                            | Db.ForeignKeyViolation "command_chart_version_fkey" ->
                                // The version this command pins was never registered, so nothing
                                // records which chart it refers to and a replay could not resolve
                                // it. Caught here so the caller gets the contract's error rather
                                // than a PostgresException crossing the store boundary.
                                return
                                    Error(
                                        StoreError.NotFound
                                            $"chart version %d{ChartVersion.value submission.ChartVersion} of machine %s{MachineId.value submission.MachineId}"
                                    )
                            | Db.UniqueViolation "command_unique_idem" ->
                                // fsm.submit_command serialises submissions per entity, so this
                                // is unreachable through the routine. It stays because the
                                // constraint, not the advisory lock, is the integrity boundary:
                                // if anything ever writes the table another way, a concurrent
                                // duplicate must still read as AlreadySubmitted rather than
                                // escaping as an error.
                                let! existing =
                                    findExisting submission.MachineId entityId submission.IdempotencyKey token

                                return
                                    existing
                                    |> Result.bind (function
                                        | Some record -> Ok(AlreadySubmitted record.CommandId)
                                        | None ->
                                            Error(
                                                Db.decodeFailure
                                                    (nameof SubmissionOutcome)
                                                    "a duplicate idempotency key had no matching command"
                                            ))
                    })
                ct

        member _.Claim(machineId, batch, lease, ct) =
            if batch < 1 then
                invalidArg (nameof batch) "A claim batch size must be positive."

            if lease <= TimeSpan.Zero then
                invalidArg (nameof lease) "A lease must be a positive duration."

            Db.protect
                (fun token ->
                    task {
                        use! conn = dataSource.OpenConnectionAsync(token).AsTask()
                        use cmd = command (SqlResources.get "command" "claim") conn
                        cmd |> addText "machine_id" (MachineId.value machineId)
                        cmd |> addInt "batch" batch
                        cmd |> addInterval "lease" lease
                        use! reader = cmd.ExecuteReaderAsync token

                        let rec read claimed =
                            task {
                                let! hasRow = reader.ReadAsync token

                                if not hasRow then
                                    return Ok(List.rev claimed)
                                else
                                    match readLeased reader with
                                    | Ok leased -> return! read (leased :: claimed)
                                    | Error error -> return Error error
                            }

                        return! read []
                    })
                ct

        member _.Reschedule(commandId, leaseToken, backoff, ct) =
            Db.protect
                (fun token ->
                    task {
                        use! conn = dataSource.OpenConnectionAsync(token).AsTask()
                        use cmd = command (SqlResources.get "command" "reschedule") conn
                        cmd |> addBigint "command_id" (CommandId.value commandId)
                        cmd |> addBigint "lease_token" (LeaseToken.value leaseToken)
                        cmd |> addInt "base_ms" (int (Backoff.baseDelay backoff).TotalMilliseconds)
                        cmd |> addInt "cap_ms" (int (Backoff.ceiling backoff).TotalMilliseconds)
                        use! reader = cmd.ExecuteReaderAsync token
                        let! hasRow = reader.ReadAsync token

                        if not hasRow then
                            return Error(Db.decodeFailure (nameof LeaseUpdateOutcome) "reschedule returned no row")
                        else
                            return Row.string reader "outcome" |> CommandMapping.outcomeFromString
                    })
                ct

        member _.ExtendLease(commandId, leaseToken, lease, ct) =
            if lease <= TimeSpan.Zero then
                invalidArg (nameof lease) "A lease must be a positive duration."

            Db.protect
                (fun token ->
                    task {
                        use! conn = dataSource.OpenConnectionAsync(token).AsTask()
                        use cmd = command (SqlResources.get "command" "extend_lease") conn
                        cmd |> addBigint "command_id" (CommandId.value commandId)
                        cmd |> addBigint "lease_token" (LeaseToken.value leaseToken)
                        cmd |> addInterval "lease" lease
                        return! readOutcome cmd token
                    })
                ct

        member _.TryGet(commandId, ct) =
            Db.protect
                (fun token ->
                    task {
                        use! conn = dataSource.OpenConnectionAsync(token).AsTask()
                        use cmd = command (SqlResources.get "command" "by_id") conn
                        cmd |> addBigint "command_id" (CommandId.value commandId)
                        use! reader = cmd.ExecuteReaderAsync token
                        return! readOptional readRecord reader token
                    })
                ct

        member _.TryFind(machineId, entityId, idempotencyKey, ct) =
            Db.protect (fun token -> findExisting machineId (options.EntityIdEncode entityId) idempotencyKey token) ct
