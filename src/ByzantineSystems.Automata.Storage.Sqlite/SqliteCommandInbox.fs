namespace ByzantineSystems.Automata.Storage.Sqlite

open System
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Sqlite.Schema
open FsToolkit.ErrorHandling
open Microsoft.Data.Sqlite
open SqlHydra.Query

/// <summary>
/// Everything the inbox needs to bridge the generic domain types to the columns of
/// <c>fsm_command</c>.
/// </summary>
type CommandInboxOptions<'EntityId, 'Event> =
    {
        Context: SqliteContext
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
module internal CommandMapping =

    let statusFromString (value: string) : Result<CommandStatus, StoreError> =
        match value with
        | "ready" -> Ok CommandStatus.Ready
        | "leased" -> Ok CommandStatus.Leased
        | "succeeded" -> Ok CommandStatus.Succeeded
        | "rejected" -> Ok CommandStatus.Rejected
        | "dead_letter" -> Ok CommandStatus.DeadLettered
        | other -> Error(Db.decodeFailure (nameof CommandStatus) $"unknown command status {other}")

    /// The schema stores the empty string for an unsupplied audit field.
    let audit (value: string option) : string = defaultArg value ""

    /// The schema stores the empty string for an unsupplied audit field.
    let optional (value: string) : string option = if value = "" then None else Some value

    let divergenceToString (divergence: Divergence) : string =
        match divergence with
        | Divergence.Fail -> "fail"
        | Divergence.Truncate -> "truncate"

    let divergenceFromString (value: string) : Result<Divergence, StoreError> =
        match value with
        | "fail" -> Ok Divergence.Fail
        | "truncate" -> Ok Divergence.Truncate
        | other -> Error(Db.decodeFailure (nameof Divergence) $"unknown divergence policy {other}")

    /// A fenced write changed one row or none, and none means the fence refused the caller.
    let leaseOutcome (changed: int) : LeaseUpdateOutcome =
        if changed = 1 then Updated else LeaseLost

    /// A duration as the schema counts it, in whole microseconds and never below one, which is
    /// what the backoff statements divide by.
    let micros (span: TimeSpan) : int64 = max 1L (span.Ticks / 10L)

    /// <summary>
    /// A claimed row, read into the generated <c>fsm_command</c> type. Building that record here,
    /// where every field is required, is what turns a column added to the table into a compile
    /// error in this reader rather than a value it silently leaves out.
    /// </summary>
    let commandRow (reader: SqliteDataReader) : main.fsm_command =
        { command_id = Row.int64 reader "command_id"
          machine_id = Row.string reader "machine_id"
          entity_id = Row.string reader "entity_id"
          seq = Row.int64 reader "seq"
          idempotency_key = Row.string reader "idempotency_key"
          chart_version = Row.int64 reader "chart_version"
          event = Row.string reader "event"
          kind = Row.string reader "kind"
          status = Row.string reader "status"
          blocked = Row.int64 reader "blocked"
          visible_at = Row.int64 reader "visible_at"
          lease_token = Row.int64 reader "lease_token"
          read_ct = Row.int64 reader "read_ct"
          attempts = Row.int64 reader "attempts"
          received_at = Row.int64 reader "received_at"
          tenant = Row.string reader "tenant"
          principal = Row.string reader "principal"
          source = Row.string reader "source"
          correlation_id = Row.string reader "correlation_id"
          causation_id = Row.string reader "causation_id" }

    let correctionRow (reader: SqliteDataReader) : main.fsm_command_correction =
        { command_id = Row.int64 reader "command_id"
          effective_at = Row.int64 reader "effective_at"
          on_divergence = Row.string reader "on_divergence"
          replay_limit = Row.int64 reader "replay_limit" }

/// <summary>
/// The SQLite command inbox.
///
/// Per-entity ordering is durable and holds across processes sharing the file: the schema holds
/// at most one claimable command per entity, and every write that finishes a claim carries the
/// lease token, so a worker that stalled past its lease changes nothing.
///
/// Each write is one <c>BEGIN IMMEDIATE</c> transaction of a few statements, which is where the
/// PostgreSQL store calls one routine. The file admits one writer at a time, so reading a value
/// and then writing from it inside that transaction is race-free without any lock of its own.
/// </summary>
type SqliteCommandInbox<'EntityId, 'Event>(options: CommandInboxOptions<'EntityId, 'Event>) =

    let context = options.Context

    /// A whole write transaction, retried as one unit when another process held the file.
    let write work ct =
        Db.protect
            context.Resilience
            (fun token -> Db.writeTransaction context.ConnectionString context.Clock work token)
            ct

    let read work ct =
        Db.query context.Resilience context.ReadConnectionString work ct

    /// What the command asks for. A correction's details live beside it, and one missing is a
    /// correction half-written, which the submission's single transaction makes impossible.
    let kindOf
        (row: main.fsm_command)
        (correction: main.fsm_command_correction option)
        : Result<CommandKind, StoreError> =
        match row.kind, correction with
        | "event", _ -> Ok CommandKind.Event
        | "correction", Some details ->
            CommandMapping.divergenceFromString details.on_divergence
            |> Result.map (fun divergence ->
                CommandKind.Correction(
                    Instant.toDateTimeOffset details.effective_at,
                    { OnDivergence = divergence
                      ReplayLimit = int details.replay_limit }
                ))
        | "correction", None -> Error(Db.decodeFailure (nameof CommandKind) "a correction has no recorded details")
        | other, _ -> Error(Db.decodeFailure (nameof CommandKind) $"unknown command kind {other}")

    /// Rebuilds one command from its generated row. The decodes can fail independently and the
    /// first failure wins; each means the row is not what this code was compiled against.
    let toRecord
        (row: main.fsm_command, correction: main.fsm_command_correction option)
        : Result<CommandRecord<'EntityId, 'Event>, StoreError> =
        let entityId =
            row.entity_id
            |> options.EntityIdDecode
            |> Result.mapError (Db.decodeFailure "EntityId")

        let chartVersion =
            int row.chart_version
            |> ChartVersion.tryCreate
            |> Result.mapError (Db.decodeFailure "ChartVersion")

        let event =
            row.event |> options.EventCodec.Decode |> Result.mapError Db.toStoreError

        let status = row.status |> CommandMapping.statusFromString
        let kind = kindOf row correction

        match entityId, chartVersion, event, status, kind with
        | Ok entityId, Ok chartVersion, Ok event, Ok status, Ok kind ->
            Ok
                { CommandId = CommandId.ofInt64 row.command_id
                  MachineId = MachineId.create row.machine_id
                  EntityId = entityId
                  Sequence = row.seq
                  IdempotencyKey = row.idempotency_key
                  ChartVersion = chartVersion
                  Kind = kind
                  Event = event
                  Status = status
                  Blocked = row.blocked <> 0L
                  VisibleAt = Instant.toDateTimeOffset row.visible_at
                  Attempts = int row.attempts
                  ReceivedAt = Instant.toDateTimeOffset row.received_at
                  Audit =
                    { Tenant = CommandMapping.optional row.tenant
                      Principal = CommandMapping.optional row.principal
                      Source = CommandMapping.optional row.source
                      CorrelationId = CommandMapping.optional row.correlation_id
                      CausationId = CommandMapping.optional row.causation_id } }
        | Error error, _, _, _, _
        | _, Error error, _, _, _
        | _, _, Error error, _, _
        | _, _, _, Error error, _
        | _, _, _, _, Error error -> Error error

    /// A claimed row carries its own lease: the token that fences it, the delivery count, and the
    /// deadline the claim assigned. visible_at means the lease deadline while a command is leased.
    let toLeased
        (corrections: Map<int64, main.fsm_command_correction>)
        (row: main.fsm_command)
        : Result<LeasedCommand<'EntityId, 'Event>, StoreError> =
        toRecord (row, Map.tryFind row.command_id corrections)
        |> Result.map (fun record ->
            { Work = record
              Token = LeaseToken.ofInt64 row.lease_token
              DeliveryCount = int row.read_ct
              ExpiresAt = record.VisibleAt })

    let single
        (row: (main.fsm_command * main.fsm_command_correction option) option)
        : Result<CommandRecord<'EntityId, 'Event> option, StoreError> =
        match row with
        | Some row -> toRecord row |> Result.map Some
        | None -> Ok None

    let findExisting (machineId: MachineId) (entityId: string) (idempotencyKey: string) (ct: CancellationToken) =
        read
            (fun query token ->
                selectTask query {
                    for c in main.fsm_command do
                        leftJoin d in main.fsm_command_correction on (c.command_id = d.Value.command_id)

                        where (
                            c.machine_id = MachineId.value machineId
                            && c.entity_id = entityId
                            && c.idempotency_key = idempotencyKey
                        )

                        select (c, d)
                        tryHead
                        cancel token
                }
                |> Task.map single)
            ct

    let submitIn
        (submission: CommandSubmission<'EntityId, 'Event>)
        (entityId: string)
        (encodedEvent: string)
        (conn: SqliteConnection)
        (transaction: SqliteTransaction)
        (now: int64)
        (ct: CancellationToken)
        : Task<Result<SubmissionOutcome, StoreError>> =
        backgroundTask {
            let machineId = MachineId.value submission.MachineId
            let version = ChartVersion.value submission.ChartVersion

            let! existing =
                Statement.tryOne
                    conn
                    transaction
                    (SqlResources.get "command" "find_by_key")
                    [ "@machine_id", box machineId
                      "@entity_id", box entityId
                      "@idempotency_key", box submission.IdempotencyKey ]
                    (fun reader -> Row.int64 reader "command_id")
                    ct

            match existing with
            | Some commandId -> return Ok(AlreadySubmitted(CommandId.ofInt64 commandId))
            | None ->
                let! registered =
                    Statement.tryOne
                        conn
                        transaction
                        (SqlResources.get "chart" "by_version")
                        [ "@machine_id", box machineId; "@version", box version ]
                        (fun reader -> Row.string reader "fingerprint")
                        ct

                match registered with
                | None ->
                    // The version this command pins was never registered, so nothing records
                    // which chart it refers to and a replay could not resolve it. Asked rather
                    // than caught: SQLite's foreign key failure names no constraint.
                    return Error(StoreError.NotFound $"chart version %d{version} of machine %s{machineId}")
                | Some _ ->
                    let kind =
                        match submission.Kind with
                        | CommandKind.Event -> "event"
                        | CommandKind.Correction _ -> "correction"

                    // A correction is claimable at once and arrives now. Its effective time is
                    // the instant it corrects, stored beside it.
                    let visibleAt, receivedAt =
                        match submission.Kind with
                        | CommandKind.Event ->
                            submission.VisibleAt
                            |> Option.map Instant.ofDateTimeOffset
                            |> Option.defaultValue now,
                            submission.ReceivedAt
                            |> Option.map Instant.ofDateTimeOffset
                            |> Option.defaultValue now
                        | CommandKind.Correction _ -> now, now

                    let! inserted =
                        Statement.tryOne
                            conn
                            transaction
                            (SqlResources.get "command" "submit")
                            [ "@machine_id", box machineId
                              "@entity_id", box entityId
                              "@idempotency_key", box submission.IdempotencyKey
                              "@chart_version", box version
                              "@event", box encodedEvent
                              "@kind", box kind
                              "@visible_at", box visibleAt
                              "@received_at", box receivedAt
                              "@tenant", box (CommandMapping.audit submission.Audit.Tenant)
                              "@principal", box (CommandMapping.audit submission.Audit.Principal)
                              "@source", box (CommandMapping.audit submission.Audit.Source)
                              "@correlation_id", box (CommandMapping.audit submission.Audit.CorrelationId)
                              "@causation_id", box (CommandMapping.audit submission.Audit.CausationId) ]
                            (fun reader -> Row.int64 reader "command_id")
                            ct

                    match inserted, submission.Kind with
                    | None, _ ->
                        return Error(Db.decodeFailure (nameof SubmissionOutcome) "the submission returned no row")
                    | Some commandId, CommandKind.Event -> return Ok(Accepted(CommandId.ofInt64 commandId))
                    | Some commandId, CommandKind.Correction(at, policy) ->
                        let! _ =
                            Statement.execute
                                conn
                                transaction
                                (SqlResources.get "command" "submit_correction")
                                [ "@command_id", box commandId
                                  "@effective_at", box (Instant.ofDateTimeOffset at)
                                  "@on_divergence", box (CommandMapping.divergenceToString policy.OnDivergence)
                                  "@replay_limit", box policy.ReplayLimit ]
                                ct

                        return Ok(Accepted(CommandId.ofInt64 commandId))
        }

    /// The lease token, the claimed rows and their correction details, all in the claim's own
    /// transaction. Decoding waits until it has committed.
    let claimIn
        (machineId: MachineId)
        (batch: int)
        (lease: TimeSpan)
        (conn: SqliteConnection)
        (transaction: SqliteTransaction)
        (now: int64)
        (ct: CancellationToken)
        =
        backgroundTask {
            let! token =
                Statement.tryOne
                    conn
                    transaction
                    (SqlResources.get "system" "next_token")
                    []
                    (fun reader -> Row.int64 reader "value")
                    ct

            match token with
            | None -> return Error(Db.decodeFailure "LeaseToken" "the lease token counter is missing")
            | Some token ->
                let! rows =
                    Statement.rows
                        conn
                        transaction
                        (SqlResources.get "command" "claim")
                        [ "@machine_id", box (MachineId.value machineId)
                          "@batch", box batch
                          "@now", box now
                          "@deadline", box (Instant.add now lease)
                          "@lease_token", box token ]
                        CommandMapping.commandRow
                        ct

                let corrections =
                    rows
                    |> List.filter (fun row -> row.kind = "correction")
                    |> List.map _.command_id

                match corrections with
                | [] -> return Ok(rows, Map.empty)
                | ids ->
                    let! details =
                        Statement.rows
                            conn
                            transaction
                            (SqlResources.get "command" "corrections")
                            [ "@ids", box (JsonSerializer.Serialize ids) ]
                            CommandMapping.correctionRow
                            ct

                    return Ok(rows, details |> List.map (fun d -> d.command_id, d) |> Map.ofList)
        }

    interface ICommandInbox<'EntityId, 'Event> with

        member _.Submit(submission, ct) =
            let entityId = options.EntityIdEncode submission.EntityId

            match options.EventCodec.Encode submission.Event with
            | Error error -> Task.FromResult(Error(Db.toStoreError error))
            | Ok encodedEvent -> write (submitIn submission entityId encodedEvent) ct

        member _.Claim(machineId, batch, lease, ct) =
            if batch < 1 then
                invalidArg (nameof batch) "A claim batch size must be positive."

            if lease <= TimeSpan.Zero then
                invalidArg (nameof lease) "A lease must be a positive duration."

            // Claiming is the one call that must not be repeated automatically. A claim whose
            // outcome was lost may have leased a batch, and a retry would lease a second one
            // while the first stays invisible until its lease lapses. The worker polls again in a
            // moment, which recovers sooner than a retry would.
            backgroundTaskResult {
                let! rows, corrections =
                    Db.protectOnce
                        (fun token ->
                            Db.writeTransaction
                                context.ConnectionString
                                context.Clock
                                (claimIn machineId batch lease)
                                token)
                        ct

                return! rows |> List.traverseResultM (toLeased corrections)
            }

        member _.Reschedule(commandId, leaseToken, backoff, ct) =
            write
                (fun conn transaction now token ->
                    Statement.execute
                        conn
                        transaction
                        (SqlResources.get "command" "reschedule")
                        [ "@command_id", box (CommandId.value commandId)
                          "@lease_token", box (LeaseToken.value leaseToken)
                          "@now", box now
                          "@base_us", box (CommandMapping.micros (Backoff.baseDelay backoff))
                          "@cap_us", box (CommandMapping.micros (Backoff.ceiling backoff)) ]
                        token
                    |> Task.map (CommandMapping.leaseOutcome >> Ok))
                ct

        member _.ExtendLease(commandId, leaseToken, lease, ct) =
            if lease <= TimeSpan.Zero then
                invalidArg (nameof lease) "A lease must be a positive duration."

            write
                (fun conn transaction now token ->
                    Statement.execute
                        conn
                        transaction
                        (SqlResources.get "command" "extend_lease")
                        [ "@command_id", box (CommandId.value commandId)
                          "@lease_token", box (LeaseToken.value leaseToken)
                          "@deadline", box (Instant.add now lease) ]
                        token
                    |> Task.map (CommandMapping.leaseOutcome >> Ok))
                ct

        member _.TryGet(commandId, ct) =
            let id = CommandId.value commandId

            read
                (fun query token ->
                    selectTask query {
                        for c in main.fsm_command do
                            leftJoin d in main.fsm_command_correction on (c.command_id = d.Value.command_id)
                            where (c.command_id = id)
                            select (c, d)
                            tryHead
                            cancel token
                    }
                    |> Task.map single)
                ct

        member _.TryFind(machineId, entityId, idempotencyKey, ct) =
            findExisting machineId (options.EntityIdEncode entityId) idempotencyKey ct
