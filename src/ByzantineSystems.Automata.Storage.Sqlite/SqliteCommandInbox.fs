namespace ByzantineSystems.Automata.Storage.Sqlite

open System
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Internal
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

    let findByKey = Statement.load "command" "find_by_key"
    let chartByVersion = Statement.load "chart" "by_version"
    let submitCommand = Statement.load "command" "submit"
    let submitCorrection = Statement.load "command" "submit_correction"
    let nextToken = Statement.load "system" "next_token"
    let claimCommands = Statement.load "command" "claim"
    let correctionsOf = Statement.load "command" "corrections"
    let rescheduleCommand = Statement.load "command" "reschedule"
    let extendLease = Statement.load "command" "extend_lease"

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
        let machine = MachineId.value machineId

        Sql.read
            context
            (sqliteRead {
                let! row =
                    Sql.select (fun query token ->
                        selectTask query {
                            for c in main.fsm_command do
                                leftJoin d in main.fsm_command_correction on (c.command_id = d.Value.command_id)

                                where (
                                    c.machine_id = machine
                                    && c.entity_id = entityId
                                    && c.idempotency_key = idempotencyKey
                                )

                                select (c, d)
                                tryHead
                                cancel token
                        })

                return! single row
            })
            ct

    let submit
        (submission: CommandSubmission<'EntityId, 'Event>)
        (entityId: string)
        (encodedEvent: string)
        : Op<WriteSession, SubmissionOutcome> =
        sqliteWrite {
            let machineId = MachineId.value submission.MachineId
            let version = ChartVersion.value submission.ChartVersion

            let! existing =
                Sql.tryOne
                    (nameof SubmissionOutcome)
                    findByKey
                    [ "@machine_id", Param.Text machineId
                      "@entity_id", Param.Text entityId
                      "@idempotency_key", Param.Text submission.IdempotencyKey ]
                    (fun reader -> Row.int64 reader "command_id")

            match existing with
            | Some commandId -> return AlreadySubmitted(CommandId.ofInt64 commandId)
            | None ->
                let! registered =
                    Sql.tryOne
                        "ChartFingerprint"
                        chartByVersion
                        [ "@machine_id", Param.Text machineId
                          "@version", Param.Integer(int64 version) ]
                        (fun reader -> Row.string reader "fingerprint")

                match registered with
                | None ->
                    // The version this command pins was never registered, so nothing records
                    // which chart it refers to and a replay could not resolve it. Asked rather
                    // than caught: SQLite's foreign key failure names no constraint.
                    return! Op.fail (StoreError.NotFound $"chart version %d{version} of machine %s{machineId}")
                | Some _ ->
                    let! now = Sql.now

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

                    let! commandId =
                        Sql.one
                            (nameof SubmissionOutcome)
                            submitCommand
                            [ "@machine_id", Param.Text machineId
                              "@entity_id", Param.Text entityId
                              "@idempotency_key", Param.Text submission.IdempotencyKey
                              "@chart_version", Param.Integer(int64 version)
                              "@event", Param.Text encodedEvent
                              "@kind", Param.Text kind
                              "@visible_at", Param.Integer visibleAt
                              "@received_at", Param.Integer receivedAt
                              "@tenant", Param.Text(CommandMapping.audit submission.Audit.Tenant)
                              "@principal", Param.Text(CommandMapping.audit submission.Audit.Principal)
                              "@source", Param.Text(CommandMapping.audit submission.Audit.Source)
                              "@correlation_id", Param.Text(CommandMapping.audit submission.Audit.CorrelationId)
                              "@causation_id", Param.Text(CommandMapping.audit submission.Audit.CausationId) ]
                            (fun reader -> Row.int64 reader "command_id")

                    match submission.Kind with
                    | CommandKind.Event -> return Accepted(CommandId.ofInt64 commandId)
                    | CommandKind.Correction(at, policy) ->
                        do!
                            Sql.execute
                                submitCorrection
                                [ "@command_id", Param.Integer commandId
                                  "@effective_at", Param.Integer(Instant.ofDateTimeOffset at)
                                  "@on_divergence", Param.Text(CommandMapping.divergenceToString policy.OnDivergence)
                                  "@replay_limit", Param.Integer(int64 policy.ReplayLimit) ]
                            |> Op.discard

                        return Accepted(CommandId.ofInt64 commandId)
        }

    /// The lease token, the claimed rows and their correction details, all in the claim's own
    /// transaction. Decoding waits until it has committed.
    let claim (machineId: MachineId) (batch: int) (lease: TimeSpan) =
        sqliteWrite {
            let! now = Sql.now
            let! token = Sql.one "LeaseToken" nextToken [] (fun reader -> Row.int64 reader "value")

            let! rows =
                Sql.rows
                    claimCommands
                    [ "@machine_id", Param.Text(MachineId.value machineId)
                      "@batch", Param.Integer(int64 batch)
                      "@now", Param.Integer now
                      "@deadline", Param.Integer(Instant.add now lease)
                      "@lease_token", Param.Integer token ]
                    CommandMapping.commandRow

            let corrections =
                rows
                |> List.filter (fun row -> row.kind = "correction")
                |> List.map _.command_id

            match corrections with
            | [] -> return rows, Map.empty
            | ids ->
                let! details =
                    Sql.rows
                        correctionsOf
                        [ "@ids", Param.Text(JsonSerializer.Serialize ids) ]
                        CommandMapping.correctionRow

                return rows, details |> List.map (fun d -> d.command_id, d) |> Map.ofList
        }

    interface ICommandInbox<'EntityId, 'Event> with

        member _.Submit(submission, ct) =
            let entityId = options.EntityIdEncode submission.EntityId

            // Encoded before the write begins, so the transaction only writes.
            backgroundTaskResult {
                let! encodedEvent = options.EventCodec.Encode submission.Event |> Result.mapError Db.toStoreError
                return! Sql.write context (submit submission entityId encodedEvent) ct
            }

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
                let! rows, corrections = Sql.writeOnce context (claim machineId batch lease) ct
                return! rows |> List.traverseResultM (toLeased corrections)
            }

        member _.Reschedule(commandId, leaseToken, backoff, ct) =
            Sql.write
                context
                (sqliteWrite {
                    let! now = Sql.now

                    let! changed =
                        Sql.execute
                            rescheduleCommand
                            [ "@command_id", Param.Integer(CommandId.value commandId)
                              "@lease_token", Param.Integer(LeaseToken.value leaseToken)
                              "@now", Param.Integer now
                              "@base_us", Param.Integer(CommandMapping.micros (Backoff.baseDelay backoff))
                              "@cap_us", Param.Integer(CommandMapping.micros (Backoff.ceiling backoff)) ]

                    return CommandMapping.leaseOutcome changed
                })
                ct

        member _.ExtendLease(commandId, leaseToken, lease, ct) =
            if lease <= TimeSpan.Zero then
                invalidArg (nameof lease) "A lease must be a positive duration."

            Sql.write
                context
                (sqliteWrite {
                    let! now = Sql.now

                    let! changed =
                        Sql.execute
                            extendLease
                            [ "@command_id", Param.Integer(CommandId.value commandId)
                              "@lease_token", Param.Integer(LeaseToken.value leaseToken)
                              "@deadline", Param.Integer(Instant.add now lease) ]

                    return CommandMapping.leaseOutcome changed
                })
                ct

        member _.TryGet(commandId, ct) =
            let id = CommandId.value commandId

            Sql.read
                context
                (sqliteRead {
                    let! row =
                        Sql.select (fun query token ->
                            selectTask query {
                                for c in main.fsm_command do
                                    leftJoin d in main.fsm_command_correction on (c.command_id = d.Value.command_id)
                                    where (c.command_id = id)
                                    select (c, d)
                                    tryHead
                                    cancel token
                            })

                    return! single row
                })
                ct

        member _.TryFind(machineId, entityId, idempotencyKey, ct) =
            findExisting machineId (options.EntityIdEncode entityId) idempotencyKey ct
