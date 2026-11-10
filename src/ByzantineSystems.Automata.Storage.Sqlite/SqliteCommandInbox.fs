namespace ByzantineSystems.Automata.Storage.Sqlite

open System
open System.IO
open System.Security.Cryptography
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Internal
open ByzantineSystems.Automata.Storage.Sqlite.Schema
open FsToolkit.ErrorHandling
open Microsoft.Data.Sqlite

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

    /// Provider-local identity, independent of JSON whitespace and object key order.
    /// PostgreSQL computes its digest from jsonb instead; digest bytes are not portable.
    let requestDigest version kind (encodedEvent: string) =
        use document = JsonDocument.Parse encodedEvent
        use buffer = new MemoryStream()
        use writer = new Utf8JsonWriter(buffer)

        let rec write (element: JsonElement) =
            match element.ValueKind with
            | JsonValueKind.Object ->
                writer.WriteStartObject()

                element.EnumerateObject()
                |> Seq.map (fun property -> property.Name, property.Value)
                |> Map.ofSeq
                |> Map.iter (fun name value ->
                    writer.WritePropertyName name
                    write value)

                writer.WriteEndObject()
            | JsonValueKind.Array ->
                writer.WriteStartArray()
                element.EnumerateArray() |> Seq.iter write
                writer.WriteEndArray()
            | _ -> element.WriteTo writer

        writer.WriteStartArray()
        writer.WriteNumberValue(version: int)

        match kind with
        | CommandKind.Event -> writer.WriteStringValue "event"
        | CommandKind.Correction(at, policy) ->
            writer.WriteStringValue "correction"
            writer.WriteNumberValue(Instant.ofDateTimeOffset at)
            writer.WriteStringValue(EnumCodec.encode EnumCodec.divergence policy.OnDivergence)
            writer.WriteNumberValue policy.ReplayLimit

        write document.RootElement
        writer.WriteEndArray()
        writer.Flush()
        SHA256.HashData(buffer.ToArray())

    /// The schema stores the empty string for an unsupplied audit field.
    let audit (value: string option) : string = defaultArg value ""

    /// The schema stores the empty string for an unsupplied audit field.
    let optional (value: string) : string option = if value = "" then None else Some value

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
          request_digest = reader.GetFieldValue<byte array>(reader.GetOrdinal "request_digest")
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

    let encodeEntityId entityId =
        options.EntityIdEncode entityId
        |> ByzantineSystems.Automata.Storage.Internal.EntityKey.ofString

    let findByKey = StatementKey.create "command" "find_by_key" |> Sql.prepareProgram
    let commandById = StatementKey.create "command" "by_id" |> Sql.prepareProgram

    let commandByKey =
        StatementKey.create "command" "find_record_by_key" |> Sql.prepareProgram

    let chartByVersion = StatementKey.create "chart" "by_version" |> Sql.prepareProgram
    let submitCommand = StatementKey.create "command" "submit" |> Sql.prepareProgram

    let submitCorrection =
        StatementKey.create "command" "submit_correction" |> Sql.prepareProgram

    let nextToken = StatementKey.create "system" "next_token" |> Sql.prepareProgram
    let claimCommands = StatementKey.create "command" "claim" |> Sql.prepareProgram

    let rescheduleCommand =
        StatementKey.create "command" "reschedule" |> Sql.prepareProgram

    let extendLease = StatementKey.create "command" "extend_lease" |> Sql.prepareProgram

    /// What the command asks for. A correction's details live beside it, and one missing is a
    /// correction half-written, which the submission's single transaction makes impossible.
    let kindOf
        (row: main.fsm_command)
        (correction: main.fsm_command_correction option)
        : Result<CommandKind, StoreError> =
        match row.kind, correction with
        | "event", _ -> Ok CommandKind.Event
        | "correction", Some details ->
            EnumCodec.decode EnumCodec.divergence details.on_divergence
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

        let machineId =
            row.machine_id
            |> MachineId.tryCreate
            |> Result.mapError (Db.decodeFailure "MachineId")

        let event =
            row.event |> options.EventCodec.Decode |> Result.mapError Db.toStoreError

        let status = row.status |> EnumCodec.decode EnumCodec.commandStatus
        let kind = kindOf row correction

        match machineId, entityId, chartVersion, event, status, kind with
        | Ok machineId, Ok entityId, Ok chartVersion, Ok event, Ok status, Ok kind ->
            Ok
                { CommandId = CommandId.ofInt64 row.command_id
                  MachineId = machineId
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
        | Error error, _, _, _, _, _
        | _, Error error, _, _, _, _
        | _, _, Error error, _, _, _
        | _, _, _, Error error, _, _
        | _, _, _, _, Error error, _
        | _, _, _, _, _, Error error -> Error error

    let correctionOption (reader: SqliteDataReader) : main.fsm_command_correction option =
        let marker = reader.GetOrdinal "correction_command_id"

        if reader.IsDBNull marker then
            None
        else
            Some
                { command_id = reader.GetInt64 marker
                  effective_at = Row.int64 reader "correction_effective_at"
                  on_divergence = Row.string reader "correction_on_divergence"
                  replay_limit = Row.int64 reader "correction_replay_limit" }

    let recordRow (reader: SqliteDataReader) : Result<CommandRecord<'EntityId, 'Event>, StoreError> =
        toRecord (CommandMapping.commandRow reader, correctionOption reader)

    /// A claimed row carries its own lease: the token that fences it, the delivery count, and the
    /// deadline the claim assigned. visible_at means the lease deadline while a command is leased.
    let leasedRow (reader: SqliteDataReader) : Result<LeasedCommand<'EntityId, 'Event>, StoreError> =
        let row = CommandMapping.commandRow reader

        toRecord (row, correctionOption reader)
        |> Result.map (fun record ->
            { Work = record
              Token = LeaseToken.ofInt64 row.lease_token
              DeliveryCount = int row.read_ct
              ExpiresAt = record.VisibleAt })

    let findExisting
        (machineId: MachineId)
        (entityId: ByzantineSystems.Automata.Storage.Internal.EntityKey)
        (idempotencyKey: string)
        (ct: CancellationToken)
        =
        Sql.readProgram
            context
            (StoreProgram.tryQueryOne
                commandByKey
                [ Params.machineId "@machine_id" machineId
                  Params.entityKey "@entity_id" entityId
                  "@idempotency_key", Param.Text idempotencyKey ]
                recordRow)
            ct

    let submit
        (submission: CommandSubmission<'EntityId, 'Event>)
        (entityId: ByzantineSystems.Automata.Storage.Internal.EntityKey)
        (encodedEvent: string)
        (now: int64)
        : StoreProgram<WriteAccess, (string * Param) list, SqliteDataReader, SubmissionOutcome> =
        let machineId = submission.MachineId
        let version = ChartVersion.value submission.ChartVersion
        let digest = CommandMapping.requestDigest version submission.Kind encodedEvent

        let existing =
            StoreProgram.tryQueryOne
                findByKey
                [ Params.machineId "@machine_id" machineId
                  Params.entityKey "@entity_id" entityId
                  "@idempotency_key", Param.Text submission.IdempotencyKey ]
                (fun reader ->
                    let commandId = Row.int64 reader "command_id" |> CommandId.ofInt64
                    let stored = reader.GetFieldValue<byte array>(reader.GetOrdinal "request_digest")

                    if stored = digest then
                        Ok(AlreadySubmitted commandId)
                    else
                        Ok(SubmissionOutcome.Refused(KeyReused commandId)))

        let registered =
            StoreProgram.tryQueryOne
                chartByVersion
                [ Params.machineId "@machine_id" machineId
                  "@version", Param.Integer(int64 version) ]
                (fun reader -> Ok(Row.string reader "fingerprint"))
            |> StoreProgram.map Option.isSome

        let kind, correction =
            match submission.Kind with
            | CommandKind.Event -> "event", None
            | CommandKind.Correction(at, policy) ->
                "correction",
                Some(fun commandId ->
                    StoreProgram.executeDiscard
                        submitCorrection
                        [ Params.commandId "@command_id" commandId
                          "@effective_at", Param.Integer(Instant.ofDateTimeOffset at)
                          "@on_divergence", Param.Text(EnumCodec.encode EnumCodec.divergence policy.OnDivergence)
                          "@replay_limit", Param.Integer(int64 policy.ReplayLimit) ])

        // A correction is claimable at once and arrives now. Its effective time is stored beside it.
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

        let insert =
            StoreProgram.queryOne
                submitCommand
                [ Params.machineId "@machine_id" machineId
                  Params.entityKey "@entity_id" entityId
                  "@idempotency_key", Param.Text submission.IdempotencyKey
                  "@chart_version", Param.Integer(int64 version)
                  "@event", Param.Text encodedEvent
                  "@request_digest", Param.Blob digest
                  "@kind", Param.Text kind
                  "@visible_at", Param.Integer visibleAt
                  "@received_at", Param.Integer receivedAt
                  "@tenant", Param.Text(CommandMapping.audit submission.Audit.Tenant)
                  "@principal", Param.Text(CommandMapping.audit submission.Audit.Principal)
                  "@source", Param.Text(CommandMapping.audit submission.Audit.Source)
                  "@correlation_id", Param.Text(CommandMapping.audit submission.Audit.CorrelationId)
                  "@causation_id", Param.Text(CommandMapping.audit submission.Audit.CausationId) ]
                (fun reader -> Ok(Row.int64 reader "command_id" |> CommandId.ofInt64))

        CheckedSubmission(
            existing,
            registered,
            StoreError.NotFound $"chart version %d{version} of machine %s{MachineId.value machineId}",
            insert,
            correction
        )
        |> CommandInboxProgram.submit

    /// The lease token and complete claimed rows, all in the claim's own transaction. Decoding
    /// waits until it has committed.
    let claim (machineId: MachineId) (batch: int) (lease: TimeSpan) (now: int64) =
        let token =
            StoreProgram.queryOne nextToken [] (fun reader -> Ok(Row.int64 reader "value"))

        store {
            let! token = token

            return!
                StoreProgram.queryRows
                    claimCommands
                    [ Params.machineId "@machine_id" machineId
                      "@batch", Param.Integer(int64 batch)
                      "@now", Param.Integer now
                      "@deadline", Param.Integer(Instant.add now lease)
                      "@lease_token", Param.Integer token ]
                    leasedRow
        }

    interface ICommandInbox<'EntityId, 'Event> with

        member _.Submit(submission, ct) =
            let entityId = encodeEntityId submission.EntityId

            // Encoded before the write begins, so the transaction only writes.
            backgroundTaskResult {
                let! encodedEvent = options.EventCodec.Encode submission.Event |> Result.mapError Db.toStoreError
                return! Sql.writeProgramWith context (submit submission entityId encodedEvent) ct
            }

        member _.Claim(machineId, batch, lease, ct) =
            // Claiming is the one call that must not be repeated automatically. A claim whose
            // outcome was lost may have leased a batch, and a retry would lease a second one
            // while the first stays invisible until its lease lapses. The worker polls again in a
            // moment, which recovers sooner than a retry would.
            Sql.writeProgramOnceWith context (claim machineId (ClaimBatch.value batch) (LeaseDuration.value lease)) ct

        member _.Reschedule(commandId, leaseToken, backoff, ct) =
            Sql.writeProgramWith
                context
                (fun now ->
                    CommandInboxProgram.leaseUpdate (
                        ChangedRows(
                            rescheduleCommand,
                            [ Params.commandId "@command_id" commandId
                              Params.commandLease "@lease_token" leaseToken
                              "@now", Param.Integer now
                              "@base_us", Param.Integer(CommandMapping.micros (Backoff.baseDelay backoff))
                              "@cap_us", Param.Integer(CommandMapping.micros (Backoff.ceiling backoff)) ]
                        )
                    ))
                ct

        member _.ExtendLease(commandId, leaseToken, lease, ct) =
            Sql.writeProgramWith
                context
                (fun now ->
                    CommandInboxProgram.leaseUpdate (
                        ChangedRows(
                            extendLease,
                            [ Params.commandId "@command_id" commandId
                              Params.commandLease "@lease_token" leaseToken
                              "@deadline", Param.Integer(Instant.add now (LeaseDuration.value lease)) ]
                        )
                    ))
                ct

        member _.TryGet(commandId, ct) =
            Sql.readProgram
                context
                (StoreProgram.tryQueryOne commandById [ Params.commandId "@command_id" commandId ] recordRow)
                ct

        member _.TryFind(machineId, entityId, idempotencyKey, ct) =
            findExisting machineId (encodeEntityId entityId) idempotencyKey ct
