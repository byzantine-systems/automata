namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Internal
open ByzantineSystems.Automata.Storage.Postgres.Schema
open Npgsql
open FsToolkit.ErrorHandling

/// <summary>
/// Everything the inbox needs to bridge the generic domain types to the columns of
/// <c>fsm.command</c>.
/// </summary>
type CommandInboxOptions<'EntityId, 'Event> =
    {
        Context: PostgresContext
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

    let context = options.Context

    let submitCommand = StatementKey.create "command" "submit" |> Sql.prepareProgram

    let submitCorrection =
        StatementKey.create "command" "submit_correction" |> Sql.prepareProgram

    let claimCommands = StatementKey.create "command" "claim" |> Sql.prepareProgram

    let commandById = StatementKey.create "command" "by_id" |> Sql.prepareProgram

    let commandByKey = StatementKey.create "command" "find_by_key" |> Sql.prepareProgram

    let rescheduleCommand =
        StatementKey.create "command" "reschedule" |> Sql.prepareProgram

    let extendLease = StatementKey.create "command" "extend_lease" |> Sql.prepareProgram

    /// The schema stores the empty string for an unsupplied audit field.
    let optional (value: string) : string option = if value = "" then None else Some value

    /// Rebuilds one command from its generated row. Three decodes can fail independently, and
    /// the first failure wins; none of them is expected, and all of them mean the row is not what
    /// this version of the code was compiled against.
    /// What the command asks for. A correction's details live beside it, and one missing is a
    /// correction the database half-wrote, which the routine that writes it makes impossible.
    let kindOf (row: fsm.command) (correction: fsm.command_correction option) : Result<CommandKind, StoreError> =
        match row.kind, correction with
        | "event", _ -> Ok CommandKind.Event
        | "correction", Some details ->
            EnumCodec.decode EnumCodec.divergence details.on_divergence
            |> Result.map (fun divergence ->
                CommandKind.Correction(
                    Db.fromTimestamp details.effective_at,
                    { OnDivergence = divergence
                      ReplayLimit = details.replay_limit }
                ))
        | "correction", None -> Error(Db.decodeFailure (nameof CommandKind) "a correction has no recorded details")
        | other, _ -> Error(Db.decodeFailure (nameof CommandKind) $"unknown command kind {other}")

    let toRecord
        (row: fsm.command, correction: fsm.command_correction option)
        : Result<CommandRecord<'EntityId, 'Event>, StoreError> =
        let entityId =
            row.entity_id
            |> options.EntityIdDecode
            |> Result.mapError (Db.decodeFailure "EntityId")

        let chartVersion =
            row.chart_version
            |> ChartVersion.tryCreate
            |> Result.mapError (Db.decodeFailure "ChartVersion")

        let event =
            row.event |> options.EventCodec.Decode |> Result.mapError Db.toStoreError

        let status = row.status |> EnumCodec.decode EnumCodec.commandStatus
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
                  Blocked = row.blocked
                  VisibleAt = Db.fromTimestamp row.visible_at
                  Attempts = row.attempts
                  ReceivedAt = Db.fromTimestamp row.received_at
                  Audit =
                    { Tenant = optional row.tenant
                      Principal = optional row.principal
                      Source = optional row.source
                      CorrelationId = optional row.correlation_id
                      CausationId = optional row.causation_id } }
        | Error error, _, _, _, _
        | _, Error error, _, _, _
        | _, _, Error error, _, _
        | _, _, _, Error error, _
        | _, _, _, _, Error error -> Error error

    /// <summary>
    /// A claimed row, read into the generated <c>fsm.command</c> type.
    ///
    /// The claim is a routine SqlHydra cannot call, but it returns <c>SETOF fsm.command</c>, so
    /// its rows have the generated type's shape. Building that record here, where every field is
    /// required, is what turns a column added to the table into a compile error in this reader
    /// rather than a value it silently leaves out.
    /// </summary>
    let commandRow (reader: NpgsqlDataReader) : fsm.command =
        { command_id = Row.int64 reader "command_id"
          machine_id = Row.string reader "machine_id"
          entity_id = Row.string reader "entity_id"
          seq = Row.int64 reader "seq"
          idempotency_key = Row.string reader "idempotency_key"
          chart_version = Row.int32 reader "chart_version"
          event = Row.string reader "event"
          kind = Row.string reader "kind"
          status = Row.string reader "status"
          blocked = Row.bool reader "blocked"
          visible_at = reader.GetDateTime(reader.GetOrdinal "visible_at")
          lease_token = Row.int64 reader "lease_token"
          read_ct = Row.int32 reader "read_ct"
          attempts = Row.int32 reader "attempts"
          received_at = reader.GetDateTime(reader.GetOrdinal "received_at")
          tenant = Row.string reader "tenant"
          principal = Row.string reader "principal"
          source = Row.string reader "source"
          correlation_id = Row.string reader "correlation_id"
          causation_id = Row.string reader "causation_id" }

    let correctionOption (reader: NpgsqlDataReader) : fsm.command_correction option =
        let marker = reader.GetOrdinal "correction_command_id"

        if reader.IsDBNull marker then
            None
        else
            Some
                { command_id = reader.GetInt64 marker
                  effective_at = reader.GetDateTime(reader.GetOrdinal "correction_effective_at")
                  on_divergence = Row.string reader "correction_on_divergence"
                  replay_limit = Row.int32 reader "correction_replay_limit" }

    let recordRow (reader: NpgsqlDataReader) : Result<CommandRecord<'EntityId, 'Event>, StoreError> =
        toRecord (commandRow reader, correctionOption reader)

    /// A claimed row carries its own lease: the token that fences it, the delivery count, and the
    /// deadline the database assigned. visible_at means the lease deadline while a command is
    /// leased, which is the one place that dual reading of the column surfaces in F#.
    let leasedRow (reader: NpgsqlDataReader) : Result<LeasedCommand<'EntityId, 'Event>, StoreError> =
        let row = commandRow reader

        toRecord (row, correctionOption reader)
        |> Result.map (fun record ->
            { Work = record
              Token = LeaseToken.ofInt64 row.lease_token
              DeliveryCount = row.read_ct
              ExpiresAt = record.VisibleAt })

    let findExisting
        (machineId: MachineId)
        (entityId: string)
        (idempotencyKey: string)
        (ct: CancellationToken)
        : Task<Result<CommandRecord<'EntityId, 'Event> option, StoreError>> =
        Sql.readProgram
            context
            (StoreProgram.tryQueryOne
                commandByKey
                [ "machine_id", Param.Text(MachineId.value machineId)
                  "entity_id", Param.Text entityId
                  "idempotency_key", Param.Text idempotencyKey ]
                recordRow)
            ct

    /// <summary>
    /// One submission. A correction is submitted through its own routine, which records what it
    /// corrects in the same transaction. It is claimable at once and arrives now: its effective
    /// time is the instant it corrects, which the routine stores beside it.
    /// </summary>
    let submit (submission: CommandSubmission<'EntityId, 'Event>) (encodedEvent: string) (entityId: string) =
        let common =
            [ "machine_id", Param.Text(MachineId.value submission.MachineId)
              "entity_id", Param.Text entityId
              "idempotency_key", Param.Text submission.IdempotencyKey
              "chart_version", Param.Int(ChartVersion.value submission.ChartVersion)
              "event", Param.Jsonb encodedEvent
              "tenant", Param.Text(CommandMapping.audit submission.Audit.Tenant)
              "principal", Param.Text(CommandMapping.audit submission.Audit.Principal)
              "source", Param.Text(CommandMapping.audit submission.Audit.Source)
              "correlation_id", Param.Text(CommandMapping.audit submission.Audit.CorrelationId)
              "causation_id", Param.Text(CommandMapping.audit submission.Audit.CausationId) ]

        let statement, values =
            match submission.Kind with
            | CommandKind.Event ->
                submitCommand,
                common
                @ [ "visible_at", Param.TimestampOrNull submission.VisibleAt
                    "received_at", Param.TimestampOrNull submission.ReceivedAt ]
            | CommandKind.Correction(at, policy) ->
                submitCorrection,
                common
                @ [ "effective_at", Param.Timestamp at
                    "on_divergence", Param.Text(EnumCodec.encode EnumCodec.divergence policy.OnDivergence)
                    "replay_limit", Param.Int policy.ReplayLimit ]

        StoreProgram.queryOne statement values (fun reader ->
            let commandId = Row.int64 reader "command_id" |> CommandId.ofInt64

            if Row.bool reader "accepted" then
                Ok(Accepted commandId)
            else
                Ok(AlreadySubmitted commandId))
        |> AtomicSubmission
        |> CommandInboxProgram.submit

    let leaseUpdate statement values =
        CommandInboxProgram.leaseUpdate (
            ReturnedOutcome(statement, values, EnumCodec.read Row.string EnumCodec.leaseOutcome "outcome")
        )

    interface ICommandInbox<'EntityId, 'Event> with

        member _.Submit(submission, ct) =
            let entityId = options.EntityIdEncode submission.EntityId

            backgroundTaskResult {
                let! encodedEvent = options.EventCodec.Encode submission.Event |> Result.mapError Db.toStoreError

                match! Sql.attemptProgram context (submit submission encodedEvent entityId) ct with
                | Db.Answered outcome -> return! outcome
                | Db.Refused(Db.ForeignKey "command_chart_version_fkey", _) ->
                    // The version this command pins was never registered, so nothing records
                    // which chart it refers to and a replay could not resolve it.
                    return!
                        Error(
                            StoreError.NotFound
                                $"chart version %d{ChartVersion.value submission.ChartVersion} of machine %s{MachineId.value submission.MachineId}"
                        )
                | Db.Refused(Db.Unique "command_unique_idem", _) ->
                    // fsm.submit_command serialises submissions per entity, so this is
                    // unreachable through the routine. It stays because the constraint, not the
                    // advisory lock, is the integrity boundary: if anything ever writes the table
                    // another way, a concurrent duplicate must still read as AlreadySubmitted
                    // rather than escaping as an error.
                    match! findExisting submission.MachineId entityId submission.IdempotencyKey ct with
                    | Some record -> return AlreadySubmitted record.CommandId
                    | None ->
                        return!
                            Error(
                                Db.decodeFailure
                                    (nameof SubmissionOutcome)
                                    "a duplicate idempotency key had no matching command"
                            )
                | Db.Refused(_, cause) -> return! Error(StoreError.Unexpected cause)
            }

        member _.Claim(machineId, batch, lease, ct) =
            if batch < 1 then
                invalidArg (nameof batch) "A claim batch size must be positive."

            if lease <= TimeSpan.Zero then
                invalidArg (nameof lease) "A lease must be a positive duration."

            // Claiming is the one call here that must not be repeated automatically. It is not
            // idempotent: a claim whose reply was lost has already leased a batch, and a retry
            // leases a second one while the first stays invisible until its lease lapses. The
            // worker polls again in a moment, which recovers sooner than a retry would.
            Sql.runProgramOnce
                context
                (StoreProgram.queryRows
                    claimCommands
                    [ "machine_id", Param.Text(MachineId.value machineId)
                      "batch", Param.Int batch
                      "lease", Param.Interval lease ]
                    leasedRow)
                ct

        member _.Reschedule(commandId, leaseToken, backoff, ct) =
            Sql.runProgram
                context
                (leaseUpdate
                    rescheduleCommand
                    [ "command_id", Param.Bigint(CommandId.value commandId)
                      "lease_token", Param.Bigint(LeaseToken.value leaseToken)
                      "base_ms", Param.Int(int (Backoff.baseDelay backoff).TotalMilliseconds)
                      "cap_ms", Param.Int(int (Backoff.ceiling backoff).TotalMilliseconds) ])
                ct

        member _.ExtendLease(commandId, leaseToken, lease, ct) =
            if lease <= TimeSpan.Zero then
                invalidArg (nameof lease) "A lease must be a positive duration."

            Sql.runProgram
                context
                (leaseUpdate
                    extendLease
                    [ "command_id", Param.Bigint(CommandId.value commandId)
                      "lease_token", Param.Bigint(LeaseToken.value leaseToken)
                      "lease", Param.Interval lease ])
                ct

        member _.TryGet(commandId, ct) =
            Sql.readProgram
                context
                (StoreProgram.tryQueryOne
                    commandById
                    [ "command_id", Param.Bigint(CommandId.value commandId) ]
                    recordRow)
                ct

        member _.TryFind(machineId, entityId, idempotencyKey, ct) =
            findExisting machineId (options.EntityIdEncode entityId) idempotencyKey ct
