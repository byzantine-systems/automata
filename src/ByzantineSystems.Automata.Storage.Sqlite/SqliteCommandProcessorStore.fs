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

/// <summary>
/// Everything the processor needs to bridge the generic domain types to the columns of
/// <c>fsm_transition</c>, <c>fsm_entity_snapshot</c>, <c>fsm_action</c> and
/// <c>fsm_command_error</c>.
/// </summary>
type CommandProcessorOptions<'EntityId, 'State, 'Event, 'Action, 'Err> =
    {
        Context: SqliteContext
        ActionDelivery: ActionDelivery
        StateCodec: Codec<'State>
        EventCodec: Codec<'Event>
        /// <summary>
        /// The whole action list as one JSON array. The transition row stores it as it is, and the
        /// commit fans it out into one queued row per element.
        /// </summary>
        ActionCodec: Codec<'Action list>
        ErrorCodec: Codec<'Err>
        EntityIdEncode: 'EntityId -> string
        /// <summary>Decoding returns a result, so a corrupt row is a typed failure rather than an exception.</summary>
        EntityIdDecode: string -> Result<'EntityId, string>
    }

/// <summary>What finalizing reads about a command before deciding anything.</summary>
type internal FinalizeTarget =
    { Machine: MachineId
      Entity: ByzantineSystems.Automata.Storage.Internal.EntityKey
      Status: CommandStatus
      Holder: LeaseToken<CommandWork>
      Version: int64 }

/// <summary>Classifies a SQLite command using the shared finalization vocabulary.</summary>
[<RequireQualifiedAccess>]
module internal LeaseStanding =

    let classify
        (held: LeaseToken<CommandWork>)
        (target: FinalizeTarget option)
        : FinalizationStanding<FinalizeTarget> =
        match target with
        | None -> Unknown
        | Some target ->
            let callerHolds = target.Holder = held

            match target.Status with
            | CommandStatus.Succeeded
            | CommandStatus.Rejected
            | CommandStatus.DeadLettered ->
                if callerHolds then
                    FinishedByCaller target.Status
                else
                    FinishedByAnother
            | CommandStatus.Ready -> HeldByAnother
            | CommandStatus.Leased -> if callerHolds then HeldByCaller target else HeldByAnother

/// <summary>
/// The SQLite end of processing a command.
///
/// Every operation is one write transaction, and it follows <c>fsm.finalize_command</c> in the
/// PostgreSQL store step for step: the transition, the snapshot, the queued actions, the result
/// and the inbox row move together or not at all. Retrying after an ambiguous failure is safe,
/// because a repeated call from the same lease is answered with what that lease already did:
///
/// <code>
/// stored result | lease token | outcome
/// none          | current     | finalized
/// exists        | this one    | already_finalized
/// exists        | another     | lease_lost
/// none          | stale       | lease_lost
/// </code>
///
/// Corrections and replay are not offered: this store keeps the current state rather than a
/// bitemporal history, so there is no past to rewrite. The runtime sees the capability absent
/// and dead-letters a correction, which still releases its entity.
/// </summary>
type SqliteCommandProcessorStore<'EntityId, 'State, 'Event, 'Action, 'Err>
    (options: CommandProcessorOptions<'EntityId, 'State, 'Event, 'Action, 'Err>) =

    let context = options.Context

    let encodeEntityId entityId =
        let key =
            options.EntityIdEncode entityId
            |> ByzantineSystems.Automata.Storage.Internal.EntityKey.ofString

        Tap.requireEntityKey "entityId" key
        key

    let statement domain operation =
        StatementKey.create domain operation |> Sql.prepareProgram

    let finalizeRead = statement "command" "finalize_read"
    let committedEpoch = statement "command" "committed_epoch"
    let currentEpochOf = statement "command" "current_epoch"
    let insertTransition = statement "command" "insert_transition"
    let upsertSnapshot = statement "command" "upsert_snapshot"
    let enqueueActions = statement "action" "enqueue"
    let insertError = statement "command" "insert_error"
    let closeCommand = statement "command" "close"
    let promoteHead = statement "command" "promote_head"

    let entitySnapshot = StatementKey.create "command" "snapshot" |> Sql.prepareProgram

    let historyPage = StatementKey.create "command" "history_page" |> Sql.prepareProgram

    let commandResult = StatementKey.create "command" "result" |> Sql.prepareProgram

    let encodeDraft expected draft =
        DraftEncoding.encode options.StateCodec options.ActionCodec (Some expected) draft

    /// <summary>
    /// The failure column is a tagged envelope rather than the bare domain error, because the
    /// cases have to be told apart on the way back out. A domain error is the application's own
    /// value, encoded by its codec; a machine reason and a replay error are this library's.
    /// </summary>
    let encodeFailure failure =
        FailureEncoding.encode options.ErrorCodec failure

    /// The exited and entered paths, stored as JSON arrays of state ids.
    let encodePath (states: StateId list) : string =
        states |> List.map StateId.value |> JsonSerializer.Serialize

    let decodePath (column: string) (json: string) : Result<StateId list, StoreError> =
        try
            match JsonSerializer.Deserialize<string array> json with
            | null -> Error(Db.decodeFailure "StateId list" $"{column} was null")
            | ids ->
                ids
                |> Array.toList
                |> List.traverseResultM (fun value ->
                    StateId.tryCreate value |> Result.mapError (Db.decodeFailure "StateId"))
        with :? JsonException as error ->
            Error(StoreError.Serialization("StateId list", error))

    /// <summary>
    /// A committed transition's columns, read into the generated record by name. Building the
    /// record here, where every field is required, is what turns a column added to the table
    /// into a compile error in this reader rather than a value it silently leaves out. Shared
    /// by the history page and the result read, so one row shape has one reader.
    /// </summary>
    let transitionRow (reader: SqliteDataReader) : main.fsm_transition * string =
        let row: main.fsm_transition =
            { machine_id = Row.string reader "machine_id"
              entity_id = Row.string reader "entity_id"
              epoch = Row.int64 reader "epoch"
              command_id = Row.int64 reader "command_id"
              chart_version = Row.int64 reader "chart_version"
              actions = Row.string reader "actions"
              from_state = Row.string reader "from_state"
              to_state = Row.string reader "to_state"
              handled_by = Row.string reader "handled_by"
              exited = Row.string reader "exited"
              entered = Row.string reader "entered"
              status = Row.string reader "status"
              effective_at = Row.int64 reader "effective_at"
              committed_at = Row.int64 reader "committed_at" }

        row, Row.string reader "event"

    let toTransition
        ((row, eventJson): main.fsm_transition * string)
        : Result<CommittedTransition<'EntityId, 'State, 'Event, 'Action>, StoreError> =
        let decode (codec: Codec<'T>) (json: string) =
            codec.Decode json |> Result.mapError Db.toStoreError

        result {
            let! entityId =
                row.entity_id
                |> options.EntityIdDecode
                |> Result.mapError (Db.decodeFailure "EntityId")

            let! event = decode options.EventCodec eventJson
            let! actions = decode options.ActionCodec row.actions
            let! fromState = decode options.StateCodec row.from_state
            let! toState = decode options.StateCodec row.to_state
            let! status = EnumCodec.decode EnumCodec.instanceStatus row.status
            let! exited = decodePath "exited" row.exited
            let! entered = decodePath "entered" row.entered

            let! chartVersion =
                int row.chart_version
                |> ChartVersion.tryCreate
                |> Result.mapError (Db.decodeFailure "ChartVersion")

            let! machineId =
                row.machine_id
                |> MachineId.tryCreate
                |> Result.mapError (Db.decodeFailure "MachineId")

            let! handledBy =
                row.handled_by
                |> StateId.tryCreate
                |> Result.mapError (Db.decodeFailure "StateId")

            return
                { Draft =
                    { MachineId = machineId
                      EntityId = entityId
                      Event = event
                      Actions = actions
                      FromState = fromState
                      ToState = toState
                      HandledBy = handledBy
                      Exited = exited
                      Entered = entered
                      Status = status
                      EffectiveAt = Instant.toDateTimeOffset row.effective_at }
                  Epoch = Db.epochOf row.epoch
                  CommandId = CommandId.ofInt64 row.command_id
                  ChartVersion = chartVersion
                  CommittedAt = Instant.toDateTimeOffset row.committed_at }
        }

    let snapshotRow (reader: SqliteDataReader) : Result<Snapshot<'State>, StoreError> =
        result {
            let! state =
                options.StateCodec.Decode(Row.string reader "state")
                |> Result.mapError Db.toStoreError

            let! status = EnumCodec.read Row.string EnumCodec.instanceStatus "status" reader

            return
                { State = state
                  Epoch = Db.epochOf (Row.int64 reader "epoch")
                  Status = status }
        }

    /// What became of one command, from its status, its transition if it committed, and its
    /// error if it failed.
    let resultRow
        (reader: SqliteDataReader)
        : Result<CommandResult<'EntityId, 'State, 'Event, 'Action, 'Err>, StoreError> =
        let transition =
            let marker = reader.GetOrdinal "command_id"

            if reader.IsDBNull marker then
                None
            else
                Some(transitionRow reader)

        let failure () =
            let optional column =
                if reader.IsDBNull(reader.GetOrdinal column) then
                    None
                else
                    Some(Row.string reader column)

            FailureEncoding.decode
                options.ErrorCodec
                { Kind = optional "failure_kind"
                  Domain = optional "domain_error"
                  Machine = optional "machine_reason"
                  Replay =
                    optional "replay_kind"
                    |> Option.map (fun kind ->
                        kind,
                        int (Row.int64 reader "failure_version"),
                        Row.int64 reader "failure_epoch",
                        Row.string reader "replay_reason",
                        int (Row.int64 reader "failure_budget")) }

        EnumCodec.read Row.string EnumCodec.commandStatus "command_status" reader
        |> Result.bind (fun status ->
            match status, transition with
            | CommandStatus.Ready, _
            | CommandStatus.Leased, _ -> Ok CommandResult.Pending
            | CommandStatus.Succeeded, Some transition -> toTransition transition |> Result.map CommandResult.Committed
            | CommandStatus.Succeeded, None ->
                Error(Db.decodeFailure (nameof CommandResult) "a succeeded command has no transition")
            | CommandStatus.Rejected, _ -> failure () |> Result.map CommandResult.Rejected
            | CommandStatus.DeadLettered, _ -> failure () |> Result.map CommandResult.DeadLettered)

    let entityOf (target: FinalizeTarget) =
        [ Params.machineId "@machine_id" target.Machine
          Params.entityKey "@entity_id" target.Entity ]

    let finalizeTargetRow reader =
        result {
            let! status = EnumCodec.read Row.string EnumCodec.commandStatus "status" reader

            let! machine =
                Row.string reader "machine_id"
                |> MachineId.tryCreate
                |> Result.mapError (Db.decodeFailure "MachineId")

            return
                { Machine = machine
                  Entity =
                    Row.string reader "entity_id"
                    |> ByzantineSystems.Automata.Storage.Internal.EntityKey.ofString
                  Status = status
                  Holder = Row.int64 reader "lease_token" |> LeaseToken.ofInt64
                  Version = Row.int64 reader "chart_version" }
        }

    /// The entity's current epoch: the initial one when it has never committed.
    let currentEpoch
        (target: FinalizeTarget)
        : StoreProgram<WriteAccess, (string * Param) list, SqliteDataReader, Epoch> =
        StoreProgram.tryQueryOne currentEpochOf (entityOf target) (fun reader ->
            Row.int64 reader "epoch" |> Db.epochOf |> Ok)
        |> StoreProgram.map (Option.defaultValue Epoch.initial)

    /// <summary>
    /// A commit's writes, in order: the transition, then the snapshot, then the queued actions.
    /// The transition goes first so a snapshot that cannot be written leaves no transition
    /// claiming it did, and the actions share the transaction, which is the whole outbox.
    /// </summary>
    let append
        (now: int64)
        (commandId: CommandId)
        (target: FinalizeTarget)
        (epoch: Epoch)
        (encoded: EncodedDraft<'EntityId, 'State, 'Event, 'Action>)
        : StoreProgram<WriteAccess, (string * Param) list, SqliteDataReader, unit> =
        store {
            let draft = encoded.Source
            let entity = entityOf target

            let written =
                entity
                @ [ Params.commandId "@command_id" commandId
                    "@epoch", Param.Integer(int64 (Epoch.value epoch))
                    "@chart_version", Param.Integer target.Version
                    "@status", Param.Text(EnumCodec.encode EnumCodec.instanceStatus draft.Status)
                    "@effective_at", Param.Integer(Instant.ofDateTimeOffset draft.EffectiveAt)
                    "@now", Param.Integer now ]

            do!
                StoreProgram.executeDiscard
                    insertTransition
                    (written
                     @ [ "@actions", Param.Text encoded.ActionsJson
                         "@from_state", Param.TextOrNull(CommitOrigin.payload encoded.Origin)
                         "@to_state", Param.Text encoded.ToJson
                         "@handled_by", Param.Text(StateId.value draft.HandledBy)
                         "@exited", Param.Text(encodePath draft.Exited)
                         "@entered", Param.Text(encodePath draft.Entered) ])

            do! StoreProgram.executeDiscard upsertSnapshot (written @ [ "@state", Param.Text encoded.ToJson ])

            match options.ActionDelivery with
            | ActionDelivery.Discard -> ()
            | ActionDelivery.Deliver _ ->
                do!
                    StoreProgram.executeDiscard
                        enqueueActions
                        (entity
                         @ [ Params.commandId "@command_id" commandId
                             "@epoch", Param.Integer(int64 (Epoch.value epoch))
                             "@actions", Param.Text encoded.ActionsJson
                             "@now", Param.Integer now ])
        }

    /// The encoded failure of a rejected or dead-lettered command.
    let recordError
        (now: int64)
        (commandId: CommandId)
        (error: EncodedFailure)
        : StoreProgram<WriteAccess, (string * Param) list, SqliteDataReader, unit> =
        let fields = FailureEncoding.fields error

        StoreProgram.executeDiscard
            insertError
            [ Params.commandId "@command_id" commandId
              "@kind", Param.Text fields.Kind
              "@domain", Param.Text fields.Domain
              "@reason", Param.Text fields.Reason
              "@replay_kind", Param.Text fields.ReplayKind
              "@version", Param.Integer(int64 fields.Version)
              "@failure_epoch", Param.Integer fields.Epoch
              "@budget", Param.Integer(int64 fields.Budget)
              "@now", Param.Integer now ]

    /// <summary>
    /// Closes the command at its terminal status and promotes the entity's next command to head.
    /// Promotion runs after the close, so the command just finished is no longer open and cannot
    /// be re-elected.
    /// </summary>
    let close
        (commandId: CommandId)
        (target: FinalizeTarget)
        (resolution: Resolution<'EntityId, 'State, 'Event, 'Action>)
        : StoreProgram<WriteAccess, (string * Param) list, SqliteDataReader, unit> =
        store {
            do!
                StoreProgram.executeDiscard
                    closeCommand
                    [ Params.commandId "@command_id" commandId
                      "@status", Param.Text(EnumCodec.encode EnumCodec.commandStatus (Resolution.status resolution)) ]

            do! StoreProgram.executeDiscard promoteHead (entityOf target)
        }

    /// <summary>
    /// The writes of a finalize the caller holds the lease for.
    ///
    /// The epoch check guards the state write, so only a commit makes it. It is a backstop rather
    /// than the mechanism: the head invariant already stops a second command for this entity
    /// being processed while this one is leased. What it catches is a writer outside that path.
    /// </summary>
    let resolve
        (now: int64)
        (commandId: CommandId)
        (target: FinalizeTarget)
        (resolution: Resolution<'EntityId, 'State, 'Event, 'Action>)
        : StoreProgram<WriteAccess, (string * Param) list, SqliteDataReader, FinalizeOutcome> =
        store {
            match resolution with
            | Commit(expected, encoded) ->
                let! current = currentEpoch target

                if current <> expected then
                    return Conflict(expected, current)
                else
                    let committed = Epoch.next expected
                    do! append now commandId target committed encoded
                    do! close commandId target resolution
                    return Finalized committed
            | Reject error
            | DeadLetter error ->
                do! recordError now commandId error
                do! close commandId target resolution
                return Finalized(Resolution.unmoved resolution)
        }

    /// <summary>
    /// One finalize: read where the caller stands, then write only if it holds the lease. Every
    /// answer other than a missing command is an outcome rather than an error, so the transaction
    /// commits whether or not it wrote anything.
    /// </summary>
    let finalize
        (commandId: CommandId)
        (token: LeaseToken<CommandWork>)
        (resolution: Resolution<'EntityId, 'State, 'Event, 'Action>)
        (ct: CancellationToken)
        : Task<Result<FinalizeOutcome, StoreError>> =
        Sql.writeProgramWith
            context
            (fun now ->
                let standing =
                    store {
                        let! read =
                            StoreProgram.tryQueryOne
                                finalizeRead
                                [ Params.commandId "@command_id" commandId ]
                                finalizeTargetRow

                        return LeaseStanding.classify token read
                    }

                let already status =
                    store {
                        let! committed =
                            StoreProgram.tryQueryOne
                                committedEpoch
                                [ Params.commandId "@command_id" commandId ]
                                (fun reader -> Row.int64 reader "epoch" |> Db.epochOf |> Ok)

                        return
                            AlreadyFinalized(committed |> Option.defaultValue (Resolution.unmoved resolution), status)
                    }

                FinalizeProgram.finish (
                    CheckedFinalization(
                        standing,
                        already,
                        (fun target -> resolve now commandId target resolution),
                        StoreError.NotFound $"command %d{CommandId.value commandId}"
                    )
                ))
            ct

    /// <summary>
    /// A rejection or a dead letter. The failure is encoded before the write begins, so the
    /// transaction only writes.
    /// </summary>
    let fail (resolve: EncodedFailure -> Resolution<'EntityId, 'State, 'Event, 'Action>) commandId token failure ct =
        backgroundTaskResult {
            let! encoded = encodeFailure failure
            return! finalize commandId token (resolve encoded) ct
        }

    interface IStateReader<'EntityId, 'State, 'Event, 'Action> with

        member _.TryGetSnapshotAsync(machineId, entityId, cancellationToken) =
            Tap.requireMachineId (nameof machineId) machineId
            Tap.requireArg (nameof entityId) entityId

            Tap.start cancellationToken (fun () ->
                Sql.readProgram
                    context
                    (StoreProgram.tryQueryOne
                        entitySnapshot
                        [ Params.machineId "@machine_id" machineId
                          Params.entityKey "@entity_id" (encodeEntityId entityId) ]
                        snapshotRow)
                    cancellationToken)

        member _.HistoryAsync(machineId, entityId, paging, cancellationToken) =
            Tap.requireMachineId (nameof machineId) machineId
            Tap.requireArg (nameof entityId) entityId

            Tap.start cancellationToken (fun () ->
                // An absent cursor is zero, which transition_epoch_positive makes smaller than any
                // stored epoch, so one query shape serves every page.
                let after =
                    Page.cursor paging
                    |> Option.map (Epoch.value >> int64)
                    |> Option.defaultValue 0L

                Sql.readProgram
                    context
                    (StoreProgram.queryRows
                        historyPage
                        [ Params.machineId "@machine_id" machineId
                          Params.entityKey "@entity_id" (encodeEntityId entityId)
                          "@after", Param.Integer after
                          "@limit", Param.Integer(int64 (Page.limit paging)) ]
                        (transitionRow >> toTransition))
                    cancellationToken)

    interface ICommandProcessorStore<'EntityId, 'State, 'Event, 'Action, 'Err> with

        member _.CommitAsync(commandId, token, expected, draft, cancellationToken) =
            Tap.requireArg (nameof commandId) commandId
            Tap.requireArg (nameof token) token
            Tap.requireArg (nameof draft) draft

            // Encoded before the write begins: one file has one writer, so codec work inside the
            // transaction would be time every machine sharing the file waits for.
            Tap.start cancellationToken (fun () ->
                backgroundTaskResult {
                    let! encoded = encodeDraft expected draft
                    return! finalize commandId token (Commit(expected, encoded)) cancellationToken
                })

        member _.RejectAsync(commandId, token, error, cancellationToken) =
            Tap.requireArg (nameof commandId) commandId
            Tap.requireArg (nameof token) token
            Tap.requireArg (nameof error) error

            Tap.start cancellationToken (fun () -> fail Reject commandId token error cancellationToken)

        member _.DeadLetterAsync(commandId, token, error, cancellationToken) =
            Tap.requireArg (nameof commandId) commandId
            Tap.requireArg (nameof token) token
            Tap.requireArg (nameof error) error

            Tap.start cancellationToken (fun () -> fail DeadLetter commandId token error cancellationToken)

        member _.TryGetResultAsync(commandId, cancellationToken) =
            Tap.requireArg (nameof commandId) commandId

            Tap.start cancellationToken (fun () ->
                Sql.readProgram
                    context
                    (StoreProgram.tryQueryOne commandResult [ Params.commandId "@command_id" commandId ] resultRow)
                    cancellationToken)
