namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Internal
open ByzantineSystems.Automata.Storage.Postgres.Schema
open FsToolkit.ErrorHandling
open Npgsql

/// <summary>
/// Everything the processor needs to bridge the generic domain types to the columns of
/// <c>fsm.transition</c>, <c>fsm.instance_state</c> and <c>fsm.command_error</c>.
/// </summary>
type CommandProcessorOptions<'EntityId, 'State, 'Event, 'Action, 'Err> =
    {
        Context: PostgresContext
        StateCodec: Codec<'State>
        EventCodec: Codec<'Event>
        ActionCodec: Codec<'Action list>
        ErrorCodec: Codec<'Err>
        EntityIdEncode: 'EntityId -> string
        /// <summary>Decoding returns a result, so a corrupt row is a typed failure rather than an exception.</summary>
        EntityIdDecode: string -> Result<'EntityId, string>
    }

/// <summary>Translations between the processor's domain types and their column representations.</summary>
[<RequireQualifiedAccess>]
module private ProcessorMapping =

    type FinalizeTag =
        | Finalized
        | AlreadyFinalized
        | Conflict
        | LeaseLost

    let finalizeTag =
        EnumCodec.create "finalize outcome" [ Finalized; AlreadyFinalized; Conflict; LeaseLost ] (function
            | Finalized -> "finalized"
            | AlreadyFinalized -> "already_finalized"
            | Conflict -> "conflict"
            | LeaseLost -> "lease_lost")

    let outcome (tag: FinalizeTag) (epoch: int64) (status: CommandStatus) (expected: Epoch) : FinalizeOutcome =
        match tag with
        | Finalized -> FinalizeOutcome.Finalized(Db.epochOf epoch)
        | AlreadyFinalized -> FinalizeOutcome.AlreadyFinalized(Db.epochOf epoch, status)
        | Conflict -> FinalizeOutcome.Conflict(expected, Db.epochOf epoch)
        | LeaseLost -> FinalizeOutcome.LeaseLost

/// <summary>
/// The PostgreSQL end of processing a command.
///
/// Every operation is one call to the profile commit routine, which is one transaction: the
/// transition, the state, the result and the inbox row move together or not at all. Retrying
/// after an ambiguous failure is safe, because the routine answers a repeated call from the same
/// lease with what that lease already did.
/// </summary>
type PostgresCommandProcessorStore<'EntityId, 'State, 'Event, 'Action, 'Err>
    (options: CommandProcessorOptions<'EntityId, 'State, 'Event, 'Action, 'Err>, ?beliefs: BeliefModel) =

    let context = options.Context

    let encodeEntityId entityId =
        let key =
            options.EntityIdEncode entityId
            |> ByzantineSystems.Automata.Storage.Internal.EntityKey.ofString

        Tap.requireEntityKey "entityId" key
        key

    let prepare = Sql.prepareProgram context.Schema

    let finalizeCommand =
        match defaultArg beliefs BeliefModel.Snapshot with
        | BeliefModel.Snapshot -> StatementKey.create "command" "finalize" |> prepare
        | BeliefModel.Bitemporal -> StatementKey.create "command" "finalize_temporal" |> prepare

    let finalizeCorrection =
        StatementKey.create "command" "finalize_correction" |> prepare

    let finalizeFailure = StatementKey.create "command" "finalize_failure" |> prepare

    let currentBelief = StatementKey.create "belief" "current" |> prepare

    let historyPage = StatementKey.create "transition" "history_page" |> prepare

    let transitionResult = StatementKey.create "transition" "result" |> prepare

    let transitionSuffix = StatementKey.create "transition" "suffix" |> prepare

    let transitionWatermark =
        StatementKey.create "transition" "purge_watermark" |> prepare

    let encodeDraft expected draft =
        DraftEncoding.encode options.StateCodec options.ActionCodec expected draft

    let states (ids: StateId list) : string list = ids |> List.map StateId.value

    let finalizeOutcome expected reader =
        result {
            let! tag = EnumCodec.read Row.string ProcessorMapping.finalizeTag "outcome" reader
            let! status = EnumCodec.read Row.string EnumCodec.commandStatus "status" reader
            return ProcessorMapping.outcome tag (Row.int64 reader "epoch") status expected
        }

    /// <summary>
    /// One call to the profile commit routine. Which half of its parameters is filled follows
    /// from how the command ends, so the two shapes cannot be half-filled or mixed: a commit
    /// sends its transition and no error, a failure sends its error and no transition.
    ///
    /// A failure writes no state, so there is nothing for a stale epoch to conflict with; it
    /// sends the initial epoch and the routine skips the check.
    /// </summary>
    let finalize
        (commandId: CommandId)
        (token: LeaseToken<CommandWork>)
        (resolution: Resolution<'EntityId, 'State, 'Event, 'Action>)
        : StoreProgram<WriteAccess, (string * Param) list, NpgsqlDataReader, FinalizeOutcome> =
        let expected = Resolution.unmoved resolution

        let ending =
            match resolution with
            | Commit(_, encoded) ->
                let draft = encoded.Source

                [ "error", Param.JsonbOrNull None
                  "instance_status", Param.TextOrNull(Some(EnumCodec.encode EnumCodec.instanceStatus draft.Status))
                  "effective_at", Param.TimestampOrNull(Some draft.EffectiveAt)
                  "actions", Param.JsonbOrNull(Some encoded.ActionsJson)
                  "from_state", Param.JsonbOrNull(CommitOrigin.payload encoded.Origin)
                  "to_state", Param.JsonbOrNull(Some encoded.ToJson)
                  "handled_by", Param.TextOrNull(Some(StateId.value draft.HandledBy))
                  "exited", Param.TextArrayOrNull(Some(states draft.Exited))
                  "entered", Param.TextArrayOrNull(Some(states draft.Entered)) ]
            | Reject _
            | DeadLetter _ -> invalidOp "Failures use the relational failure program."

        StoreProgram.queryOne
            finalizeCommand
            ([ Params.commandId "command_id" commandId
               Params.commandLease "lease_token" token
               "expected_epoch", Param.Bigint(int64 (Epoch.value expected))
               "status", Param.Text(EnumCodec.encode EnumCodec.commandStatus (Resolution.status resolution)) ]
             @ ending)
            (finalizeOutcome expected)
        |> AtomicFinalization
        |> FinalizeProgram.finish

    /// <summary>
    /// The failure column is a tagged envelope rather than the bare domain error, because the
    /// two cases have to be told apart on the way back out. A domain error is the
    /// application's own value and is encoded by its codec; a machine reason is a string this
    /// library wrote. Reading a row and guessing which one it holds is not possible once they
    /// share a column, so the tag goes in.
    /// </summary>
    let encodeFailure failure =
        FailureEncoding.encode options.ErrorCodec failure

    /// A rejection or a dead letter, encoded before anything is sent.
    let fail
        (resolve: EncodedFailure -> Resolution<'EntityId, 'State, 'Event, 'Action>)
        (commandId: CommandId)
        token
        (failure: CommandFailure<'Err>)
        ct
        =
        backgroundTaskResult {
            let! encoded = encodeFailure failure
            let fields = FailureEncoding.fields encoded

            return!
                Sql.runProgram
                    context
                    (StoreProgram.queryOne
                        finalizeFailure
                        [ Params.commandId "command_id" commandId
                          Params.commandLease "lease_token" token
                          "status",
                          Param.Text(EnumCodec.encode EnumCodec.commandStatus (Resolution.status (resolve encoded)))
                          "kind", Param.Text fields.Kind
                          "domain", Param.Jsonb fields.Domain
                          "reason", Param.Text fields.Reason
                          "replay_kind", Param.Text fields.ReplayKind
                          "version", Param.Int fields.Version
                          "failure_epoch", Param.Bigint fields.Epoch
                          "budget", Param.Int fields.Budget ]
                        (finalizeOutcome Epoch.initial))
                    ct
        }

    /// <summary>
    /// A committed transition's columns, read into the generated record by name. Building the
    /// record here, where every field is required, is what turns a column added to the table
    /// into a compile error in this reader rather than a value it silently leaves out. Shared
    /// by the history page, the result read and the replay suffix, so one row shape has one
    /// reader.
    /// </summary>
    let transitionRow (reader: NpgsqlDataReader) : fsm.transition * string =
        let row: fsm.transition =
            { machine_id = Row.string reader "machine_id"
              entity_id = Row.string reader "entity_id"
              epoch = Row.int64 reader "epoch"
              command_id = Row.int64 reader "command_id"
              chart_version = Row.int32 reader "chart_version"
              actions = Row.string reader "actions"
              from_state = Row.string reader "from_state"
              to_state = Row.string reader "to_state"
              handled_by = Row.string reader "handled_by"
              exited = reader.GetFieldValue<string[]>(reader.GetOrdinal "exited")
              entered = reader.GetFieldValue<string[]>(reader.GetOrdinal "entered")
              status = Row.string reader "status"
              effective_at = reader.GetDateTime(reader.GetOrdinal "effective_at")
              committed_at = reader.GetDateTime(reader.GetOrdinal "committed_at") }

        row, Row.string reader "event"

    /// Rebuilds a committed transition from its row. Seven decodes can fail independently, and
    /// none of them is expected: each means the row is not what this code was compiled for.
    let toTransition
        ((row, eventJson): fsm.transition * string)
        : Result<CommittedTransition<'EntityId, 'State, 'Event, 'Action>, StoreError> =
        let decode (codec: Codec<'T>) (json: string) =
            codec.Decode json |> Result.mapError Db.toStoreError

        let decodeStateIds (values: string[]) : Result<StateId list, StoreError> =
            values
            |> Array.toList
            |> List.traverseResultM (fun value ->
                StateId.tryCreate value |> Result.mapError (Db.decodeFailure "StateId"))

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
            let! exited = decodeStateIds row.exited
            let! entered = decodeStateIds row.entered

            let! chartVersion =
                row.chart_version
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
                      EffectiveAt = Db.fromTimestamp row.effective_at }
                  Epoch = Db.epochOf row.epoch
                  CommandId = CommandId.ofInt64 row.command_id
                  ChartVersion = chartVersion
                  CommittedAt = Db.fromTimestamp row.committed_at }
        }

    /// The current belief, from the view's nullable columns. The view cannot carry NOT NULL, so
    /// each column is required here instead of assumed.
    let snapshotRow (reader: NpgsqlDataReader) : Result<Snapshot<'State>, StoreError> =
        let optional (read: int -> _) (column: string) =
            let ordinal = reader.GetOrdinal column

            if reader.IsDBNull ordinal then None else Some(read ordinal)

        result {
            let! state = Db.required "Snapshot" "state" (optional reader.GetString "state")

            let! status = Db.required "Snapshot" "status" (optional reader.GetString "status")

            let! epoch = Db.required "Snapshot" "epoch" (optional reader.GetInt64 "epoch")
            let! state = options.StateCodec.Decode state |> Result.mapError Db.toStoreError
            let! status = EnumCodec.decode EnumCodec.instanceStatus status

            return
                { State = state
                  Epoch = Db.epochOf epoch
                  Status = status }
        }

    /// What became of one command, from its status, its transition if it committed, and its
    /// error if it failed.
    let resultRow
        (reader: NpgsqlDataReader)
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
                        Row.int32 reader "failure_version",
                        Row.int64 reader "failure_epoch",
                        Row.string reader "replay_reason",
                        Row.int32 reader "failure_budget") }

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

    interface IStateReader<'EntityId, 'State, 'Event, 'Action> with

        member _.TryGetSnapshotAsync(machineId, entityId, cancellationToken) =
            Tap.requireMachineId (nameof machineId) machineId
            Tap.requireArg (nameof entityId) entityId

            Tap.start cancellationToken (fun () ->
                Sql.readProgram
                    context
                    (StoreProgram.tryQueryOne
                        currentBelief
                        [ Params.machineId "machine_id" machineId
                          Params.entityKey "entity_id" (encodeEntityId entityId) ]
                        snapshotRow)
                    cancellationToken)

        member _.HistoryAsync(machineId, entityId, paging, cancellationToken) =
            Tap.requireMachineId (nameof machineId) machineId
            Tap.requireArg (nameof entityId) entityId

            Tap.start cancellationToken (fun () ->
                // An absent cursor is zero rather than a null, because transition_epoch_positive
                // forbids a stored epoch of zero. One query shape then serves every page.
                let after =
                    Page.cursor paging
                    |> Option.map (Epoch.value >> int64)
                    |> Option.defaultValue 0L

                Sql.readProgram
                    context
                    (StoreProgram.queryRows
                        historyPage
                        [ Params.machineId "machine_id" machineId
                          Params.entityKey "entity_id" (encodeEntityId entityId)
                          "after", Param.Bigint after
                          "limit", Param.Int(Page.limit paging) ]
                        (transitionRow >> toTransition))
                    cancellationToken)

    interface ICommandProcessorStore<'EntityId, 'State, 'Event, 'Action, 'Err> with

        member _.CommitAsync(commandId, token, expected, draft, cancellationToken) =
            Tap.requireArg (nameof commandId) commandId
            Tap.requireArg (nameof token) token
            Tap.requireArg (nameof draft) draft

            Tap.start cancellationToken (fun () ->
                backgroundTaskResult {
                    let! encoded = encodeDraft (Some expected) draft

                    return!
                        Sql.runProgram context (finalize commandId token (Commit(expected, encoded))) cancellationToken
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
                    (StoreProgram.tryQueryOne transitionResult [ Params.commandId "command_id" commandId ] resultRow)
                    cancellationToken)

    interface ITransitionLog<'EntityId, 'State, 'Event, 'Action> with

        member _.SuffixAsync(machineId, entityId, from, limit, cancellationToken) =
            Tap.requireMachineId (nameof machineId) machineId
            Tap.requireArg (nameof entityId) entityId

            Tap.start cancellationToken (fun () ->
                // Encoded before the program is built, so a null entity id is a usage error
                // at the call site rather than a failure inside the read: the builder defers
                // everything it is handed.
                let key = encodeEntityId entityId

                Sql.readProgram
                    context
                    (store {
                        let! suffix =
                            StoreProgram.queryRows
                                transitionSuffix
                                [ Params.machineId "machine_id" machineId
                                  Params.entityKey "entity_id" key
                                  "from", Param.Timestamp from
                                  "limit", Param.Int limit ]
                                (transitionRow >> toTransition)

                        // Retention purges an entity's oldest commands with their transitions, and
                        // records the greatest effective instant it removed as a watermark. A suffix
                        // whose start is at or before that watermark may have lost events; one that
                        // starts strictly after it has not. The suffix is read first, so a concurrent
                        // purge cannot pair a post-purge suffix with a stale pre-purge watermark.
                        let! watermark =
                            StoreProgram.tryQueryOne
                                transitionWatermark
                                [ Params.machineId "machine_id" machineId; Params.entityKey "entity_id" key ]
                                (fun reader -> Row.timestamp reader "max_purged_effective_at" |> Ok)

                        let incomplete =
                            match watermark with
                            | Some maxPurged -> maxPurged >= from
                            | None -> false

                        return
                            { Transitions = suffix
                              Incomplete = incomplete }
                    })
                    cancellationToken)

    interface ICorrectionCommitter<'EntityId, 'State, 'Event, 'Action> with

        member _.CommitCorrectionAsync(commandId, token, expected, commit, cancellationToken) =
            Tap.requireArg (nameof commandId) commandId
            Tap.requireArg (nameof token) token
            Tap.requireArg (nameof commit) commit

            Tap.start cancellationToken (fun () ->
                backgroundTaskResult {
                    // Everything is encoded before anything is sent, so a state that will not encode
                    // fails the correction rather than half-writing it.
                    let! encoded = encodeDraft None commit.Transition
                    let head, tail = commit.Beliefs
                    let! beliefs = BeliefBatch.encode options.StateCodec (head :: tail)
                    let draft = commit.Transition

                    return!
                        Sql.runProgram
                            context
                            (StoreProgram.queryOne
                                finalizeCorrection
                                ([ Params.commandId "command_id" commandId
                                   Params.commandLease "lease_token" token
                                   "expected_epoch", Param.Bigint(int64 (Epoch.value expected))
                                   "valid_from", Param.Timestamp commit.ValidFrom
                                   "instance_status",
                                   Param.Text(EnumCodec.encode EnumCodec.instanceStatus draft.Status)
                                   "from_state", Param.JsonbOrNull(CommitOrigin.payload encoded.Origin)
                                   "to_state", Param.Jsonb encoded.ToJson
                                   "handled_by", Param.Text(StateId.value draft.HandledBy)
                                   "exited", Param.TextArray(states draft.Exited)
                                   "entered", Param.TextArray(states draft.Entered) ]
                                 @ BeliefBatch.parameters beliefs)
                                (finalizeOutcome expected))
                            cancellationToken
                })
