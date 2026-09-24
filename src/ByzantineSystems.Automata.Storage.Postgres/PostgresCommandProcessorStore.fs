namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Postgres.Schema
open FsToolkit.ErrorHandling
open Npgsql
open NpgsqlTypes
open SqlHydra.Query

/// <summary>
/// Everything the processor needs to bridge the generic domain types to the columns of
/// <c>fsm.transition</c>, <c>fsm.instance_state</c> and <c>fsm.command_error</c>.
/// </summary>
type CommandProcessorOptions<'EntityId, 'State, 'Event, 'Action, 'Err> =
    {
        Context: PostgresContext
        /// <summary>
        /// The queue a commit enqueues its actions into, inside the commit's own transaction.
        /// The empty string means this machine delivers no actions, which is how a chart that
        /// emits none avoids requiring a queue to exist.
        /// </summary>
        ActionQueue: string
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

    let outcomeFromString (value: string) (epoch: int64) (expected: Epoch) : Result<FinalizeOutcome, StoreError> =
        match value with
        | "finalized" -> Ok(Finalized(Db.epochOf epoch))
        | "already_finalized" -> Ok(AlreadyFinalized(Db.epochOf epoch))
        | "conflict" -> Ok(Conflict(expected, Db.epochOf epoch))
        | "lease_lost" -> Ok FinalizeOutcome.LeaseLost
        | other -> Error(Db.decodeFailure (nameof FinalizeOutcome) $"unknown finalize outcome {other}")

/// <summary>
/// The PostgreSQL end of processing a command.
///
/// Every operation is one call to <c>fsm.finalize_command</c>, which is one transaction: the
/// transition, the belief, the result and the inbox row move together or not at all. Retrying
/// after an ambiguous failure is safe, because the routine answers a repeated call from the same
/// lease with what that lease already did.
/// </summary>
type PostgresCommandProcessorStore<'EntityId, 'State, 'Event, 'Action, 'Err>
    (options: CommandProcessorOptions<'EntityId, 'State, 'Event, 'Action, 'Err>) =

    let dataSource = options.Context.DataSource

    /// Every statement goes through the context's pipeline, which is where transient driver
    /// failures are retried and classified. Bound once here so no call site can forget it.
    let protect work ct =
        Db.protect options.Context.Resilience work ct

    let addText (name: string) (value: string) (cmd: NpgsqlCommand) =
        cmd.Parameters.AddWithValue(name, value) |> ignore

    let addBigint (name: string) (value: int64) (cmd: NpgsqlCommand) =
        cmd.Parameters.AddWithValue(name, value) |> ignore

    /// An absent value is sent as a typed NULL. The routine rejects the combinations that make no
    /// sense, so a half-filled shape fails loudly rather than being written.
    let addNullable (name: string) (dbType: NpgsqlDbType) (value: obj option) (cmd: NpgsqlCommand) =
        let parameter = NpgsqlParameter(name, dbType)

        parameter.Value <-
            match value with
            | Some value -> value
            | None -> box DBNull.Value

        cmd.Parameters.Add parameter |> ignore

    /// What a committed finalize sends beyond the command's identity: the draft and its four
    /// encoded payloads. Kept together so the two shapes of the call cannot be half-filled.
    let encodeDraft
        (draft: TransitionDraft<'EntityId, 'State, 'Event, 'Action>)
        : Result<string * string * string * string, StoreError> =
        match
            options.EventCodec.Encode draft.Event,
            options.ActionCodec.Encode draft.Actions,
            options.StateCodec.Encode draft.FromState,
            options.StateCodec.Encode draft.ToState
        with
        | Ok event, Ok actions, Ok fromState, Ok toState -> Ok(event, actions, fromState, toState)
        | Error error, _, _, _
        | _, Error error, _, _
        | _, _, Error error, _
        | _, _, _, Error error -> Error(Db.toStoreError error)

    /// One finalize call. The three operations differ only in the status and which half of the
    /// parameters they fill, so the call is written once.
    let finalize
        (commandId: CommandId)
        (token: LeaseToken<CommandWork>)
        (expected: Epoch)
        (status: string)
        (error: string option)
        (committed: (TransitionDraft<'EntityId, 'State, 'Event, 'Action> * string * string * string * string) option)
        (ct: CancellationToken)
        : Task<Result<FinalizeOutcome, StoreError>> =
        task {
            use! conn = dataSource.OpenConnectionAsync(ct).AsTask()
            use cmd = new NpgsqlCommand(SqlResources.get "command" "finalize", conn)
            cmd |> addBigint "command_id" (CommandId.value commandId)
            cmd |> addBigint "lease_token" (LeaseToken.value token)
            cmd |> addBigint "expected_epoch" (int64 (Epoch.value expected))
            cmd |> addText "status" status
            cmd |> addText "action_queue" options.ActionQueue
            cmd |> addNullable "error" NpgsqlDbType.Jsonb (error |> Option.map box)

            let jsonb name selector =
                cmd
                |> addNullable name NpgsqlDbType.Jsonb (committed |> Option.map (selector >> box))

            jsonb "state" (fun (_, _, _, _, toState) -> toState)
            jsonb "event" (fun (_, event, _, _, _) -> event)
            jsonb "actions" (fun (_, _, actions, _, _) -> actions)
            jsonb "from_state" (fun (_, _, _, fromState, _) -> fromState)
            jsonb "to_state" (fun (_, _, _, _, toState) -> toState)

            let fromDraft name dbType selector =
                cmd
                |> addNullable name dbType (committed |> Option.map (fun (draft, _, _, _, _) -> selector draft))

            fromDraft "instance_status" NpgsqlDbType.Text (fun d -> box (Db.instanceStatusToString d.Status))

            fromDraft "effective_at" NpgsqlDbType.TimestampTz (fun d -> box (Db.timestamp d.EffectiveAt))
            fromDraft "handled_by" NpgsqlDbType.Text (fun d -> box (StateId.value d.HandledBy))

            let textArray name selector =
                cmd
                |> addNullable
                    name
                    (NpgsqlDbType.Array ||| NpgsqlDbType.Text)
                    (committed
                     |> Option.map (fun (draft, _, _, _, _) ->
                         box (selector draft |> List.map StateId.value |> List.toArray)))

            textArray "exited" (fun d -> d.Exited)
            textArray "entered" (fun d -> d.Entered)

            use! reader = cmd.ExecuteReaderAsync ct
            let! hasRow = reader.ReadAsync ct

            if not hasRow then
                return Error(Db.decodeFailure (nameof FinalizeOutcome) "finalize_command returned no row")
            else
                return
                    ProcessorMapping.outcomeFromString (Row.string reader "outcome") (Row.int64 reader "epoch") expected
        }

    /// <summary>
    /// The failure column is a tagged envelope rather than the bare domain error, because the
    /// two cases have to be told apart on the way back out. A domain error is the
    /// application's own value and is encoded by its codec; a machine reason is a string this
    /// library wrote. Reading a row and guessing which one it holds is not possible once they
    /// share a column, so the tag goes in.
    /// </summary>
    let encodeFailure (failure: CommandFailure<'Err>) : Result<string, StoreError> =
        match failure with
        | CommandFailure.Domain error ->
            options.ErrorCodec.Encode error
            |> Result.mapError Db.toStoreError
            |> Result.map (fun encoded -> $"""{{"domain":%s{encoded}}}""")
        | CommandFailure.Machine reason -> Ok $"""{{"machine":%s{JsonSerializer.Serialize reason}}}"""

    let readFailure (json: string) : Result<CommandFailure<'Err>, StoreError> =
        // The column is jsonb, so the server has already proved this parses. What it has not
        // proved is that the shape is one this version writes.
        use document = JsonDocument.Parse json
        let root = document.RootElement

        match root.TryGetProperty "domain" with
        | true, domain ->
            domain.GetRawText()
            |> options.ErrorCodec.Decode
            |> Result.mapError Db.toStoreError
            |> Result.map CommandFailure.Domain
        | _ ->
            match root.TryGetProperty "machine" with
            | true, machine when machine.ValueKind = JsonValueKind.String ->
                Ok(CommandFailure.Machine(machine.GetString()))
            | _ ->
                Error(
                    Db.decodeFailure
                        (nameof CommandFailure)
                        "a command error carried neither a domain nor a machine tag"
                )

    let failWith (status: string) (commandId: CommandId) token (failure: CommandFailure<'Err>) ct =
        protect
            (fun cancel ->
                task {
                    match encodeFailure failure with
                    | Error error -> return Error error
                    | Ok encoded ->
                        // No expected epoch: a failure writes no state, so there is nothing for a
                        // stale epoch to conflict with, and the routine skips the check.
                        return! finalize commandId token Epoch.initial status (Some encoded) None cancel
                })
            ct

    /// Rebuilds a committed transition from its generated row. Seven decodes can fail
    /// independently, and none of them is expected: each means the row is not what this code was
    /// compiled for. Shared by the command-result read and the history page, so one row shape
    /// has one decoder.
    let toTransition
        (row: fsm.transition)
        : Result<CommittedTransition<'EntityId, 'State, 'Event, 'Action>, StoreError> =
        let entityId =
            row.entity_id
            |> options.EntityIdDecode
            |> Result.mapError (Db.decodeFailure "EntityId")

        let decode (codec: Codec<'T>) (json: string) =
            codec.Decode json |> Result.mapError Db.toStoreError

        let chartVersion =
            row.chart_version
            |> ChartVersion.tryCreate
            |> Result.mapError (Db.decodeFailure "ChartVersion")

        match
            entityId,
            decode options.EventCodec row.event,
            decode options.ActionCodec row.actions,
            decode options.StateCodec row.from_state,
            decode options.StateCodec row.to_state,
            Db.instanceStatusFromString row.status,
            chartVersion
        with
        | Ok entityId, Ok event, Ok actions, Ok fromState, Ok toState, Ok status, Ok chartVersion ->
            Ok
                { Draft =
                    { MachineId = MachineId.create row.machine_id
                      EntityId = entityId
                      Event = event
                      Actions = actions
                      FromState = fromState
                      ToState = toState
                      HandledBy = StateId.create row.handled_by
                      Exited = row.exited |> Array.map StateId.create |> List.ofArray
                      Entered = row.entered |> Array.map StateId.create |> List.ofArray
                      Status = status
                      EffectiveAt = Db.fromTimestamp row.effective_at }
                  Epoch = Db.epochOf row.epoch
                  CommandId = CommandId.ofInt64 row.command_id
                  ChartVersion = chartVersion
                  CommittedAt = Db.fromTimestamp row.committed_at }
        | Error error, _, _, _, _, _, _
        | _, Error error, _, _, _, _, _
        | _, _, Error error, _, _, _, _
        | _, _, _, Error error, _, _, _
        | _, _, _, _, Error error, _, _
        | _, _, _, _, _, Error error, _
        | _, _, _, _, _, _, Error error -> Error error

    /// The current belief, from the view's nullable columns. The view cannot carry NOT NULL, so
    /// each column is required here instead of assumed.
    let toSnapshot (row: fsm.current_belief) : Result<Snapshot<'State>, StoreError> =
        let column name value = Db.required "Snapshot" name value

        result {
            let! state = column "state" row.state
            let! status = column "status" row.status
            let! epoch = column "epoch" row.epoch
            let! state = options.StateCodec.Decode state |> Result.mapError Db.toStoreError
            let! status = Db.instanceStatusFromString status

            return
                { State = state
                  Epoch = Db.epochOf epoch
                  Status = status }
        }

    /// What became of one command, from the command, its transition if it committed, and its
    /// error if it failed.
    let toResult
        (command: fsm.command, transition: fsm.transition option, error: fsm.command_error option)
        : Result<CommandResult<'EntityId, 'State, 'Event, 'Action, 'Err>, StoreError> =
        let failure () =
            match error with
            | Some error -> readFailure error.error
            | None -> Error(Db.decodeFailure (nameof CommandResult) "a failed command has no recorded error")

        match command.status, transition with
        | ("ready" | "leased"), _ -> Ok CommandResult.Pending
        | "succeeded", Some transition -> toTransition transition |> Result.map CommandResult.Committed
        | "succeeded", None -> Error(Db.decodeFailure (nameof CommandResult) "a succeeded command has no transition")
        | "rejected", _ -> failure () |> Result.map CommandResult.Rejected
        | "dead_letter", _ -> failure () |> Result.map CommandResult.DeadLettered
        | other, _ -> Error(Db.decodeFailure (nameof CommandResult) $"unknown command status {other}")

    let read work ct =
        Db.query options.Context.Resilience dataSource work ct

    interface IStateReader<'EntityId, 'State, 'Event, 'Action> with

        member _.TryGetSnapshot(machineId, entityId, ct) =
            let machine = Some(MachineId.value machineId)
            let entity = Some(options.EntityIdEncode entityId)

            read
                (fun context token ->
                    selectTask context {
                        for b in fsm.current_belief do
                            where (b.machine_id = machine && b.entity_id = entity)
                            select b
                            tryHead
                            cancel token
                    }
                    |> Task.map (Option.traverseResult toSnapshot))
                ct

        member _.History(machineId, entityId, paging, ct) =
            let machine = MachineId.value machineId
            let entity = options.EntityIdEncode entityId

            // An absent cursor is zero rather than a null, because transition_epoch_positive
            // forbids a stored epoch of zero. One query shape then serves every page.
            let after =
                Page.cursor paging
                |> Option.map (Epoch.value >> int64)
                |> Option.defaultValue 0L

            let limit = Page.limit paging

            read
                (fun context token ->
                    selectTask context {
                        for t in fsm.transition do
                            where (t.machine_id = machine && t.entity_id = entity && t.epoch > after)
                            orderBy t.epoch
                            take limit
                            select t
                            toList
                            cancel token
                    }
                    |> Task.map (List.traverseResultM toTransition))
                ct

    interface ICommandProcessorStore<'EntityId, 'State, 'Event, 'Action, 'Err> with

        member _.Commit(commandId, token, expected, draft, ct) =
            protect
                (fun cancel ->
                    task {
                        match encodeDraft draft with
                        | Error error -> return Error error
                        | Ok(event, actions, fromState, toState) ->
                            return!
                                finalize
                                    commandId
                                    token
                                    expected
                                    "succeeded"
                                    None
                                    (Some(draft, event, actions, fromState, toState))
                                    cancel
                    })
                ct

        member _.Reject(commandId, token, error, ct) =
            failWith "rejected" commandId token error ct

        member _.DeadLetter(commandId, token, error, ct) =
            failWith "dead_letter" commandId token error ct

        member _.TryGetResult(commandId, ct) =
            let id = CommandId.value commandId

            read
                (fun context token ->
                    selectTask context {
                        for c in fsm.command do
                            leftJoin t in fsm.transition on (c.command_id = t.Value.command_id)
                            leftJoin e in fsm.command_error on (c.command_id = e.Value.command_id)
                            where (c.command_id = id)
                            select (c, t, e)
                            tryHead
                            cancel token
                    }
                    |> Task.map (Option.traverseResult toResult))
                ct
