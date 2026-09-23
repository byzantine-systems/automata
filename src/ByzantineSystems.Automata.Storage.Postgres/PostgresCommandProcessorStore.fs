namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open Npgsql
open NpgsqlTypes

/// <summary>
/// Everything the processor needs to bridge the generic domain types to the columns of
/// <c>fsm.transition</c>, <c>fsm.instance_state</c> and <c>fsm.command_error</c>.
/// </summary>
type CommandProcessorOptions<'EntityId, 'State, 'Event, 'Action, 'Err> =
    {
        DataSource: NpgsqlDataSource
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

    let instanceStatusToString (status: InstanceStatus) : string =
        match status with
        | Running -> "running"
        | Suspended -> "suspended"
        | Terminated -> "terminated"

    let instanceStatusFromString (value: string) : Result<InstanceStatus, StoreError> =
        match value with
        | "running" -> Ok Running
        | "suspended" -> Ok Suspended
        | "terminated" -> Ok Terminated
        | other -> Error(Db.decodeFailure (nameof InstanceStatus) $"unknown instance status {other}")

    let epochOf (value: int64) : Epoch = Epoch.ofUInt64 (uint64 value)

    let outcomeFromString (value: string) (epoch: int64) (expected: Epoch) : Result<FinalizeOutcome, StoreError> =
        match value with
        | "finalized" -> Ok(Finalized(epochOf epoch))
        | "already_finalized" -> Ok(AlreadyFinalized(epochOf epoch))
        | "conflict" -> Ok(Conflict(expected, epochOf epoch))
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

    let dataSource = options.DataSource

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

            fromDraft "instance_status" NpgsqlDbType.Text (fun d ->
                box (ProcessorMapping.instanceStatusToString d.Status))

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

    let failWith (status: string) (commandId: CommandId) token (error: 'Err) ct =
        Db.protect
            (fun cancel ->
                task {
                    match options.ErrorCodec.Encode error with
                    | Error failure -> return Error(Db.toStoreError failure)
                    | Ok encoded ->
                        // No expected epoch: a failure writes no state, so there is nothing for a
                        // stale epoch to conflict with, and the routine skips the check.
                        return! finalize commandId token Epoch.initial status (Some encoded) None cancel
                })
            ct

    /// Rebuilds a committed transition from the result row. Five decodes can fail independently,
    /// and none of them is expected: each means the row is not what this code was compiled for.
    let readCommitted
        (reader: NpgsqlDataReader)
        (commandId: CommandId)
        : Result<CommandResult<'EntityId, 'State, 'Event, 'Action, 'Err>, StoreError> =
        let entityId =
            Row.string reader "entity_id"
            |> options.EntityIdDecode
            |> Result.mapError (Db.decodeFailure "EntityId")

        let event =
            Row.string reader "event"
            |> options.EventCodec.Decode
            |> Result.mapError Db.toStoreError

        let actions =
            Row.string reader "actions"
            |> options.ActionCodec.Decode
            |> Result.mapError Db.toStoreError

        let fromState =
            Row.string reader "from_state"
            |> options.StateCodec.Decode
            |> Result.mapError Db.toStoreError

        let toState =
            Row.string reader "to_state"
            |> options.StateCodec.Decode
            |> Result.mapError Db.toStoreError

        let status =
            Row.string reader "instance_status" |> ProcessorMapping.instanceStatusFromString

        let chartVersion =
            Row.int32 reader "chart_version"
            |> ChartVersion.tryCreate
            |> Result.mapError (Db.decodeFailure "ChartVersion")

        match entityId, event, actions, fromState, toState, status, chartVersion with
        | Ok entityId, Ok event, Ok actions, Ok fromState, Ok toState, Ok status, Ok chartVersion ->
            Ok(
                CommandResult.Committed
                    { Draft =
                        { MachineId = Row.string reader "machine_id" |> MachineId.create
                          EntityId = entityId
                          Event = event
                          Actions = actions
                          FromState = fromState
                          ToState = toState
                          HandledBy = Row.string reader "handled_by" |> StateId.create
                          Exited = Row.textArray reader "exited" |> List.map StateId.create
                          Entered = Row.textArray reader "entered" |> List.map StateId.create
                          Status = status
                          EffectiveAt = Row.timestamp reader "effective_at" }
                      Epoch = Row.int64 reader "epoch" |> ProcessorMapping.epochOf
                      CommandId = commandId
                      ChartVersion = chartVersion
                      CommittedAt = Row.timestamp reader "committed_at" }
            )
        | Error error, _, _, _, _, _, _
        | _, Error error, _, _, _, _, _
        | _, _, Error error, _, _, _, _
        | _, _, _, Error error, _, _, _
        | _, _, _, _, Error error, _, _
        | _, _, _, _, _, Error error, _
        | _, _, _, _, _, _, Error error -> Error error

    interface IStateReader<'EntityId, 'State> with

        member _.TryGetSnapshot(machineId, entityId, ct) =
            Db.protect
                (fun cancel ->
                    task {
                        use! conn = dataSource.OpenConnectionAsync(cancel).AsTask()
                        use cmd = new NpgsqlCommand(SqlResources.get "belief" "live", conn)
                        cmd |> addText "machine_id" (MachineId.value machineId)
                        cmd |> addText "entity_id" (options.EntityIdEncode entityId)
                        use! reader = cmd.ExecuteReaderAsync cancel
                        let! hasRow = reader.ReadAsync cancel

                        if not hasRow then
                            return Ok None
                        else
                            let state =
                                Row.string reader "state"
                                |> options.StateCodec.Decode
                                |> Result.mapError Db.toStoreError

                            let status = Row.string reader "status" |> ProcessorMapping.instanceStatusFromString

                            match state, status with
                            | Ok state, Ok status ->
                                return
                                    Ok(
                                        Some
                                            { State = state
                                              Epoch = Row.int64 reader "epoch" |> ProcessorMapping.epochOf
                                              Status = status }
                                    )
                            | Error error, _
                            | _, Error error -> return Error error
                    })
                ct

    interface ICommandProcessorStore<'EntityId, 'State, 'Event, 'Action, 'Err> with

        member _.Commit(commandId, token, expected, draft, ct) =
            Db.protect
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
            Db.protect
                (fun cancel ->
                    task {
                        use! conn = dataSource.OpenConnectionAsync(cancel).AsTask()
                        use cmd = new NpgsqlCommand(SqlResources.get "command" "result", conn)
                        cmd |> addBigint "command_id" (CommandId.value commandId)
                        use! reader = cmd.ExecuteReaderAsync cancel
                        let! hasRow = reader.ReadAsync cancel

                        if not hasRow then
                            return Ok None
                        else
                            match Row.string reader "status" with
                            | "ready"
                            | "leased" -> return Ok(Some CommandResult.Pending)
                            | "succeeded" -> return readCommitted reader commandId |> Result.map Some
                            | ("rejected" | "dead_letter") as status ->
                                return
                                    Row.string reader "error"
                                    |> options.ErrorCodec.Decode
                                    |> Result.mapError Db.toStoreError
                                    |> Result.map (fun error ->
                                        Some(
                                            if status = "dead_letter" then
                                                CommandResult.DeadLettered error
                                            else
                                                CommandResult.Rejected error
                                        ))
                            | other ->
                                return Error(Db.decodeFailure (nameof CommandResult) $"unknown command status {other}")
                    })
                ct
