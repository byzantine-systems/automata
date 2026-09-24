namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Text.Json
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

    /// Rebuilds a committed transition from a row of fsm.transition. Seven decodes can fail
    /// independently, and none of them is expected: each means the row is not what this code was
    /// compiled for.
    ///
    /// Shared by the command-result read and the history page, which is why sql/transition/history.sql
    /// returns the same column names sql/command/result.sql does. Two decoders for one row shape
    /// would be two places for it to drift.
    let readTransition
        (reader: NpgsqlDataReader)
        (commandId: CommandId)
        : Result<CommittedTransition<'EntityId, 'State, 'Event, 'Action>, StoreError> =
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

        let status = Row.string reader "instance_status" |> Db.instanceStatusFromString

        let chartVersion =
            Row.int32 reader "chart_version"
            |> ChartVersion.tryCreate
            |> Result.mapError (Db.decodeFailure "ChartVersion")

        match entityId, event, actions, fromState, toState, status, chartVersion with
        | Ok entityId, Ok event, Ok actions, Ok fromState, Ok toState, Ok status, Ok chartVersion ->
            Ok
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
                  Epoch = Row.int64 reader "epoch" |> Db.epochOf
                  CommandId = commandId
                  ChartVersion = chartVersion
                  CommittedAt = Row.timestamp reader "committed_at" }
        | Error error, _, _, _, _, _, _
        | _, Error error, _, _, _, _, _
        | _, _, Error error, _, _, _, _
        | _, _, _, Error error, _, _, _
        | _, _, _, _, Error error, _, _
        | _, _, _, _, _, Error error, _
        | _, _, _, _, _, _, Error error -> Error error

    let readCommitted (reader: NpgsqlDataReader) (commandId: CommandId) =
        readTransition reader commandId |> Result.map CommandResult.Committed

    interface IStateReader<'EntityId, 'State, 'Event, 'Action> with

        member _.TryGetSnapshot(machineId, entityId, ct) =
            protect
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

                            let status = Row.string reader "status" |> Db.instanceStatusFromString

                            match state, status with
                            | Ok state, Ok status ->
                                return
                                    Ok(
                                        Some
                                            { State = state
                                              Epoch = Row.int64 reader "epoch" |> Db.epochOf
                                              Status = status }
                                    )
                            | Error error, _
                            | _, Error error -> return Error error
                    })
                ct

        member _.History(machineId, entityId, paging, ct) =
            protect
                (fun cancel ->
                    task {
                        use! conn = dataSource.OpenConnectionAsync(cancel).AsTask()
                        use cmd = new NpgsqlCommand(SqlResources.get "transition" "history", conn)
                        cmd |> addText "machine_id" (MachineId.value machineId)
                        cmd |> addText "entity_id" (options.EntityIdEncode entityId)

                        // An absent cursor is zero rather than a null, because
                        // transition_epoch_positive forbids a stored epoch of zero. One query
                        // shape then serves the first page and every later one.
                        cmd
                        |> addBigint
                            "after_epoch"
                            (Page.cursor paging
                             |> Option.map (Epoch.value >> int64)
                             |> Option.defaultValue 0L)

                        cmd.Parameters.AddWithValue("limit", Page.limit paging) |> ignore
                        use! reader = cmd.ExecuteReaderAsync cancel

                        let page = ResizeArray()
                        let mutable failure = None
                        let mutable reading = true

                        while reading do
                            let! hasRow = reader.ReadAsync cancel

                            if not hasRow then
                                reading <- false
                            else
                                let commandId = Row.int64 reader "command_id" |> CommandId.ofInt64

                                match readTransition reader commandId with
                                | Error error ->
                                    failure <- Some error
                                    reading <- false
                                | Ok transition -> page.Add transition

                        match failure with
                        | Some error -> return Error error
                        | None -> return Ok(List.ofSeq page)
                    })
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
            protect
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
                                    |> readFailure
                                    |> Result.map (fun failure ->
                                        Some(
                                            if status = "dead_letter" then
                                                CommandResult.DeadLettered failure
                                            else
                                                CommandResult.Rejected failure
                                        ))
                            | other ->
                                return Error(Db.decodeFailure (nameof CommandResult) $"unknown command status {other}")
                    })
                ct
