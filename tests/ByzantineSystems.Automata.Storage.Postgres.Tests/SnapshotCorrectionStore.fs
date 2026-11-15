namespace ByzantineSystems.Automata.Storage.Postgres.Tests

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Internal
open ByzantineSystems.Automata.Storage.Postgres
open FsToolkit.ErrorHandling
open Npgsql

/// A test-only third-party correction implementation. It stores no beliefs and uses only
/// the transition log plus the public core transaction routines.
type SnapshotCorrectionStore<'EntityId, 'State, 'Event, 'Action, 'Err>
    (options: MachineStoreOptions<'EntityId, 'State, 'Event, 'Action, 'Err>) =
    inherit PostgresMachineStore<'EntityId, 'State, 'Event, 'Action, 'Err>(options)

    let name = SchemaName.value options.Context.Schema

    interface IPriorState<'EntityId, 'State> with
        member _.BeforeAsync(machineId, entityId, at, cancellationToken) =
            Tap.requireMachineId (nameof machineId) machineId
            Tap.requireArg (nameof entityId) entityId

            Tap.start cancellationToken (fun () ->
                backgroundTaskResult {
                    use! connection = options.Context.DataSource.OpenConnectionAsync(cancellationToken)

                    use command =
                        new NpgsqlCommand(
                            $"SELECT to_state,status,epoch FROM {name}.transition WHERE machine_id=@machine AND entity_id=@entity
                        AND effective_at < @at ORDER BY effective_at DESC,epoch DESC LIMIT 1",
                            connection
                        )

                    command.Parameters.AddWithValue("machine", MachineId.value machineId) |> ignore

                    command.Parameters.AddWithValue("entity", options.EntityIdEncode entityId)
                    |> ignore

                    command.Parameters.AddWithValue("at", at) |> ignore
                    use! row = command.ExecuteReaderAsync cancellationToken
                    let! found = row.ReadAsync cancellationToken

                    if not found then
                        return None
                    else
                        let! state =
                            options.StateCodec.Decode(Row.string row "to_state")
                            |> Result.mapError Db.toStoreError

                        let! status = EnumCodec.decode EnumCodec.instanceStatus (Row.string row "status")

                        return
                            Some
                                { State = state
                                  Status = status
                                  Epoch = Db.epochOf (Row.int64 row "epoch") }
                })

    interface ICorrectionCommitter<'EntityId, 'State, 'Event, 'Action> with
        member _.CommitCorrectionAsync(commandId, token, expected, correction, cancellationToken) =
            Tap.requireArg (nameof correction) correction

            Tap.start cancellationToken (fun () ->
                backgroundTaskResult {
                    let draft = correction.Transition
                    let! fromState = options.StateCodec.Encode draft.FromState |> Result.mapError Db.toStoreError
                    let! toState = options.StateCodec.Encode draft.ToState |> Result.mapError Db.toStoreError
                    use! connection = options.Context.DataSource.OpenConnectionAsync(cancellationToken)
                    use! transaction = connection.BeginTransactionAsync cancellationToken

                    let! standing =
                        task {
                            use command =
                                new NpgsqlCommand(
                                    $"SELECT * FROM {name}.command_standing(@id,@token)",
                                    connection,
                                    transaction
                                )

                            command.Parameters.AddWithValue("id", CommandId.value commandId) |> ignore
                            command.Parameters.AddWithValue("token", LeaseToken.value token) |> ignore
                            use! row = command.ExecuteReaderAsync cancellationToken
                            let! found = row.ReadAsync cancellationToken

                            if not found then
                                return Error(Db.decodeFailure "command_standing" "missing fence verdict")
                            else
                                let epoch =
                                    if row.IsDBNull(row.GetOrdinal "epoch") then
                                        expected
                                    else
                                        Db.epochOf (Row.int64 row "epoch")

                                return
                                    Ok(
                                        Row.string row "outcome",
                                        epoch,
                                        Row.string row "status",
                                        Row.string row "machine_id",
                                        Row.string row "entity_id",
                                        Row.int32 row "chart_version"
                                    )
                        }

                    let outcome, actual, status, machine, entity, version = standing

                    match outcome with
                    | "lease_lost" -> return FinalizeOutcome.LeaseLost
                    | "already_finalized" ->
                        let! status = EnumCodec.decode EnumCodec.commandStatus status
                        return AlreadyFinalized(actual, status)
                    | "finalized" when actual <> expected -> return FinalizeOutcome.Conflict(expected, actual)
                    | "finalized" ->
                        use write =
                            new NpgsqlCommand(
                                $"SELECT {name}.append_transition(@machine,@entity,@epoch,@id,@version,'[]'::jsonb,
                              @from::jsonb,@to::jsonb,@handled,@exited,@entered,@status::{name}.instance_status,@at);
                          SELECT {name}.set_entity_state(@machine,@entity,@to::jsonb,@status::{name}.instance_status,@epoch,@id,@version,@at);
                          SELECT {name}.close_command(@id,'succeeded');",
                                connection,
                                transaction
                            )

                        for parameter, value in
                            [ "machine", box machine
                              "entity", box entity
                              "epoch", box (int64 (Epoch.value (Epoch.next actual)))
                              "id", box (CommandId.value commandId)
                              "version", box version
                              "from", box fromState
                              "to", box toState
                              "handled", box (StateId.value draft.HandledBy)
                              "exited", box (draft.Exited |> List.map StateId.value |> List.toArray)
                              "entered", box (draft.Entered |> List.map StateId.value |> List.toArray)
                              "status", box (EnumCodec.encode EnumCodec.instanceStatus draft.Status)
                              "at", box correction.ValidFrom ] do
                            write.Parameters.AddWithValue(parameter, value) |> ignore

                        let! _ = write.ExecuteNonQueryAsync cancellationToken
                        do! transaction.CommitAsync cancellationToken
                        return Finalized(Epoch.next actual)
                    | unknown -> return! Error(Db.decodeFailure "command_standing" $"unknown verdict {unknown}")
                })
