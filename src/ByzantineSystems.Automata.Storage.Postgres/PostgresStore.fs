namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open FsToolkit.ErrorHandling
open Npgsql

/// <summary>
/// PostgreSQL implementation of every durable storage contract. State, history, retry,
/// dead-letter, and outbox share one <see cref="T:ByzantineSystems.Automata.Storage.IMachineStore`4" />.
/// Commit is a single transaction: epoch-guarded snapshot advance, history append, and
/// outbox inserts. Claims use <c>FOR UPDATE SKIP LOCKED</c>. All timestamps come from the
/// injected <see cref="T:System.TimeProvider" />; the schema contains no NULLs, using
/// <c>'-infinity'</c> as the "not leased" sentinel and <c>'infinity'</c> for open range bounds.
/// </summary>
type PostgresStore<'EntityId, 'State, 'Event, 'Action>(options: StoreOptions<'EntityId, 'State, 'Event, 'Action>) =

    let dataSource = options.DataSource
    let stateCodec = options.StateCodec
    let eventCodec = options.EventCodec
    let actionCodec = options.ActionCodec
    let actionListCodec = options.ActionListCodec
    let encodeEntity = options.EntityIdEncode
    let decodeEntity = options.EntityIdDecode
    let statePath = options.StatePath
    let timeProvider = options.TimeProvider

    let run
        (work: NpgsqlConnection -> CancellationToken -> Task<'T>)
        (ct: CancellationToken)
        : Task<Result<'T, StoreError>> =
        Db.protect
            (fun token ->
                task {
                    use! conn = dataSource.OpenConnectionAsync(token).AsTask()
                    let! value = work conn token
                    return Ok value
                })
            ct

    let runResult
        (work: NpgsqlConnection -> CancellationToken -> Task<Result<'T, StoreError>>)
        (ct: CancellationToken)
        : Task<Result<'T, StoreError>> =
        Db.protect
            (fun token ->
                task {
                    use! conn = dataSource.OpenConnectionAsync(token).AsTask()
                    return! work conn token
                })
            ct

    let encodeEachAction (actions: 'Action list) : Result<string[], CodecError> =
        let rec encode encoded =
            function
            | [] -> encoded |> List.rev |> List.toArray |> Ok
            | action :: rest ->
                result {
                    let! json = actionCodec.Encode action
                    return! encode (json :: encoded) rest
                }

        encode [] actions

    let encodePayloads (transition: Transition<'EntityId, 'State, 'Event, 'Action>) =
        result {
            let! toState = stateCodec.Encode transition.ToState |> Result.mapError Db.toStoreError
            let! fromState = stateCodec.Encode transition.FromState |> Result.mapError Db.toStoreError
            let! event = eventCodec.Encode transition.Event |> Result.mapError Db.toStoreError
            let! actions = actionListCodec.Encode transition.Actions |> Result.mapError Db.toStoreError
            let! outbox = encodeEachAction transition.Actions |> Result.mapError Db.toStoreError
            return toState, fromState, event, actions, outbox
        }

    let readRetryItem (reader: NpgsqlDataReader) : Result<RetryItem<'EntityId, 'Event>, StoreError> =
        result {
            let! event = eventCodec.Decode(reader.GetString 4) |> Result.mapError Db.toStoreError

            let lastError =
                match reader.GetString 8 with
                | "" -> None
                | value -> Some value

            return
                { RetryId = RetryId.create (reader.GetInt64 0)
                  MachineId = MachineId.create (reader.GetString 1)
                  EntityId = decodeEntity (reader.GetString 2)
                  IdempotencyKey = reader.GetString 3
                  Event = event
                  Attempts = reader.GetInt32 5
                  NextAttemptAt = Db.fromTimestamp (reader.GetDateTime 6)
                  LockedUntil = Db.lockedUntilFromDb (reader.GetDateTime 7)
                  LastError = lastError }
        }

    let readOutboxItem (reader: NpgsqlDataReader) : Result<OutboxItem<'EntityId, 'Action>, StoreError> =
        result {
            let machineId = MachineId.create (reader.GetString 0)
            let entityId = decodeEntity (reader.GetString 1)
            let eventIdempotencyKey = reader.GetString 2
            let! action = actionCodec.Decode(reader.GetString 4) |> Result.mapError Db.toStoreError

            return
                { ActionKey =
                    { MachineId = machineId
                      EntityId = entityId
                      EventIdempotencyKey = eventIdempotencyKey
                      Ordinal = reader.GetInt32 3 }
                  MachineId = machineId
                  EntityId = entityId
                  EventIdempotencyKey = eventIdempotencyKey
                  Action = action
                  Attempts = reader.GetInt32 5
                  NextAttemptAt = Db.fromTimestamp (reader.GetDateTime 6)
                  LockedUntil = Db.lockedUntilFromDb (reader.GetDateTime 7) }
        }

    let readTransition (reader: NpgsqlDataReader) : Result<Transition<'EntityId, 'State, 'Event, 'Action>, StoreError> =
        result {
            let! event = eventCodec.Decode(reader.GetString 4) |> Result.mapError Db.toStoreError
            let! actions = actionListCodec.Decode(reader.GetString 5) |> Result.mapError Db.toStoreError
            let! fromState = stateCodec.Decode(reader.GetString 6) |> Result.mapError Db.toStoreError
            let! toState = stateCodec.Decode(reader.GetString 7) |> Result.mapError Db.toStoreError
            let! status = Db.statusFromString (reader.GetString 8)

            return
                { MachineId = MachineId.create (reader.GetString 0)
                  EntityId = decodeEntity (reader.GetString 1)
                  Epoch = Epoch.ofUInt64 (uint64 (reader.GetInt64 2))
                  OccurredAt = Db.fromTimestamp (reader.GetDateTime 3)
                  Event = event
                  Actions = actions
                  FromState = fromState
                  ToState = toState
                  Status = status
                  HandledBy = StateId.create (reader.GetString 9)
                  Exited = reader.GetFieldValue<string[]>(10) |> Array.toList |> List.map StateId.create
                  Entered = reader.GetFieldValue<string[]>(11) |> Array.toList |> List.map StateId.create
                  IdempotencyKey = reader.GetString 12 }
        }

    let readAll
        (reader: NpgsqlDataReader)
        (ct: CancellationToken)
        (readRow: NpgsqlDataReader -> Result<'T, StoreError>)
        : Task<Result<'T list, StoreError>> =
        let rec read rows =
            taskResult {
                let! more = reader.ReadAsync(ct)

                if more then
                    let! row = readRow reader |> TaskResult.ofResult
                    return! read (row :: rows)
                else
                    return List.rev rows
            }

        read []

    let findReceiptInternal machineId entityKey idempotencyKey ct =
        runResult
            (fun conn token ->
                task {
                    use cmd = new NpgsqlCommand(Sql.findReceipt, conn)
                    cmd.Parameters.AddWithValue("machine_id", machineId) |> ignore
                    cmd.Parameters.AddWithValue("entity_id", entityKey) |> ignore
                    cmd.Parameters.AddWithValue("idempotency_key", idempotencyKey) |> ignore

                    use! reader = cmd.ExecuteReaderAsync(token)
                    let! more = reader.ReadAsync(token)

                    return
                        if more then
                            Some
                                { IdempotencyKey = idempotencyKey
                                  Epoch = Epoch.ofUInt64 (uint64 (reader.GetInt64 0))
                                  OccurredAt = Db.fromTimestamp (reader.GetDateTime 1) }
                            |> Ok
                        else
                            Ok None
                })
            ct

    let readActualEpoch (conn: NpgsqlConnection) (tx: NpgsqlTransaction) machineId entityKey (ct: CancellationToken) =
        task {
            use cmd = new NpgsqlCommand(Sql.readEpoch, conn, tx)
            cmd.Parameters.AddWithValue("machine_id", machineId) |> ignore
            cmd.Parameters.AddWithValue("entity_id", entityKey) |> ignore
            use! reader = cmd.ExecuteReaderAsync(ct)
            let! more = reader.ReadAsync(ct)
            return if more then uint64 (reader.GetInt64 0) else 0UL
        }

    let resolveCommitConflict machineId entityKey idempotencyKey expected actual ct =
        taskResult {
            let! existing = findReceiptInternal machineId entityKey idempotencyKey ct

            return!
                match existing with
                | Some receipt -> Ok receipt
                | None -> Error(StoreError.Concurrency(expected, Epoch.ofUInt64 actual))
        }

    let commitTransaction
        (toStateJson, fromStateJson, eventJson, actionsJson, outboxJsons)
        (transition: Transition<'EntityId, 'State, 'Event, 'Action>)
        expected
        ct
        =
        let machineId = MachineId.value transition.MachineId
        let entityKey = encodeEntity transition.EntityId
        let idempotencyKey = transition.IdempotencyKey
        let status = Db.statusToString transition.Status
        let occurredAt = Db.timestamp transition.OccurredAt
        let newEpoch = int64 (Epoch.value transition.Epoch)
        let expectedEpoch = int64 (Epoch.value expected)

        Db.protect
            (fun token ->
                task {
                    use! conn = dataSource.OpenConnectionAsync(token).AsTask()
                    use! tx = conn.BeginTransactionAsync(token).AsTask()

                    let! advanced =
                        task {
                            let sql =
                                if expectedEpoch = 0L then
                                    Sql.insertInstance
                                else
                                    Sql.updateInstance

                            use cmd = new NpgsqlCommand(sql, conn, tx)
                            cmd.Parameters.AddWithValue("machine_id", machineId) |> ignore
                            cmd.Parameters.AddWithValue("entity_id", entityKey) |> ignore
                            cmd.Parameters.AddWithValue("epoch", newEpoch) |> ignore
                            cmd.Parameters.AddWithValue("state", toStateJson) |> ignore

                            cmd.Parameters.AddWithValue("state_path", statePath transition.ToState)
                            |> ignore

                            cmd.Parameters.AddWithValue("status", status) |> ignore
                            cmd.Parameters.AddWithValue("updated_at", occurredAt) |> ignore

                            if expectedEpoch <> 0L then
                                cmd.Parameters.AddWithValue("expected", expectedEpoch) |> ignore

                            return! cmd.ExecuteNonQueryAsync(token)
                        }

                    if advanced = 0 then
                        let! actual = readActualEpoch conn tx machineId entityKey token
                        do! tx.RollbackAsync(token)
                        return! resolveCommitConflict machineId entityKey idempotencyKey expected actual token
                    else
                        let! transitionInserted =
                            use cmd = new NpgsqlCommand(Sql.insertTransition, conn, tx)
                            cmd.Parameters.AddWithValue("machine_id", machineId) |> ignore
                            cmd.Parameters.AddWithValue("entity_id", entityKey) |> ignore
                            cmd.Parameters.AddWithValue("epoch", newEpoch) |> ignore
                            cmd.Parameters.AddWithValue("occurred_at", occurredAt) |> ignore
                            cmd.Parameters.AddWithValue("event", eventJson) |> ignore
                            cmd.Parameters.AddWithValue("actions", actionsJson) |> ignore
                            cmd.Parameters.AddWithValue("from_state", fromStateJson) |> ignore
                            cmd.Parameters.AddWithValue("to_state", toStateJson) |> ignore
                            cmd.Parameters.AddWithValue("status", status) |> ignore

                            cmd.Parameters.AddWithValue("handled_by", StateId.value transition.HandledBy)
                            |> ignore

                            cmd.Parameters.AddWithValue(
                                "exited",
                                transition.Exited |> List.map StateId.value |> List.toArray
                            )
                            |> ignore

                            cmd.Parameters.AddWithValue(
                                "entered",
                                transition.Entered |> List.map StateId.value |> List.toArray
                            )
                            |> ignore

                            cmd.Parameters.AddWithValue("idempotency_key", idempotencyKey) |> ignore
                            Db.insertUnlessDuplicate "uq_transition_idem" (cmd.ExecuteNonQueryAsync(token) :> Task)

                        if not transitionInserted then
                            do! tx.RollbackAsync(token)

                            return!
                                resolveCommitConflict
                                    machineId
                                    entityKey
                                    idempotencyKey
                                    expected
                                    (uint64 expectedEpoch)
                                    token
                        else
                            if not (Array.isEmpty outboxJsons) then
                                use cmd = new NpgsqlCommand(Sql.insertOutbox, conn, tx)
                                cmd.Parameters.AddWithValue("machine_id", machineId) |> ignore
                                cmd.Parameters.AddWithValue("entity_id", entityKey) |> ignore
                                cmd.Parameters.AddWithValue("idempotency_key", idempotencyKey) |> ignore
                                cmd.Parameters.AddWithValue("actions", outboxJsons) |> ignore

                                cmd.Parameters.AddWithValue("ordinals", [| 0 .. outboxJsons.Length - 1 |])
                                |> ignore

                                cmd.Parameters.AddWithValue("occurred_at", occurredAt) |> ignore
                                do! (cmd.ExecuteNonQueryAsync(token) :> Task)

                            do! tx.CommitAsync(token)

                            return
                                Ok
                                    { IdempotencyKey = idempotencyKey
                                      Epoch = transition.Epoch
                                      OccurredAt = transition.OccurredAt }
                })
            ct

    interface IStateStore<'EntityId, 'State, 'Event, 'Action> with

        member _.TryGet(machineId, entityId, ct) =
            runResult
                (fun conn token ->
                    task {
                        use cmd = new NpgsqlCommand(Sql.tryGet, conn)
                        cmd.Parameters.AddWithValue("machine_id", MachineId.value machineId) |> ignore
                        cmd.Parameters.AddWithValue("entity_id", encodeEntity entityId) |> ignore
                        use! reader = cmd.ExecuteReaderAsync(token)
                        let! more = reader.ReadAsync(token)

                        return
                            if not more then
                                Ok None
                            else
                                result {
                                    let! status = Db.statusFromString (reader.GetString 2)

                                    let! state =
                                        stateCodec.Decode(reader.GetString 1) |> Result.mapError Db.toStoreError

                                    return
                                        Some
                                            { State = state
                                              Epoch = Epoch.ofUInt64 (uint64 (reader.GetInt64 0))
                                              Status = status }
                                }
                    })
                ct

        member _.FindReceipt(machineId, entityId, idempotencyKey, ct) =
            findReceiptInternal (MachineId.value machineId) (encodeEntity entityId) idempotencyKey ct

        member _.Commit(transition, expected, ct) =
            taskResult {
                let! payloads = encodePayloads transition |> TaskResult.ofResult
                return! commitTransaction payloads transition expected ct
            }

        member _.History(machineId, entityId, paging, ct) =
            runResult
                (fun conn token ->
                    task {
                        use cmd = new NpgsqlCommand(Sql.history, conn)
                        cmd.Parameters.AddWithValue("machine_id", MachineId.value machineId) |> ignore
                        cmd.Parameters.AddWithValue("entity_id", encodeEntity entityId) |> ignore

                        cmd.Parameters.AddWithValue(
                            "cursor",
                            int64 (Epoch.value (Page.cursor paging |> Option.defaultValue Epoch.initial))
                        )
                        |> ignore

                        cmd.Parameters.AddWithValue("limit", Page.limit paging) |> ignore
                        use! reader = cmd.ExecuteReaderAsync(token)
                        return! readAll reader token readTransition
                    })
                ct

    interface IRetryQueue<'EntityId, 'Event> with

        member _.Enqueue(request, ct) =
            taskResult {
                let! eventJson =
                    eventCodec.Encode request.Event
                    |> Result.mapError Db.toStoreError
                    |> TaskResult.ofResult

                return!
                    run
                        (fun conn token ->
                            task {
                                use cmd = new NpgsqlCommand(Sql.enqueueRetry, conn)

                                cmd.Parameters.AddWithValue("machine_id", MachineId.value request.MachineId)
                                |> ignore

                                cmd.Parameters.AddWithValue("entity_id", encodeEntity request.EntityId)
                                |> ignore

                                cmd.Parameters.AddWithValue("idempotency_key", request.IdempotencyKey) |> ignore
                                cmd.Parameters.AddWithValue("event", eventJson) |> ignore

                                cmd.Parameters.AddWithValue("next_retry_at", Db.timestamp request.NextAttemptAt)
                                |> ignore

                                cmd.Parameters.AddWithValue("last_error", request.LastError |> Option.defaultValue "")
                                |> ignore

                                let! id = cmd.ExecuteScalarAsync(token)
                                return RetryId.create (Convert.ToInt64 id)
                            })
                        ct
            }

        member _.Claim(batch, lease, ct) =
            runResult
                (fun conn token ->
                    task {
                        use cmd = new NpgsqlCommand(Sql.claimRetries, conn)
                        cmd.Parameters.AddWithValue("batch", batch) |> ignore
                        cmd.Parameters.AddWithValue("lease", lease) |> ignore

                        cmd.Parameters.AddWithValue("now", Db.timestamp (timeProvider.GetUtcNow()))
                        |> ignore

                        use! reader = cmd.ExecuteReaderAsync(token)
                        return! readAll reader token readRetryItem
                    })
                ct

        member _.Complete(retryId, ct) =
            run
                (fun conn token ->
                    task {
                        use cmd = new NpgsqlCommand(Sql.completeRetry, conn)
                        cmd.Parameters.AddWithValue("id", RetryId.value retryId) |> ignore
                        do! (cmd.ExecuteNonQueryAsync(token) :> Task)
                    })
                ct

        member _.Fail(retryId, nextAttemptAt, error, ct) =
            run
                (fun conn token ->
                    task {
                        use cmd = new NpgsqlCommand(Sql.failRetry, conn)
                        cmd.Parameters.AddWithValue("id", RetryId.value retryId) |> ignore

                        cmd.Parameters.AddWithValue("next_retry_at", Db.timestamp nextAttemptAt)
                        |> ignore

                        cmd.Parameters.AddWithValue("error", error) |> ignore
                        do! (cmd.ExecuteNonQueryAsync(token) :> Task)
                    })
                ct

    interface IDeadLetterStore<'EntityId, 'Event> with

        member _.Record(deadLetter, ct) =
            taskResult {
                let! eventJson =
                    eventCodec.Encode deadLetter.Event
                    |> Result.mapError Db.toStoreError
                    |> TaskResult.ofResult

                return!
                    run
                        (fun conn token ->
                            task {
                                use cmd = new NpgsqlCommand(Sql.recordDeadLetter, conn)

                                cmd.Parameters.AddWithValue("machine_id", MachineId.value deadLetter.MachineId)
                                |> ignore

                                cmd.Parameters.AddWithValue("entity_id", encodeEntity deadLetter.EntityId)
                                |> ignore

                                cmd.Parameters.AddWithValue("idempotency_key", deadLetter.IdempotencyKey)
                                |> ignore

                                cmd.Parameters.AddWithValue("event", eventJson) |> ignore
                                cmd.Parameters.AddWithValue("final_error", deadLetter.FinalError) |> ignore
                                cmd.Parameters.AddWithValue("attempts", deadLetter.Attempts) |> ignore
                                cmd.Parameters.AddWithValue("died_at", Db.timestamp deadLetter.DiedAt) |> ignore
                                do! (cmd.ExecuteNonQueryAsync(token) :> Task)
                            })
                        ct
            }

    interface IActionOutbox<'EntityId, 'Action> with

        member _.Claim(batch, lease, ct) =
            runResult
                (fun conn token ->
                    task {
                        use cmd = new NpgsqlCommand(Sql.claimOutbox, conn)
                        cmd.Parameters.AddWithValue("batch", batch) |> ignore
                        cmd.Parameters.AddWithValue("lease", lease) |> ignore

                        cmd.Parameters.AddWithValue("now", Db.timestamp (timeProvider.GetUtcNow()))
                        |> ignore

                        use! reader = cmd.ExecuteReaderAsync(token)
                        return! readAll reader token readOutboxItem
                    })
                ct

        member _.Complete(actionKey, ct) =
            run
                (fun conn token ->
                    task {
                        use cmd = new NpgsqlCommand(Sql.completeOutbox, conn)

                        cmd.Parameters.AddWithValue("machine_id", MachineId.value actionKey.MachineId)
                        |> ignore

                        cmd.Parameters.AddWithValue("entity_id", encodeEntity actionKey.EntityId)
                        |> ignore

                        cmd.Parameters.AddWithValue("event_idempotency_key", actionKey.EventIdempotencyKey)
                        |> ignore

                        cmd.Parameters.AddWithValue("ordinal", actionKey.Ordinal) |> ignore
                        do! (cmd.ExecuteNonQueryAsync(token) :> Task)
                    })
                ct

        member _.Fail(actionKey, nextAttemptAt, ct) =
            run
                (fun conn token ->
                    task {
                        use cmd = new NpgsqlCommand(Sql.failOutbox, conn)

                        cmd.Parameters.AddWithValue("machine_id", MachineId.value actionKey.MachineId)
                        |> ignore

                        cmd.Parameters.AddWithValue("entity_id", encodeEntity actionKey.EntityId)
                        |> ignore

                        cmd.Parameters.AddWithValue("event_idempotency_key", actionKey.EventIdempotencyKey)
                        |> ignore

                        cmd.Parameters.AddWithValue("ordinal", actionKey.Ordinal) |> ignore

                        cmd.Parameters.AddWithValue("next_attempt_at", Db.timestamp nextAttemptAt)
                        |> ignore

                        do! (cmd.ExecuteNonQueryAsync(token) :> Task)
                    })
                ct

    interface IMachineStore<'EntityId, 'State, 'Event, 'Action>

/// <summary>Construction helpers for <see cref="T:ByzantineSystems.Automata.Storage.Postgres.PostgresStore`4" />.</summary>
[<RequireQualifiedAccess>]
module PostgresStore =

    /// <summary>Builds a pooled data source for a connection string.</summary>
    let dataSource (connectionString: string) : NpgsqlDataSource =
        NpgsqlDataSource.Create(connectionString)
