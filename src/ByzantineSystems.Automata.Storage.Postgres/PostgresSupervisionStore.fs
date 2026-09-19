namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open Npgsql

[<RequireQualifiedAccess>]
module private SupervisionMapping =

    let kindToString =
        function
        | SupervisionAuditKind.Started -> "Started"
        | SupervisionAuditKind.Restarted -> "Restarted"
        | SupervisionAuditKind.Escalated -> "Escalated"
        | SupervisionAuditKind.Stopped -> "Stopped"

    let kindFromString value =
        match value with
        | "Started" -> Ok SupervisionAuditKind.Started
        | "Restarted" -> Ok SupervisionAuditKind.Restarted
        | "Escalated" -> Ok SupervisionAuditKind.Escalated
        | "Stopped" -> Ok SupervisionAuditKind.Stopped
        | other ->
            Error(
                StoreError.Serialization(
                    typeof<SupervisionAuditKind>.Name,
                    FormatException $"unknown supervision audit kind {other}"
                )
            )

    let strategyToString =
        function
        | SupervisionAuditStrategy.OneForOne -> "OneForOne"
        | SupervisionAuditStrategy.OneForAll -> "OneForAll"
        | SupervisionAuditStrategy.RestForOne -> "RestForOne"

    let strategyFromString value =
        match value with
        | "OneForOne" -> Ok SupervisionAuditStrategy.OneForOne
        | "OneForAll" -> Ok SupervisionAuditStrategy.OneForAll
        | "RestForOne" -> Ok SupervisionAuditStrategy.RestForOne
        | other ->
            Error(
                StoreError.Serialization(
                    typeof<SupervisionAuditStrategy>.Name,
                    FormatException $"unknown supervision audit strategy {other}"
                )
            )

    let reasonFromJson (json: string) =
        try
            Ok(JsonSerializer.Deserialize<string> json)
        with :? JsonException as ex ->
            Error(StoreError.Serialization("SupervisionRecord.Reason", ex))

[<RequireQualifiedAccess>]
module private SupervisionDb =

    let protect
        (work: CancellationToken -> Task<Result<'T, StoreError>>)
        (ct: CancellationToken)
        : Task<Result<'T, StoreError>> =
        task {
            try
                return! work ct
            with
            | :? OperationCanceledException when ct.IsCancellationRequested ->
                return! Task.FromCanceled<Result<'T, StoreError>>(ct)
            | :? NpgsqlException as ex -> return Error(StoreError.Unavailable ex)
        }

/// <summary>PostgreSQL append-only supervision audit writer backed by a pooled data source.</summary>
type PostgresSupervisionStore(dataSource: NpgsqlDataSource) =

    interface ISupervisionEventStore with

        member _.Record(record, ct) =
            SupervisionDb.protect
                (fun token ->
                    task {
                        use! conn = dataSource.OpenConnectionAsync(token).AsTask()
                        use cmd = new NpgsqlCommand(Sql.recordSupervisionEvent, conn)

                        cmd.Parameters.AddWithValue("supervisor", SupervisorName.value record.Supervisor)
                        |> ignore

                        cmd.Parameters.AddWithValue("child_id", SupervisedChildId.value record.ChildId)
                        |> ignore

                        cmd.Parameters.AddWithValue("kind", SupervisionMapping.kindToString record.Kind)
                        |> ignore

                        cmd.Parameters.AddWithValue("strategy", SupervisionMapping.strategyToString record.Strategy)
                        |> ignore

                        cmd.Parameters.AddWithValue("reason", JsonSerializer.Serialize record.Reason)
                        |> ignore

                        cmd.Parameters.AddWithValue("at", Db.timestamp record.At) |> ignore
                        do! (cmd.ExecuteNonQueryAsync(token) :> Task)
                        return Ok()
                    })
                ct

/// <summary>Read-side query adapters for the supervision audit log.</summary>
[<RequireQualifiedAccess>]
module PostgresSupervisionQueries =

    /// <summary>Lists the newest audit facts first.</summary>
    let listRecent
        (dataSource: NpgsqlDataSource)
        (limit: int)
        (ct: CancellationToken)
        : Task<Result<SupervisionRecord list, StoreError>> =
        if limit < 1 then
            invalidArg (nameof limit) "The supervision query limit must be positive."

        SupervisionDb.protect
            (fun token ->
                task {
                    use! conn = dataSource.OpenConnectionAsync(token).AsTask()
                    use cmd = new NpgsqlCommand(Sql.listRecentSupervisionEvents, conn)
                    cmd.Parameters.AddWithValue("limit", limit) |> ignore
                    use! reader = cmd.ExecuteReaderAsync(token)

                    let rec read records =
                        task {
                            let! more = reader.ReadAsync(token)

                            if not more then
                                return Ok(List.rev records)
                            else
                                match
                                    SupervisionMapping.kindFromString (reader.GetString 2),
                                    SupervisionMapping.strategyFromString (reader.GetString 3),
                                    SupervisionMapping.reasonFromJson (reader.GetString 4)
                                with
                                | Ok kind, Ok strategy, Ok reason ->
                                    let record =
                                        { Supervisor = SupervisorName.create (reader.GetString 0)
                                          ChildId = SupervisedChildId.create (reader.GetString 1)
                                          Kind = kind
                                          Strategy = strategy
                                          Reason = reason
                                          At = Db.fromTimestamp (reader.GetDateTime 5) }

                                    return! read (record :: records)
                                | Error error, _, _
                                | _, Error error, _
                                | _, _, Error error -> return Error error
                        }

                    return! read []
                })
            ct
