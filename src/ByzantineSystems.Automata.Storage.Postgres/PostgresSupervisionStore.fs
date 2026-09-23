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

    let private reasonCodec = Serialization.systemTextJson<string> ()

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
        reasonCodec.Decode json |> Result.mapError Db.toStoreError

/// <summary>PostgreSQL append-only supervision audit writer backed by a pooled data source.</summary>
type PostgresSupervisionStore(context: PostgresContext) =

    let protect work ct = Db.protect context.Resilience work ct

    interface ISupervisionEventStore with

        member _.Record(record, ct) =
            protect
                (fun token ->
                    task {
                        use! conn = context.DataSource.OpenConnectionAsync(token).AsTask()
                        use cmd = new NpgsqlCommand(SqlResources.get "supervision" "record", conn)

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
        (context: PostgresContext)
        (limit: int)
        (ct: CancellationToken)
        : Task<Result<SupervisionRecord list, StoreError>> =
        if limit < 1 then
            invalidArg (nameof limit) "The supervision query limit must be positive."

        Db.protect
            context.Resilience
            (fun token ->
                task {
                    use! conn = context.DataSource.OpenConnectionAsync(token).AsTask()
                    use cmd = new NpgsqlCommand(SqlResources.get "supervision" "list_recent", conn)
                    cmd.Parameters.AddWithValue("limit", limit) |> ignore
                    use! reader = cmd.ExecuteReaderAsync(token)

                    let rec read records =
                        task {
                            let! more = reader.ReadAsync(token)

                            if not more then
                                return Ok(List.rev records)
                            else
                                match
                                    SupervisionMapping.kindFromString (Row.string reader "kind"),
                                    SupervisionMapping.strategyFromString (Row.string reader "strategy"),
                                    SupervisionMapping.reasonFromJson (Row.string reader "reason")
                                with
                                | Ok kind, Ok strategy, Ok reason ->
                                    let record =
                                        { Supervisor = SupervisorName.create (Row.string reader "supervisor")
                                          ChildId = SupervisedChildId.create (Row.string reader "child_id")
                                          Kind = kind
                                          Strategy = strategy
                                          Reason = reason
                                          At = Row.timestamp reader "at" }

                                    return! read (record :: records)
                                | Error error, _, _
                                | _, Error error, _
                                | _, _, Error error -> return Error error
                        }

                    return! read []
                })
            ct
