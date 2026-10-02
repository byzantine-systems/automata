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
open SqlHydra.Query

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

    let record = Statement.load "supervision" "record"

    interface ISupervisionEventStore with

        member _.Record(entry, ct) =
            Sql.run
                context
                (Sql.execute
                    record
                    [ "supervisor", Param.Text(SupervisorName.value entry.Supervisor)
                      "child_id", Param.Text(SupervisedChildId.value entry.ChildId)
                      "kind", Param.Text(SupervisionMapping.kindToString entry.Kind)
                      "strategy", Param.Text(SupervisionMapping.strategyToString entry.Strategy)
                      "reason", Param.Jsonb(JsonSerializer.Serialize entry.Reason)
                      "at", Param.Timestamp entry.At ]
                 |> Op.discard)
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

        let toRecord (row: fsm.supervision_event) : Result<SupervisionRecord, StoreError> =
            match
                SupervisionMapping.kindFromString row.kind,
                SupervisionMapping.strategyFromString row.strategy,
                SupervisionMapping.reasonFromJson row.reason
            with
            | Ok kind, Ok strategy, Ok reason ->
                Ok
                    { Supervisor = SupervisorName.create row.supervisor
                      ChildId = SupervisedChildId.create row.child_id
                      Kind = kind
                      Strategy = strategy
                      Reason = reason
                      At = Db.fromTimestamp row.at }
            | Error error, _, _
            | _, Error error, _
            | _, _, Error error -> Error error

        // Newest first is the only order this table is ever read in, and it is what
        // supervision_event_recent_idx is built for.
        Sql.run
            context
            (postgres {
                let! rows =
                    Sql.select (fun query token ->
                        selectTask query {
                            for e in fsm.supervision_event do
                                orderByDescending e.at
                                thenByDescending e.id
                                take limit
                                select e
                                toList
                                cancel token
                        })

                return! rows |> List.traverseResultM toRecord
            })
            ct
