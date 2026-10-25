namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Internal
open FsToolkit.ErrorHandling
open Npgsql

[<RequireQualifiedAccess>]
module private SupervisionMapping =

    let private reasonCodec = Serialization.systemTextJson<string> ()

    let reasonFromJson (json: string) =
        reasonCodec.Decode json |> Result.mapError Db.toStoreError

/// <summary>PostgreSQL append-only supervision audit writer backed by a pooled data source.</summary>
type PostgresSupervisionStore(context: PostgresContext) =

    let record = StatementKey.create "supervision" "record" |> Sql.prepareProgram

    interface ISupervisionEventStore with

        member _.Record(entry, ct) =
            Sql.runProgram
                context
                (StoreProgram.executeDiscard
                    record
                    [ "supervisor", Param.Text(SupervisorName.value entry.Supervisor)
                      "child_id", Param.Text(SupervisedChildId.value entry.ChildId)
                      "kind", Param.Text(EnumCodec.encode EnumCodec.supervisionKind entry.Kind)
                      "strategy", Param.Text(EnumCodec.encode EnumCodec.supervisionStrategy entry.Strategy)
                      "reason", Param.Jsonb(JsonSerializer.Serialize entry.Reason)
                      "at", Param.Timestamp entry.At ])
                ct

/// <summary>Read-side query adapters for the supervision audit log.</summary>
[<RequireQualifiedAccess>]
module PostgresSupervisionQueries =

    let private recent =
        StatementKey.create "supervision" "recent" |> Sql.prepareProgram

    /// <summary>Lists the newest audit facts first.</summary>
    let listRecent
        (context: PostgresContext)
        (limit: ClaimBatch)
        (ct: CancellationToken)
        : Task<Result<SupervisionRecord list, StoreError>> =
        let toRecord (reader: NpgsqlDataReader) : Result<SupervisionRecord, StoreError> =
            result {
                let! kind = EnumCodec.read Row.string EnumCodec.supervisionKind "kind" reader
                let! strategy = EnumCodec.read Row.string EnumCodec.supervisionStrategy "strategy" reader
                let! reason = SupervisionMapping.reasonFromJson (Row.string reader "reason")

                let! supervisor =
                    Row.string reader "supervisor"
                    |> SupervisorName.tryCreate
                    |> Result.mapError (Db.decodeFailure "SupervisorName")

                let! childId =
                    Row.string reader "child_id"
                    |> SupervisedChildId.tryCreate
                    |> Result.mapError (Db.decodeFailure "SupervisedChildId")

                return
                    { Supervisor = supervisor
                      ChildId = childId
                      Kind = kind
                      Strategy = strategy
                      Reason = reason
                      At = Db.fromTimestamp (reader.GetDateTime(reader.GetOrdinal "at")) }
            }

        // Newest first is the only order this table is ever read in, and it is what
        // supervision_event_recent_idx is built for.
        Sql.readProgram
            context
            (StoreProgram.queryRows recent [ "limit", Param.Int(ClaimBatch.value limit) ] toRecord)
            ct
