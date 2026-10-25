namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Text.Json.Nodes
open System.Threading
open System.Threading.Tasks
open FsToolkit.ErrorHandling
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Internal
open Npgsql

/// <summary>
/// What a temporal store needs to bridge one machine's state to the belief tables.
///
/// Only the state codec, because both capabilities deal in beliefs and a belief carries a state.
/// Events, actions and errors never appear on either side.
/// </summary>
type TemporalOptions<'EntityId, 'State> =
    {
        Context: PostgresContext
        StateCodec: Codec<'State>
        EntityIdEncode: 'EntityId -> string
        /// <summary>Decoding returns a result, so a corrupt row is a typed failure rather than an exception.</summary>
        EntityIdDecode: string -> Result<'EntityId, string>
    }

/// <summary>
/// The PostgreSQL end of reading the past and of changing it.
///
/// Both capabilities live on one type because they share a row shape and a codec, and are
/// declared as two interfaces because they are two separate rights: a deployment may want the
/// reader available everywhere and the correction path reachable only where an operator has a
/// reason.
/// </summary>
type PostgresTemporalStore<'EntityId, 'State>(options: TemporalOptions<'EntityId, 'State>) =

    let context = options.Context

    let beliefAsOf = StatementKey.create "belief" "as_of" |> Sql.prepareProgram
    let correctBeliefs = StatementKey.create "belief" "correct" |> Sql.prepareProgram

    /// Rebuilds one belief from a row of fsm.belief. The view cannot carry NOT NULL, so every
    /// column is required here rather than assumed; a row that will not decode fails on its first
    /// reason, because the reasons are not independent complaints, they are one corrupt row.
    let beliefRow (reader: NpgsqlDataReader) : Result<Belief<'EntityId, 'State>, StoreError> =
        let required (read: int -> 'T) (column: string) =
            let ordinal = reader.GetOrdinal column

            if reader.IsDBNull ordinal then
                Error(Db.decodeFailure (nameof Belief) $"the view returned no {column}")
            else
                Ok(read ordinal)

        result {
            let! machineId = required reader.GetString "machine_id"
            let! entity = required reader.GetString "entity_id"
            let! state = required reader.GetString "state"
            let! status = required reader.GetString "status"
            let! epoch = required reader.GetInt64 "epoch"
            let! commandId = required reader.GetInt64 "command_id"
            let! chartVersion = required reader.GetInt32 "chart_version"
            let! validFrom = required reader.GetDateTime "valid_from"
            let! validTo = required reader.GetDateTime "valid_to"
            let! knownFrom = required reader.GetDateTime "known_from"
            let! knownTo = required reader.GetDateTime "known_to"

            let! entityId =
                entity
                |> options.EntityIdDecode
                |> Result.mapError (Db.decodeFailure "EntityId")

            let! state = state |> options.StateCodec.Decode |> Result.mapError Db.toStoreError

            let! status = EnumCodec.decode EnumCodec.instanceStatus status

            let! chartVersion =
                chartVersion
                |> ChartVersion.tryCreate
                |> Result.mapError (Db.decodeFailure "ChartVersion")

            return
                { MachineId = MachineId.create machineId
                  EntityId = entityId
                  Snapshot =
                    { State = state
                      Status = status
                      Epoch = Db.epochOf epoch }
                  CommandId = CommandId.ofInt64 commandId
                  ChartVersion = chartVersion
                  ValidFrom = Db.fromTimestamp validFrom
                  ValidTo = Db.openEnded validTo
                  KnownFrom = Db.fromTimestamp knownFrom
                  KnownTo = Db.openEnded knownTo }
        }

    /// <summary>
    /// One as-of read, from fsm.belief. <c>ValidAt</c> is this with <c>knownAt</c> set to now
    /// rather than a second query: two queries that have to agree are two queries that can drift.
    ///
    /// Both windows are half-open by constraint, so <c>from &lt;= t &lt; to</c> on the view's
    /// bounds is exactly the containment the ranges state. The view serves live beliefs, recent
    /// history and the materialized cold history as one relation, and hides beliefs past their
    /// machine's retention.
    /// </summary>
    let asOf machineId entityId (validAt: DateTimeOffset) (knownAt: DateTimeOffset) ct =
        Sql.readProgram
            context
            (StoreProgram.tryQueryOne
                beliefAsOf
                [ "machine_id", Param.Text(MachineId.value machineId)
                  "entity_id", Param.Text(options.EntityIdEncode entityId)
                  "valid_at", Param.Timestamp validAt
                  "known_at", Param.Timestamp knownAt ]
                beliefRow)
            ct

    interface ITemporalReader<'EntityId, 'State> with

        member _.ValidAt(machineId, entityId, validAt, ct) =
            // The clock is read here and nowhere else in this type. "What do we think now" is the
            // one temporal question whose second coordinate is the present; a caller wanting any
            // other one calls AsOf and says so.
            asOf machineId entityId validAt DateTimeOffset.UtcNow ct

        member _.AsOf(machineId, entityId, validAt, knownAt, ct) =
            asOf machineId entityId validAt knownAt ct

    interface ICorrectionStore<'EntityId, 'State> with

        member _.Correct(machineId, entityId, validFrom, beliefs, ct) =
            backgroundTaskResult {
                // The whole timeline is encoded before anything is sent.
                let! payload = BeliefPayload.encode options.StateCodec beliefs

                let! superseded =
                    Sql.runProgram
                        context
                        (StoreProgram.queryOne
                            correctBeliefs
                            [ "machine_id", Param.Text(MachineId.value machineId)
                              "entity_id", Param.Text(options.EntityIdEncode entityId)
                              "valid_from", Param.Timestamp validFrom
                              "beliefs", Param.Jsonb payload ]
                            (fun reader -> Row.int32 reader "superseded" |> Ok))
                        ct

                return
                    match superseded with
                    | 0 -> NothingSuperseded
                    | superseded -> Corrected superseded
            }
