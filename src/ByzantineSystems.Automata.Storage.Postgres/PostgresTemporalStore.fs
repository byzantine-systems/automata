namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Text.Json.Nodes
open System.Threading
open System.Threading.Tasks
open FsToolkit.ErrorHandling
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open Npgsql
open NpgsqlTypes

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

    let dataSource = options.Context.DataSource

    let protect work ct =
        Db.protect options.Context.Resilience work ct

    let addText (name: string) (value: string) (cmd: NpgsqlCommand) =
        cmd.Parameters.AddWithValue(name, value) |> ignore

    let addInstant (name: string) (value: DateTimeOffset) (cmd: NpgsqlCommand) =
        cmd.Parameters.AddWithValue(name, Db.timestamp value) |> ignore

    let range (reader: NpgsqlDataReader) (name: string) =
        reader.GetFieldValue<NpgsqlRange<DateTime>>(reader.GetOrdinal name)

    /// Rebuilds one belief from an as-of row. Four decodes can fail independently, and none of
    /// them is expected: each means the row is not what this code was compiled for.
    let readBelief (reader: NpgsqlDataReader) : Result<Belief<'EntityId, 'State>, StoreError> =
        let validDuring = range reader "valid_during"
        let systemTime = range reader "system_time"

        validation {
            let! entityId =
                Row.string reader "entity_id"
                |> options.EntityIdDecode
                |> Result.mapError (Db.decodeFailure "EntityId")

            and! state =
                Row.string reader "state"
                |> options.StateCodec.Decode
                |> Result.mapError Db.toStoreError

            and! status = Row.string reader "status" |> Db.instanceStatusFromString

            and! chartVersion =
                Row.int32 reader "chart_version"
                |> ChartVersion.tryCreate
                |> Result.mapError (Db.decodeFailure "ChartVersion")

            return
                { MachineId = Row.string reader "machine_id" |> MachineId.create
                  EntityId = entityId
                  Snapshot =
                    { State = state
                      Status = status
                      Epoch = Row.int64 reader "epoch" |> Db.epochOf }
                  CommandId = Row.int64 reader "command_id" |> CommandId.ofInt64
                  ChartVersion = chartVersion
                  ValidFrom = Db.fromTimestamp validDuring.LowerBound
                  ValidTo = Db.openEnded validDuring.UpperBound
                  KnownFrom = Db.fromTimestamp systemTime.LowerBound
                  KnownTo = Db.openEnded systemTime.UpperBound }
        }
        // A row that will not decode fails on the first reason rather than all of them: they are
        // not independent complaints a caller can act on, they are one corrupt row.
        |> Result.mapError List.head

    /// One as-of read. `ValidAt` is this with `knownAt` set to now, rather than a second query:
    /// two queries that have to agree are two queries that can drift.
    let asOf machineId entityId validAt knownAt ct =
        protect
            (fun cancel ->
                task {
                    use! conn = dataSource.OpenConnectionAsync(cancel).AsTask()
                    use cmd = new NpgsqlCommand(SqlResources.get "belief" "as_of", conn)
                    cmd |> addText "machine_id" (MachineId.value machineId)
                    cmd |> addText "entity_id" (options.EntityIdEncode entityId)
                    cmd |> addInstant "valid_at" validAt
                    cmd |> addInstant "known_at" knownAt
                    use! reader = cmd.ExecuteReaderAsync cancel
                    let! hasRow = reader.ReadAsync cancel

                    if not hasRow then
                        return Ok None
                    else
                        return readBelief reader |> Result.map Some
                })
            ct

    /// <summary>
    /// One belief as the routine reads it.
    ///
    /// Built as a node rather than interpolated into a string. The state is already JSON that a
    /// codec produced, so it has to be embedded as a value and not as text, and every other field
    /// then escapes itself.
    /// </summary>
    let beliefNode (belief: CorrectedBelief<'State>) : Result<JsonNode, StoreError> =
        options.StateCodec.Encode belief.Asserted.State
        |> Result.mapError Db.toStoreError
        |> Result.map (fun state ->
            let node = JsonObject()
            node["state"] <- JsonNode.Parse state
            node["status"] <- JsonValue.Create(Db.instanceStatusToString belief.Asserted.Status)
            node["valid_from"] <- JsonValue.Create(Db.timestamp belief.ValidFrom)
            node["epoch"] <- JsonValue.Create(int64 (Epoch.value belief.Asserted.Epoch))
            node["command_id"] <- JsonValue.Create(CommandId.value belief.CommandId)
            node["chart_version"] <- JsonValue.Create(ChartVersion.value belief.ChartVersion)
            node :> JsonNode)

    /// The whole timeline is encoded before anything is sent, so a state that will not encode
    /// fails the correction rather than half-writing it.
    let beliefsPayload (beliefs: CorrectedBelief<'State> list) : Result<string, StoreError> =
        beliefs
        |> List.traverseResultM beliefNode
        |> Result.map (Array.ofList >> JsonArray >> _.ToJsonString())

    let sendCorrection machineId entityId validFrom (payload: string) cancel =
        task {
            use! conn = dataSource.OpenConnectionAsync(cancel).AsTask()
            use cmd = new NpgsqlCommand(SqlResources.get "belief" "correct", conn)
            cmd |> addText "machine_id" (MachineId.value machineId)
            cmd |> addText "entity_id" (options.EntityIdEncode entityId)
            cmd |> addInstant "valid_from" validFrom

            let beliefs = NpgsqlParameter("beliefs", NpgsqlDbType.Jsonb)
            beliefs.Value <- payload
            cmd.Parameters.Add beliefs |> ignore

            use! reader = cmd.ExecuteReaderAsync cancel
            let! hasRow = reader.ReadAsync cancel

            if not hasRow then
                return Error(Db.decodeFailure (nameof CorrectionOutcome) "correct_beliefs returned no row")
            else
                return
                    Ok(
                        match Row.int32 reader "superseded" with
                        | 0 -> NothingSuperseded
                        | superseded -> Corrected superseded
                    )
        }

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
            protect
                (fun cancel ->
                    taskResult {
                        let! payload = beliefsPayload beliefs
                        return! sendCorrection machineId entityId validFrom payload cancel
                    })
                ct
