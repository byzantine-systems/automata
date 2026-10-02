namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Text.Json.Nodes
open System.Threading
open System.Threading.Tasks
open FsToolkit.ErrorHandling
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Internal
open ByzantineSystems.Automata.Storage.Postgres.Schema
open SqlHydra.Query

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

    let correctBeliefs = Statement.load "belief" "correct"

    /// Rebuilds one belief from a row of fsm.belief. The view cannot carry NOT NULL, so every
    /// column is required here rather than assumed; a row that will not decode fails on its first
    /// reason, because the reasons are not independent complaints, they are one corrupt row.
    let toBelief (row: fsm.belief) : Result<Belief<'EntityId, 'State>, StoreError> =
        let column name value = Db.required (nameof Belief) name value

        result {
            let! machineId = column "machine_id" row.machine_id
            let! entity = column "entity_id" row.entity_id
            let! state = column "state" row.state
            let! status = column "status" row.status
            let! epoch = column "epoch" row.epoch
            let! commandId = column "command_id" row.command_id
            let! chartVersion = column "chart_version" row.chart_version
            let! validFrom = column "valid_from" row.valid_from
            let! validTo = column "valid_to" row.valid_to
            let! knownFrom = column "known_from" row.known_from
            let! knownTo = column "known_to" row.known_to

            let! entityId = options.EntityIdDecode entity |> Result.mapError (Db.decodeFailure "EntityId")
            let! state = options.StateCodec.Decode state |> Result.mapError Db.toStoreError
            let! status = Db.instanceStatusFromString status

            let! chartVersion =
                ChartVersion.tryCreate chartVersion
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
        let machine = Some(MachineId.value machineId)
        let entity = Some(options.EntityIdEncode entityId)
        let valid = Some(Db.timestamp validAt)
        let known = Some(Db.timestamp knownAt)

        Sql.run
            context
            (postgres {
                let! row =
                    Sql.select (fun query token ->
                        selectTask query {
                            for b in fsm.belief do
                                where (
                                    b.machine_id = machine
                                    && b.entity_id = entity
                                    && b.valid_from <= valid
                                    && b.valid_to > valid
                                    && b.known_from <= known
                                    && b.known_to > known
                                )

                                select b
                                tryHead
                                cancel token
                        })

                return! row |> Option.traverseResult toBelief
            })
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
                    Sql.run
                        context
                        (Sql.one
                            (nameof CorrectionOutcome)
                            correctBeliefs
                            [ "machine_id", Param.Text(MachineId.value machineId)
                              "entity_id", Param.Text(options.EntityIdEncode entityId)
                              "valid_from", Param.Timestamp validFrom
                              "beliefs", Param.Jsonb payload ]
                            (fun reader -> Row.int32 reader "superseded"))
                        ct

                return
                    match superseded with
                    | 0 -> NothingSuperseded
                    | superseded -> Corrected superseded
            }
