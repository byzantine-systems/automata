namespace ByzantineSystems.Automata.Storage.Postgres

open System.Text.Json.Nodes
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open FsToolkit.ErrorHandling

/// <summary>
/// A belief timeline as <c>fsm.correct_beliefs</c> reads it, for every caller that writes one:
/// a direct correction and a replayed one encode it the same way.
/// </summary>
[<RequireQualifiedAccess>]
module internal BeliefPayload =

    /// <summary>
    /// One belief as the routine reads it.
    ///
    /// Built as a node rather than interpolated into a string. The state is already JSON that a
    /// codec produced, so it has to be embedded as a value and not as text, and every other field
    /// then escapes itself.
    /// </summary>
    let private node (codec: Codec<'State>) (belief: CorrectedBelief<'State>) : Result<JsonNode, StoreError> =
        codec.Encode belief.Asserted.State
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
    let encode (codec: Codec<'State>) (beliefs: CorrectedBelief<'State> list) : Result<string, StoreError> =
        beliefs
        |> List.traverseResultM (node codec)
        |> Result.map (Array.ofList >> JsonArray >> _.ToJsonString())
