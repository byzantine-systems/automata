namespace ByzantineSystems.Automata.Storage.Postgres

open System
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Internal
open FsToolkit.ErrorHandling

[<RequireQualifiedAccess>]
module internal BeliefBatch =
    /// <summary>One encoded row owns the relationship among the six SQL array columns.</summary>
    type EncodedBelief =
        { State: string
          Status: string
          ValidFrom: DateTimeOffset
          Epoch: int64
          CommandId: int64
          ChartVersion: int }

    let encode (codec: Codec<'State>) (beliefs: CorrectedBelief<'State> list) =
        beliefs
        |> List.traverseResultM (fun belief ->
            codec.Encode belief.Asserted.State
            |> Result.mapError Db.toStoreError
            |> Result.map (fun state ->
                { State = state
                  Status = EnumCodec.encode EnumCodec.instanceStatus belief.Asserted.Status
                  ValidFrom = belief.ValidFrom
                  Epoch = int64 (Epoch.value belief.Asserted.Epoch)
                  CommandId = CommandId.value belief.CommandId
                  ChartVersion = ChartVersion.value belief.ChartVersion }))

    let parameters (rows: EncodedBelief list) =
        [ "states", Param.JsonbArray(List.map _.State rows)
          "statuses", Param.TextArray(List.map _.Status rows)
          "valid_froms", Param.TimestampArray(List.map _.ValidFrom rows)
          "epochs", Param.BigintArray(List.map _.Epoch rows)
          "command_ids", Param.BigintArray(List.map _.CommandId rows)
          "chart_versions", Param.IntArray(List.map _.ChartVersion rows) ]
