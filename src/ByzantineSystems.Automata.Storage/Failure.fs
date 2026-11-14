namespace ByzantineSystems.Automata.Storage.Internal

open System
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage

/// <summary>A failure with only the application-owned payload encoded.</summary>
[<RequireQualifiedAccess>]
type internal EncodedFailure =
    | Domain of string
    | Machine of string
    | Replay of ReplayError

/// <summary>The checked vocabulary shared by SQL failure headers.</summary>
type internal FailureKind =
    | DomainFailure
    | MachineFailure
    | ReplayFailure

/// <summary>A flat parameter projection, derived from one failure constructor.</summary>
type internal FailureFields =
    { Kind: string
      Domain: string
      Reason: string
      ReplayKind: string
      Version: int
      Epoch: int64
      Budget: int }

/// <summary>Optional children of a left-joined failure header.</summary>
type internal FailureRow =
    { Kind: string option
      Domain: string option
      Machine: string option
      Replay: (string * int * int64 * string * int) option }

[<RequireQualifiedAccess>]
module internal FailureEncoding =
    let kind =
        EnumCodec.create "failure kind" [ DomainFailure; MachineFailure; ReplayFailure ] (function
            | DomainFailure -> "domain"
            | MachineFailure -> "machine"
            | ReplayFailure -> "replay")

    let private codecError =
        function
        | EncodeError(name, error)
        | DecodeError(name, error) -> StoreError.Serialization(name, error)

    let encode (codec: Codec<'Err>) (failure: CommandFailure<'Err>) =
        match failure with
        | CommandFailure.Domain error ->
            codec.Encode error
            |> Result.mapError codecError
            |> Result.map EncodedFailure.Domain
        | CommandFailure.Machine reason -> Ok(EncodedFailure.Machine reason)
        | CommandFailure.Replay error -> Ok(EncodedFailure.Replay error)

    let fields failure =
        let empty =
            { Kind = ""
              Domain = "null"
              Reason = ""
              ReplayKind = ""
              Version = 0
              Epoch = 0L
              Budget = 0 }

        match failure with
        | EncodedFailure.Domain payload ->
            { empty with
                Kind = "domain"
                Domain = payload }
        | EncodedFailure.Machine reason ->
            { empty with
                Kind = "machine"
                Reason = reason }
        | EncodedFailure.Replay error ->
            let replay = { empty with Kind = "replay" }

            match error with
            | ReplayError.UnknownChartVersion version ->
                { replay with
                    ReplayKind = "unknown_chart_version"
                    Version = ChartVersion.value version }
            | ReplayError.Diverged(epoch, reason) ->
                { replay with
                    ReplayKind = "diverged"
                    Epoch = int64 (Epoch.value epoch)
                    Reason = reason }
            | ReplayError.BudgetExceeded limit ->
                { replay with
                    ReplayKind = "budget_exceeded"
                    Budget = limit }
            | ReplayError.HistoryPurged ->
                { replay with
                    ReplayKind = "history_purged" }

    let decode (codec: Codec<'Err>) (row: FailureRow) : Result<CommandFailure<'Err>, StoreError> =
        let invalid () =
            Error(
                StoreError.Serialization(
                    "CommandFailure",
                    FormatException "incomplete or incompatible failure children"
                )
            )

        match row.Kind with
        | None -> invalid ()
        | Some text ->
            EnumCodec.decode kind text
            |> Result.bind (fun tag ->
                match tag, row.Domain, row.Machine, row.Replay with
                | DomainFailure, Some payload, None, None ->
                    codec.Decode payload
                    |> Result.mapError codecError
                    |> Result.map CommandFailure.Domain
                | MachineFailure, None, Some reason, None -> Ok(CommandFailure.Machine reason)
                | ReplayFailure, None, None, Some(replay, version, epoch, reason, budget) ->
                    let decoded =
                        match replay with
                        | "unknown_chart_version" when epoch = 0L && reason = "" && budget = 0 ->
                            ChartVersion.tryCreate version
                            |> Result.mapError (fun message ->
                                StoreError.Serialization("ChartVersion", FormatException message))
                            |> Result.map ReplayError.UnknownChartVersion
                        | "diverged" when version = 0 && epoch >= 0L && budget = 0 ->
                            Ok(ReplayError.Diverged(Epoch.ofUInt64 (uint64 epoch), reason))
                        | "budget_exceeded" when version = 0 && epoch = 0L && reason = "" ->
                            Ok(ReplayError.BudgetExceeded budget)
                        | "history_purged" when version = 0 && epoch = 0L && reason = "" && budget = 0 ->
                            Ok ReplayError.HistoryPurged
                        | _ -> Error(StoreError.Serialization("ReplayError", FormatException "invalid replay fields"))

                    decoded |> Result.map CommandFailure.Replay
                | _ -> invalid ())
