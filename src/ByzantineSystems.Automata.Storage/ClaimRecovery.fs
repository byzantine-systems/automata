namespace ByzantineSystems.Automata.Storage.Internal

open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core

/// A claim has already committed before codecs run. Quarantine each poison separately, keeping
/// its raw fence available even when its domain identity cannot be decoded. A failed follow-up
/// leaves only that row leased for reclamation; it never strands the healthy batch.
[<RequireQualifiedAccess>]
module internal ClaimRecovery =

    let collect
        (decode: 'Row -> Result<'Work, StoreError>)
        (quarantine: 'Row -> string -> Task<Result<unit, StoreError>>)
        (rows: 'Row list)
        (ct: CancellationToken)
        : Task<Result<'Work list, StoreError>> =
        task {
            let healthy = ResizeArray<'Work>()

            for row in rows do
                ct.ThrowIfCancellationRequested()

                match decode row with
                | Ok work -> healthy.Add work
                | Error error ->
                    let reason =
                        match error with
                        | StoreError.Serialization(typeName, cause) -> $"undecodable: {typeName}: {cause.Message}"
                        | other -> $"undecodable: {StoreError.caseName other}"

                    let! _ = quarantine row reason
                    ()

            ct.ThrowIfCancellationRequested()
            return Ok(List.ofSeq healthy)
        }
