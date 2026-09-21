namespace ByzantineSystems.Automata.Runtime

open System
open System.Threading
open System.Threading.Tasks

/// <summary>
/// Exception recognisers used at runtime task boundaries. They name the one cancellation
/// case a boundary may consume while leaving unrelated cancellation and defects visible.
/// </summary>
[<AutoOpen>]
module internal RuntimeExceptionPatterns =

    /// Recognises cancellation requested by the supplied token.
    let (|CanceledBy|_|) (ct: CancellationToken) (error: exn) =
        match error with
        | :? OperationCanceledException when ct.IsCancellationRequested -> Some()
        | _ -> None

/// <summary>
/// Converts task completion into data only at an outer ownership boundary. Workflow code
/// remains in its typed Result domain; actor and lifecycle owners use this helper to finish
/// pending replies and cleanup before propagating an unexpected exception.
/// </summary>
[<RequireQualifiedAccess>]
module internal TaskOutcome =

    let capture (work: Task<'T>) : Task<Result<'T, exn>> =
        task {
            try
                let! value = work
                return Ok value
            with error ->
                return Error error
        }

    let captureUnit (work: Task) : Task<Result<unit, exn>> =
        task {
            do! work
            return ()
        }
        |> capture
