namespace ByzantineSystems.Automata.Resilience

open System
open System.Threading
open System.Threading.Tasks

/// Exception classification and capture at asynchronous ownership boundaries. Expected
/// workflow failures remain typed Results; this module exists for Tasks supplied by callers
/// and framework callbacks, where exceptions are part of the .NET contract.
[<AutoOpen>]
module internal TaskBoundary =

    /// Matches cancellation only when it belongs to the named owner. An unrelated
    /// OperationCanceledException is a fault and must not be silently reclassified.
    let (|CanceledBy|_|) (owner: CancellationToken) (error: exn) =
        match error with
        | :? OperationCanceledException when owner.IsCancellationRequested -> Some()
        | _ -> None

    [<RequireQualifiedAccess>]
    module TaskOutcome =

        /// Awaits a foreign Task and temporarily represents its exceptional channel as data.
        /// Callers must either propagate or deliberately handle the Error case.
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

        /// Captures a synchronous framework or user callback at the point that owns it.
        let captureSync (work: unit -> 'T) : Result<'T, exn> =
            try
                Ok(work ())
            with error ->
                Error error
