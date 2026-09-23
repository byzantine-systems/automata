namespace ByzantineSystems.Automata.Core

open System
open System.Runtime.ExceptionServices
open System.Threading
open System.Threading.Tasks

/// <summary>
/// Exception recognisers for the places where a task boundary must turn a completion into
/// data. They name the one cancellation case a boundary may consume while leaving unrelated
/// cancellation and defects visible.
///
/// This module lives in <c>Core</c> because every layer above it needs the same recogniser,
/// and four independently maintained copies of it is four chances for one of them to start
/// swallowing a cancellation nobody asked for.
/// </summary>
[<AutoOpen>]
module Boundary =

    /// <summary>
    /// Recognises cancellation requested by the supplied token. An
    /// <see cref="T:System.OperationCanceledException" /> raised while a different token was
    /// cancelled is somebody else's, and does not match.
    /// </summary>
    let (|CanceledBy|_|) (ct: CancellationToken) (error: exn) =
        match error with
        | :? OperationCanceledException when ct.IsCancellationRequested -> Some()
        | _ -> None

/// <summary>
/// Converts task completion into data, only at an outer ownership boundary. Workflow code
/// stays in its typed <c>Result</c> domain; lifecycle owners use these to finish pending
/// replies and run cleanup before propagating an unexpected exception.
/// </summary>
[<RequireQualifiedAccess>]
module TaskOutcome =

    /// <summary>Captures the completion of a task returning a value.</summary>
    let capture (work: Task<'T>) : Task<Result<'T, exn>> =
        task {
            try
                let! value = work
                return Ok value
            with error ->
                return Error error
        }

    /// <summary>Captures the completion of a task returning nothing.</summary>
    let captureUnit (work: Task) : Task<Result<unit, exn>> =
        task {
            do! work
            return ()
        }
        |> capture

    /// <summary>Captures a synchronous call, for the boundaries that are not tasks.</summary>
    let captureSync (work: unit -> 'T) : Result<'T, exn> =
        try
            Ok(work ())
        with error ->
            Error error
