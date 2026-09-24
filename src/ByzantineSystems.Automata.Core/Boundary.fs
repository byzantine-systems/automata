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

/// <summary>
/// Long-running loops whose normal end is cancellation.
///
/// Every background loop in this library has the same shape: do one step, maybe wait, go again,
/// and stop when the owner cancels. Written out each time, that shape buries the step under a
/// try/with for the one cancellation it may consume and a recursion to go round again. These
/// hold the shape once, so each loop is only its step.
///
/// Cancellation by the supplied token ends a loop normally. Any other exception, including a
/// cancellation somebody else requested, propagates to the owner, as everywhere else.
/// </summary>
[<RequireQualifiedAccess>]
module Recurring =

    /// <summary>Awaits the work, or answers <c>None</c> when this token cancelled it.</summary>
    let unlessCanceled (ct: CancellationToken) (work: unit -> Task<'T>) : Task<'T option> =
        backgroundTask {
            try
                let! value = work ()
                return Some value
            with CanceledBy ct ->
                return None
        }

    /// <summary>Waits the interval on the given clock. Answers <c>false</c> when cancelled first.</summary>
    let pause (interval: TimeSpan) (time: TimeProvider) (ct: CancellationToken) : Task<bool> =
        backgroundTask {
            let! waited = unlessCanceled ct (fun () -> backgroundTask { do! Task.Delay(interval, time, ct) })
            return Option.isSome waited
        }

    /// <summary>Runs the step for as long as it answers <c>true</c> and nobody cancels.</summary>
    let repeat (step: unit -> Task<bool>) (ct: CancellationToken) : Task<unit> =
        let rec loop () =
            backgroundTask {
                match! unlessCanceled ct step with
                | Some true -> return! loop ()
                | Some false
                | None -> return ()
            }

        loop ()

    /// <summary>Runs the tick, then waits the interval, until cancelled.</summary>
    let every
        (interval: TimeSpan)
        (time: TimeProvider)
        (tick: CancellationToken -> Task)
        (ct: CancellationToken)
        : Task<unit> =
        repeat
            (fun () ->
                backgroundTask {
                    do! tick ct
                    return! pause interval time ct
                })
            ct
