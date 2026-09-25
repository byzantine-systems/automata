namespace ByzantineSystems.Automata.Runtime

open System.Threading
open System.Threading.Channels
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage

/// <summary>
/// Bounded, best-effort transport for post-commit transition observers. The processor publishes
/// without blocking; a dedicated loop drains the channel and invokes the observer, discarding
/// any exception so a throwing observer can never affect a commit that already happened.
///
/// A full channel drops rather than waits, and that is deliberate. The transition log is the
/// record of what occurred; this is a convenience beside it, and a convenience must not be able
/// to slow down the thing it is observing.
/// </summary>
type internal ObserverDispatcher<'EntityId, 'State, 'Event, 'Action>
    (observer: TransitionObserver<'EntityId, 'State, 'Event, 'Action>, capacity: int) =

    let options =
        BoundedChannelOptions(
            capacity,
            SingleReader = true,
            FullMode = BoundedChannelFullMode.DropWrite,
            AllowSynchronousContinuations = false
        )

    let channel =
        Channel.CreateBounded<CommittedTransition<'EntityId, 'State, 'Event, 'Action>>(options)

    let writer = channel.Writer
    let reader = channel.Reader

    /// <summary>Publishes a transition for observation. Never blocks; a full channel drops it.</summary>
    member _.TryPublish(transition: CommittedTransition<'EntityId, 'State, 'Event, 'Action>) : unit =
        writer.TryWrite(transition) |> ignore

    /// <summary>Drains the channel until completed or cancelled, observing each transition.</summary>
    member _.RunAsync(ct: CancellationToken) : Task =
        let rec drain () =
            task {
                // Cancellation ends the loop; anything else is a defect in the channel itself
                // and is left to propagate to whoever owns this task.
                let! ready =
                    task {
                        try
                            return! reader.WaitToReadAsync(ct).AsTask()
                        with :? System.OperationCanceledException when ct.IsCancellationRequested ->
                            return false
                    }

                if not ready then
                    return ()
                else
                    let mutable transition =
                        Unchecked.defaultof<CommittedTransition<'EntityId, 'State, 'Event, 'Action>>

                    if reader.TryRead(&transition) then
                        // Observation is explicitly best-effort. Discarding here keeps a user
                        // callback from faulting a worker that has already committed.
                        let! _ = observer transition CancellationToken.None |> TaskOutcome.captureUnit
                        return! drain ()
                    else
                        return! drain ()
            }

        drain ()

    /// <summary>Completes the channel so the drain loop finishes; idempotent.</summary>
    member _.Complete() : unit = writer.TryComplete() |> ignore
