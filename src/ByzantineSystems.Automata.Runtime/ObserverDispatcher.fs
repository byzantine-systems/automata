namespace ByzantineSystems.Automata.Runtime

open System
open System.Threading
open System.Threading.Channels
open System.Threading.Tasks
open ByzantineSystems.Automata.Core

/// <summary>
/// Bounded, best-effort transport for post-commit transition observers. Commits publish
/// without blocking; a dedicated loop drains the channel and invokes the observer,
/// swallowing any exception so a throwing observer can never fail a committed send.
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
        Channel.CreateBounded<Transition<'EntityId, 'State, 'Event, 'Action>>(options)

    let writer = channel.Writer
    let reader = channel.Reader

    /// <summary>Publishes a transition for observation. Never blocks; a full channel drops it.</summary>
    member _.TryPublish(transition: Transition<'EntityId, 'State, 'Event, 'Action>) : unit =
        writer.TryWrite(transition) |> ignore

    /// <summary>Drains the channel until completed or cancelled, observing each transition.</summary>
    member _.RunAsync(ct: CancellationToken) : Task =
        let rec drain () =
            task {
                let! readiness = reader.WaitToReadAsync(ct).AsTask() |> TaskOutcome.capture

                match readiness with
                | Error(CanceledBy ct) -> return ()
                | Error error -> return raise error
                | Ok false -> return ()
                | Ok true ->
                    let mutable transition =
                        Unchecked.defaultof<Transition<'EntityId, 'State, 'Event, 'Action>>

                    if reader.TryRead(&transition) then
                        // Observation is explicitly best-effort. Capturing here prevents a
                        // user callback from faulting the actor that already committed.
                        let! _ = observer transition CancellationToken.None |> TaskOutcome.captureUnit
                        return! drain ()
                    else
                        return! drain ()
            }

        drain ()

    /// <summary>Completes the channel so the drain loop finishes; idempotent.</summary>
    member _.Complete() : unit = writer.TryComplete() |> ignore
