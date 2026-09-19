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
        try
            writer.TryWrite(transition) |> ignore
        with :? ObjectDisposedException ->
            ()

    /// <summary>Drains the channel until completed or cancelled, observing each transition.</summary>
    member _.RunAsync(ct: CancellationToken) : Task =
        let rec drain () =
            task {
                try
                    let! transition = reader.ReadAsync(ct)

                    try
                        do! observer transition CancellationToken.None
                    with _ ->
                        ()

                    return! drain ()
                with
                | :? ChannelClosedException -> return ()
                | :? OperationCanceledException -> return ()
            }

        drain ()

    /// <summary>Completes the channel so the drain loop finishes; idempotent.</summary>
    member _.Complete() : unit =
        try
            writer.TryComplete() |> ignore
        with :? ObjectDisposedException ->
            ()
