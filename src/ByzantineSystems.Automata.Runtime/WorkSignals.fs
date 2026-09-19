namespace ByzantineSystems.Automata.Runtime

open System
open System.Threading
open System.Threading.Channels
open System.Threading.Tasks

/// <summary>
/// A bounded wake-up hint for durable workers. The durable tables are the source of
/// truth; this channel only nudges a polling worker. <c>TrySignal</c> never blocks and
/// silently coalesces when a hint is already pending, so a committed send can never stall
/// waiting for signal capacity.
/// </summary>
type WorkSignal() =

    let options =
        BoundedChannelOptions(
            1,
            SingleReader = true,
            FullMode = BoundedChannelFullMode.DropWrite,
            AllowSynchronousContinuations = false
        )

    let channel = Channel.CreateBounded<unit>(options)
    let writer = channel.Writer
    let reader = channel.Reader

    /// <summary>Raises a hint. Never blocks; a full or disposed channel drops the hint.</summary>
    member _.TrySignal() : unit =
        try
            writer.TryWrite(()) |> ignore
        with :? ObjectDisposedException ->
            ()

    /// <summary>Completes when a hint is available, or the token is cancelled.</summary>
    member _.WaitAsync(ct: CancellationToken) : Task =
        task {
            try
                let! _ = reader.ReadAsync(ct)
                return ()
            with :? ChannelClosedException ->
                return ()
        }

    /// <summary>Completes the channel so pending waiters finish; idempotent.</summary>
    member _.Complete() : unit =
        try
            writer.TryComplete() |> ignore
        with :? ObjectDisposedException ->
            ()

    interface IDisposable with

        member this.Dispose() : unit = this.Complete()
