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

    /// <summary>Returns true when a hint is consumed, or false when the signal is completed.</summary>
    member _.WaitAsync(ct: CancellationToken) : Task<bool> =
        task {
            let! available = reader.WaitToReadAsync(ct)

            if not available then
                return false
            else
                let mutable hint = ()
                return reader.TryRead(&hint)
        }

    /// <summary>Waits for a hint, signal completion, or the polling interval.</summary>
    member this.WaitOrTimeoutAsync(interval: TimeSpan, timeProvider: TimeProvider, ct: CancellationToken) : Task<bool> =
        task {
            use waitCancellation = CancellationTokenSource.CreateLinkedTokenSource(ct)
            let signalWait = this.WaitAsync(waitCancellation.Token)
            let timerWait = Task.Delay(interval, timeProvider, waitCancellation.Token)
            let! _ = Task.WhenAny(signalWait :> Task, timerWait)

            waitCancellation.Cancel()

            let mutable signalOpen = true

            try
                let! openResult = signalWait
                signalOpen <- openResult
            with :? OperationCanceledException when waitCancellation.IsCancellationRequested ->
                ()

            try
                do! timerWait
            with :? OperationCanceledException when waitCancellation.IsCancellationRequested ->
                ()

            ct.ThrowIfCancellationRequested()
            return signalOpen
        }

    /// <summary>Completes the channel so pending waiters finish; idempotent.</summary>
    member _.Complete() : unit =
        try
            writer.TryComplete() |> ignore
        with :? ObjectDisposedException ->
            ()

    interface IDisposable with

        member this.Dispose() : unit = this.Complete()
