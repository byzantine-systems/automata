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
///
/// Hints are an optimisation and nothing more. They travel no further than this process, so a
/// command submitted by one host is found by another host's polling interval and not by a
/// signal. Losing every hint costs latency and never a command, which is what lets the whole
/// mechanism stay this careless about delivery.
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
    member _.TrySignal() : unit = writer.TryWrite(()) |> ignore

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
            use waiting = CancellationTokenSource.CreateLinkedTokenSource(ct)
            let hint = this.WaitAsync(waiting.Token)
            let elapsed = Task.Delay(interval, timeProvider, waiting.Token)
            let! first = Task.WhenAny(hint :> Task, elapsed)

            // Whichever lost is cancelled, then awaited through a continuation that discards its
            // outcome. Awaiting it directly would surface the cancellation this line just caused;
            // leaving it unawaited would leave a task nobody ever observes.
            waiting.Cancel()
            ct.ThrowIfCancellationRequested()

            let discard (task: Task) =
                task.ContinueWith((fun (_: Task) -> ()), TaskScheduler.Default)

            if Object.ReferenceEquals(first, hint) then
                do! discard elapsed
                // Awaited rather than captured, so a genuine fault arrives with its own stack
                // trace rather than one stamped here.
                return! hint
            else
                do! discard hint
                return true
        }

    /// <summary>Completes the channel so pending waiters finish; idempotent.</summary>
    member _.Complete() : unit = writer.TryComplete() |> ignore

    interface IDisposable with

        member this.Dispose() : unit = this.Complete()
