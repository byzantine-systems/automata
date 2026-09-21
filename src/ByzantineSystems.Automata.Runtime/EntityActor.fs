namespace ByzantineSystems.Automata.Runtime

open System
open System.Threading
open System.Threading.Channels
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage

/// <summary>
/// The serial processing loop for one entity. A bounded, single-reader channel admits
/// messages from any number of senders while exactly one loop consumes them, so events for
/// one entity are processed in admission order and events for different entities run in
/// separate loops. Replies always continue on a caller thread, never the loop; the loop
/// yields cooperatively after 64 immediately available messages.
/// </summary>
type internal EntityActor<'EntityId, 'State, 'Event, 'Action, 'Err when 'EntityId: equality>
    (
        config: RuntimeConfig<'EntityId, 'State, 'Event, 'Action, 'Err>,
        entityId: 'EntityId,
        observerBus: ObserverDispatcher<'EntityId, 'State, 'Event, 'Action> option,
        retrySignal: WorkSignal,
        outboxSignal: WorkSignal
    ) =

    let options =
        BoundedChannelOptions(
            config.MailboxCapacity,
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait,
            AllowSynchronousContinuations = false
        )

    let channel =
        Channel.CreateBounded<ActorMessage<'EntityId, 'State, 'Event, 'Action, 'Err>>(options)

    let writer = channel.Writer
    let reader = channel.Reader
    let lastActiveTicks = ref (config.TimeProvider.GetUtcNow().UtcTicks)

    let touch () =
        Interlocked.Exchange(&lastActiveTicks.contents, config.TimeProvider.GetUtcNow().UtcTicks)
        |> ignore

    let handleSend envelope (sendCt: CancellationToken) (reply: TaskCompletionSource<_>) : Task =
        task {
            if sendCt.IsCancellationRequested then
                reply.TrySetCanceled(sendCt) |> ignore
            else
                let! outcome =
                    Send.dispatch config entityId envelope observerBus retrySignal outboxSignal sendCt
                    |> TaskOutcome.capture

                match outcome with
                | Ok result -> reply.TrySetResult(result) |> ignore
                | Error(CanceledBy sendCt) -> reply.TrySetCanceled(sendCt) |> ignore
                | Error error -> return raise error
        }

    let handleRead (readCt: CancellationToken) (reply: TaskCompletionSource<_>) : Task =
        task {
            let! outcome = config.Store.TryGet(config.MachineId, entityId, readCt) |> TaskOutcome.capture

            match outcome with
            | Ok(Ok snapshot) -> reply.TrySetResult(Ok snapshot) |> ignore
            | Ok(Error error) -> reply.TrySetResult(Error(MachineError.Store error)) |> ignore
            | Error(CanceledBy readCt) -> reply.TrySetCanceled(readCt) |> ignore
            | Error error -> return raise error
        }

    let handle (message: ActorMessage<'EntityId, 'State, 'Event, 'Action, 'Err>) : Task =
        match message with
        | Send(envelope, sendCt, reply) -> handleSend envelope sendCt reply
        | ReadState(readCt, reply) -> handleRead readCt reply

    let failMessage (error: exn) message =
        match message with
        | Send(_, _, reply) -> reply.TrySetException(error) |> ignore
        | ReadState(_, reply) -> reply.TrySetException(error) |> ignore

    let failPending error =
        let mutable pending =
            Unchecked.defaultof<ActorMessage<'EntityId, 'State, 'Event, 'Action, 'Err>>

        while reader.TryRead(&pending) do
            failMessage error pending

    let runLoop =
        let rec run sinceYield =
            task {
                let! available = reader.WaitToReadAsync().AsTask()

                if available then
                    let mutable message =
                        Unchecked.defaultof<ActorMessage<'EntityId, 'State, 'Event, 'Action, 'Err>>

                    if reader.TryRead(&message) then
                        touch ()
                        let! handled = handle message |> TaskOutcome.captureUnit

                        match handled with
                        | Error error ->
                            writer.TryComplete(error) |> ignore
                            failMessage error message
                            failPending error
                            return raise error
                        | Ok() when sinceYield + 1 >= 64 ->
                            do! Task.Yield()
                            return! run 0
                        | Ok() -> return! run (sinceYield + 1)
                    else
                        // Another continuation consumed the readiness notification. There is
                        // still only one reader; simply wait for the next notification.
                        return! run sinceYield
                else
                    return ()
            }

        run 0

    member _.Completion: Task = runLoop

    member _.Send
        (envelope: EventEnvelope<'Event>, ct: CancellationToken)
        : Task<Result<SendOutcome, MachineError<'Err>>> =
        task {
            ct.ThrowIfCancellationRequested()

            let reply =
                TaskCompletionSource<Result<SendOutcome, MachineError<'Err>>>(
                    TaskCreationOptions.RunContinuationsAsynchronously
                )

            let message = ActorMessage.Send(envelope, ct, reply)
            do! writer.WriteAsync(message, ct).AsTask()
            return! reply.Task.WaitAsync(ct)
        }

    member _.State(ct: CancellationToken) : Task<Result<Snapshot<'State> option, MachineError<'Err>>> =
        task {
            ct.ThrowIfCancellationRequested()

            let reply =
                TaskCompletionSource<Result<Snapshot<'State> option, MachineError<'Err>>>(
                    TaskCreationOptions.RunContinuationsAsynchronously
                )

            let message = ActorMessage.ReadState(ct, reply)
            do! writer.WriteAsync(message, ct).AsTask()
            return! reply.Task.WaitAsync(ct)
        }

    /// <summary>True when no message has been processed within the timeout.</summary>
    member _.IsIdle(timeout: TimeSpan) : bool =
        let last = Interlocked.Read(&lastActiveTicks.contents)
        config.TimeProvider.GetUtcNow().UtcTicks - last > timeout.Ticks

    /// <summary>Completes the mailbox so the loop drains and exits; idempotent.</summary>
    member _.Stop() : unit = writer.TryComplete() |> ignore
