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

    let handle (message: ActorMessage<'EntityId, 'State, 'Event, 'Action, 'Err>) : Task =
        task {
            match message with
            | Send(envelope, sendCt, reply) ->
                if sendCt.IsCancellationRequested then
                    reply.TrySetCanceled(sendCt) |> ignore
                else
                    try
                        let! outcome =
                            Send.dispatch config entityId envelope observerBus retrySignal outboxSignal sendCt

                        reply.TrySetResult(outcome) |> ignore
                    with :? OperationCanceledException when sendCt.IsCancellationRequested ->
                        reply.TrySetCanceled(sendCt) |> ignore

            | ReadState(readCt, reply) ->
                try
                    let! snapshot = config.Store.TryGet(config.MachineId, entityId, readCt)

                    match snapshot with
                    | Ok found -> reply.TrySetResult(Ok found) |> ignore
                    | Error e -> reply.TrySetResult(Error(MachineError.Store e)) |> ignore
                with :? OperationCanceledException when readCt.IsCancellationRequested ->
                    reply.TrySetCanceled(readCt) |> ignore
        }

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
                try
                    let! message = reader.ReadAsync()
                    touch ()

                    try
                        do! handle message
                    with error ->
                        writer.TryComplete(error) |> ignore
                        failMessage error message
                        failPending error
                        return raise error

                    if sinceYield + 1 >= 64 then
                        do! Task.Yield()
                        return! run 0
                    else
                        return! run (sinceYield + 1)
                with :? ChannelClosedException ->
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
    member _.Stop() : unit =
        try
            writer.TryComplete() |> ignore
        with :? ObjectDisposedException ->
            ()
