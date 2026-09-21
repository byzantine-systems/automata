namespace ByzantineSystems.Automata.Runtime

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage

type private MachineLifecycle =
    | Created
    | Running of CancellationTokenSource * Task option
    | Stopping of Task
    | Stopped of Task

/// <summary>
/// A configured state machine: one chart, one store, one shared resilience pipeline, and
/// the entity-actor registry that serialises events per entity. Created through the
/// <c>machine</c> computation expression; the constructor is private.
/// </summary>
type Machine<'EntityId, 'State, 'Event, 'Action, 'Err when 'EntityId: equality>
    internal
    (
        config: RuntimeConfig<'EntityId, 'State, 'Event, 'Action, 'Err>,
        registry: Registry<'EntityId, 'State, 'Event, 'Action, 'Err>,
        observerBus: ObserverDispatcher<'EntityId, 'State, 'Event, 'Action> option,
        retrySignal: WorkSignal,
        outboxSignal: WorkSignal
    ) =

    let lifecycleGate = obj ()
    let mutable lifecycle = Created

    let runCompletion =
        TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

    let finishStop
        (completion: TaskCompletionSource)
        (lifetime: CancellationTokenSource option)
        (observerTask: Task option)
        (ct: CancellationToken)
        =
        task {
            let stopComponents =
                task {
                    observerBus |> Option.iter _.Complete()
                    lifetime |> Option.iter _.Cancel()

                    match observerTask with
                    | Some task -> do! task.WaitAsync(ct)
                    | None -> ()

                    retrySignal.Complete()
                    outboxSignal.Complete()
                    do! registry.StopAsync(ct)
                }

            let! outcome = stopComponents |> TaskOutcome.captureUnit

            lifetime |> Option.iter _.Dispose()
            lock lifecycleGate (fun () -> lifecycle <- Stopped completion.Task)

            match outcome with
            | Ok() ->
                runCompletion.TrySetResult() |> ignore
                completion.TrySetResult() |> ignore
            | Error(CanceledBy ct) ->
                runCompletion.TrySetCanceled(ct) |> ignore
                completion.TrySetCanceled(ct) |> ignore
            | Error error ->
                runCompletion.TrySetException(error) |> ignore
                completion.TrySetException(error) |> ignore
        }

    member internal _.RuntimeConfig = config
    member internal _.RegistryValue = registry
    member internal _.RetrySignalValue = retrySignal
    member internal _.OutboxSignalValue = outboxSignal
    member internal _.ObserverBusValue = observerBus

    member _.MachineId: MachineId = config.MachineId
    member _.Completion: Task = runCompletion.Task

    member internal _.TryAcceptWork() : Result<unit, MachineError<'Err>> =
        lock lifecycleGate (fun () ->
            match lifecycle with
            | Running _ -> Ok()
            | Created -> Error(MachineError.Rejected MachineRejection.NotStarted)
            | Stopping _ -> Error(MachineError.Rejected MachineRejection.Stopping)
            | Stopped _ -> Error(MachineError.Rejected MachineRejection.Stopped))

    /// <summary>Starts the machine's asynchronous observer work; idempotent.</summary>
    member _.StartAsync(ct: CancellationToken) : Task =
        ct.ThrowIfCancellationRequested()

        lock lifecycleGate (fun () ->
            match lifecycle with
            | Created ->
                let lifetime = new CancellationTokenSource()
                lifecycle <- Running(lifetime, None)

                let observerTask =
                    observerBus |> Option.map (fun bus -> bus.RunAsync(lifetime.Token))

                registry.Faulted.ContinueWith(
                    (fun (fault: Task) ->
                        if fault.IsFaulted then
                            runCompletion.TrySetException(fault.Exception.GetBaseException()) |> ignore),
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default
                )
                |> ignore

                lifecycle <- Running(lifetime, observerTask)
                Task.CompletedTask
            | Running _ -> Task.CompletedTask
            | Stopping _
            | Stopped _ -> Task.FromException(InvalidOperationException "A stopped machine cannot be started again."))

    /// <summary>Stops observer work, both wake signals, and every entity actor; idempotent.</summary>
    member _.StopAsync(ct: CancellationToken) : Task =
        lock lifecycleGate (fun () ->
            match lifecycle with
            | Stopping completion
            | Stopped completion -> completion
            | Created ->
                let completion =
                    TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

                lifecycle <- Stopping completion.Task
                finishStop completion None None ct |> ignore
                completion.Task
            | Running(lifetime, observerTask) ->
                let completion =
                    TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

                lifecycle <- Stopping completion.Task
                finishStop completion (Some lifetime) observerTask ct |> ignore
                completion.Task)

    interface IAsyncDisposable with

        member this.DisposeAsync() : ValueTask =
            ValueTask(this.StopAsync(CancellationToken.None))

    interface ByzantineSystems.Automata.Resilience.ISupervisedChild with

        member _.Completion = runCompletion.Task
        member this.StopAsync(ct) = this.StopAsync(ct)

/// <summary>The public surface of a <see cref="T:ByzantineSystems.Automata.Runtime.Machine`5" />.</summary>
[<RequireQualifiedAccess>]
module Machine =

    /// <summary>
    /// Sends one event to an entity. The result distinguishes a fresh commit, an already
    /// applied idempotency key, a durable deferral, and an explicit ignore. A machine that
    /// is not running returns a typed <c>MachineError.Rejected</c>. Cancellation before
    /// mailbox admission never enqueues the event.
    /// </summary>
    let send
        (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>)
        (entityId: 'EntityId)
        (envelope: EventEnvelope<'Event>)
        (ct: CancellationToken)
        : Task<Result<SendOutcome, MachineError<'Err>>> =
        match machine.TryAcceptWork() with
        | Ok() -> machine.RegistryValue.Send(entityId, envelope, ct)
        | Error error -> Task.FromResult(Error error)

    /// <summary>
    /// Returns the latest committed snapshot for an entity, or <c>None</c>. A machine that
    /// is not running returns a typed <c>MachineError.Rejected</c>.
    /// </summary>
    let state
        (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>)
        (entityId: 'EntityId)
        (ct: CancellationToken)
        : Task<Result<Snapshot<'State> option, MachineError<'Err>>> =
        match machine.TryAcceptWork() with
        | Ok() -> machine.RegistryValue.State(entityId, ct)
        | Error error -> Task.FromResult(Error error)

    /// <summary>The logical machine name the chart and store are keyed by.</summary>
    let machineId (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>) : MachineId = machine.MachineId

    /// <summary>Starts the machine's asynchronous work; repeated calls are harmless.</summary>
    let startAsync (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>) (ct: CancellationToken) : Task =
        machine.StartAsync(ct)

    /// <summary>Stops the machine: observer, signals, then every actor loop.</summary>
    let stopAsync (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>) (ct: CancellationToken) : Task =
        machine.StopAsync(ct)

    let internal config (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>) = machine.RuntimeConfig
    let internal retrySignal (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>) = machine.RetrySignalValue

    let internal outboxSignal (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>) = machine.OutboxSignalValue

    let internal observerBus
        (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>)
        : ObserverDispatcher<'EntityId, 'State, 'Event, 'Action> option =
        machine.ObserverBusValue
