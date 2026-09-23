namespace ByzantineSystems.Automata.Runtime

open System
open System.Threading
open System.Threading.Tasks
open FsToolkit.ErrorHandling
open Microsoft.Extensions.Logging
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage

type private MachineLifecycle =
    | Created
    | Running of CancellationTokenSource * Task option
    | Stopping of Task
    | Stopped of Task

/// <summary>
/// A configured state machine: one chart, one declared version, one store.
///
/// It no longer owns the ordering. Submitting a command records it durably and returns; some
/// worker, here or on another host, claims it and finishes it. What this type owns is the
/// configuration those workers share and the lifecycle that starts and stops them.
/// </summary>
type Machine<'EntityId, 'State, 'Event, 'Action, 'Err when 'EntityId: equality>
    internal
    (
        config: RuntimeConfig<'EntityId, 'State, 'Event, 'Action, 'Err>,
        completions: Completions<'EntityId, 'State, 'Event, 'Action, 'Err>,
        observerBus: ObserverDispatcher<'EntityId, 'State, 'Event, 'Action> option,
        commandSignal: WorkSignal,
        actionSignal: WorkSignal,
        /// <summary>
        /// How often a caller awaiting a durable result asks the store, when no local worker
        /// reports it first. Provisional: the answer is a notification the database sends, which
        /// arrives with the maintenance work rather than here.
        /// </summary>
        resultPollInterval: TimeSpan
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

                    commandSignal.Complete()
                    actionSignal.Complete()

                    // A caller blocked on a durable result would otherwise wait for a
                    // notification from workers that have stopped. The command itself is
                    // unaffected and can be read back later.
                    completions.CancelAll()
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
    member internal _.Completions = completions
    member internal _.ObserverBusValue = observerBus
    member internal _.CommandSignalValue = commandSignal
    member internal _.ActionSignalValue = actionSignal
    member internal _.ResultPollInterval = resultPollInterval

    member _.MachineId: MachineId = config.MachineId
    member _.ChartVersion: ChartVersion = config.ChartVersion
    member _.Completion: Task = runCompletion.Task

    member internal _.TryAcceptWork() : Result<unit, MachineError<'Err>> =
        lock lifecycleGate (fun () ->
            match lifecycle with
            | Running _ -> Ok()
            | Created -> Error(MachineError.Rejected MachineRejection.NotStarted)
            | Stopping _ -> Error(MachineError.Rejected MachineRejection.Stopping)
            | Stopped _ -> Error(MachineError.Rejected MachineRejection.Stopped))

    /// <summary>
    /// Registers the chart's structure against its declared version, then starts observer work.
    ///
    /// Registration is the last chance to notice that a chart was edited without bumping its
    /// version. A mismatch is reported rather than raised, because whether it is fatal is the
    /// host's decision; nothing here can be submitted under an unregistered version in any case,
    /// since the command's foreign key refuses it.
    /// </summary>
    member _.StartAsync(registry: IChartRegistry, ct: CancellationToken) : Task<Result<ChartRegistration, StoreError>> =
        task {
            ct.ThrowIfCancellationRequested()

            let! registration =
                registry.Register(
                    { MachineId = config.MachineId
                      Version = config.ChartVersion
                      Fingerprint = Chart.fingerprint config.Chart },
                    ct
                )

            match registration with
            | Error error -> return Error error
            | Ok outcome ->
                match outcome with
                | ChartRegistration.Mismatched stored ->
                    config.Logger.LogError(
                        EventId(1310, "ChartFingerprintMismatch"),
                        "Machine {MachineId} declares chart version {Version}, which was first registered with a different structure ({Stored}). Bump the version, or reset the database while iterating.",
                        MachineId.value config.MachineId,
                        ChartVersion.value config.ChartVersion,
                        ChartFingerprint.value stored
                    )
                | _ -> ()

                lock lifecycleGate (fun () ->
                    match lifecycle with
                    | Created ->
                        let lifetime = new CancellationTokenSource()

                        let observerTask =
                            observerBus |> Option.map (fun bus -> bus.RunAsync(lifetime.Token))

                        lifecycle <- Running(lifetime, observerTask)
                    | Running _ -> ()
                    | Stopping _
                    | Stopped _ -> invalidOp "A stopped machine cannot be started again.")

                return Ok outcome
        }

    /// <summary>Stops observer work and both wake signals; idempotent.</summary>
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

    let private inboxOf (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>) =
        machine.RuntimeConfig.Store :> ICommandInbox<'EntityId, 'Event>

    let private processorOf (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>) =
        machine.RuntimeConfig.Store
        :> ICommandProcessorStore<'EntityId, 'State, 'Event, 'Action, 'Err>

    /// <summary>
    /// Records an event durably and returns as soon as it is safe: the command is in the inbox,
    /// ordered behind that entity's earlier work, and will be processed by whichever worker
    /// claims it.
    ///
    /// This is the throughput path. Pair it with
    /// <see cref="M:ByzantineSystems.Automata.Runtime.Machine.commandResult" /> when the outcome
    /// matters but not immediately.
    ///
    /// Repeating an idempotency key returns <c>AlreadySubmitted</c> with the original command's
    /// id. That is success, and it is how a client retry after a lost response is recognised.
    /// </summary>
    /// <example>
    /// <code lang="fsharp">
    /// let! submission =
    ///     EventEnvelope.create "capture-8812" (Capture 4200m)
    ///     |> Machine.enqueue machine orderId CancellationToken.None
    /// </code>
    /// </example>
    let enqueue
        (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>)
        (entityId: 'EntityId)
        (envelope: EventEnvelope<'Event>)
        (ct: CancellationToken)
        : Task<Result<SubmissionOutcome, MachineError<'Err>>> =
        taskResult {
            do! machine.TryAcceptWork()

            let! outcome =
                (inboxOf machine)
                    .Submit(
                        { MachineId = machine.MachineId
                          EntityId = entityId
                          IdempotencyKey = EventEnvelope.idempotencyKey envelope
                          ChartVersion = machine.ChartVersion
                          Event = EventEnvelope.event envelope
                          VisibleAt = EventEnvelope.visibleAt envelope
                          ReceivedAt = EventEnvelope.receivedAt envelope
                          Audit = EventEnvelope.audit envelope },
                        ct
                    )
                |> TaskResult.mapError MachineError.Store

            // A hint to this process's workers, and nothing more. A worker elsewhere finds the
            // command by polling, which is why losing this costs latency and never a command.
            machine.CommandSignalValue.TrySignal()
            return outcome
        }

    /// <summary>Reads what became of a command, without waiting for it.</summary>
    let commandResult
        (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>)
        (commandId: CommandId)
        (ct: CancellationToken)
        : Task<Result<CommandResult<'EntityId, 'State, 'Event, 'Action, 'Err> option, MachineError<'Err>>> =
        (processorOf machine).TryGetResult(commandId, ct)
        |> TaskResult.mapError MachineError.Store

    /// <summary>
    /// Enqueues an event and waits for its durable outcome.
    ///
    /// This remains the headline call because it is what most callers want, and it now costs a
    /// durable round trip: the command is written, claimed, resolved and committed before this
    /// returns. Callers that care about throughput more than immediacy should use
    /// <c>enqueue</c> and <c>commandResult</c> instead.
    ///
    /// The wait is a local notification raced against polling the store. The notification covers
    /// the common case where the worker that processes the command is in this process; the poll
    /// covers the case where it is not, which no amount of local bookkeeping could observe.
    /// </summary>
    let send
        (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>)
        (entityId: 'EntityId)
        (envelope: EventEnvelope<'Event>)
        (ct: CancellationToken)
        : Task<Result<CommandResult<'EntityId, 'State, 'Event, 'Action, 'Err>, MachineError<'Err>>> =
        taskResult {
            do! machine.TryAcceptWork()

            let key = EventEnvelope.idempotencyKey envelope

            // Registered before submission, so the window where a result is published with
            // nobody listening does not exist.
            let! commandId =
                (inboxOf machine)
                    .TryFind(machine.MachineId, entityId, key, ct)
                |> TaskResult.mapError MachineError.Store
                |> TaskResult.map (Option.map _.CommandId)

            let! submitted =
                match commandId with
                | Some existing -> TaskResult.ok (AlreadySubmitted existing)
                | None -> enqueue machine entityId envelope ct

            let identifier =
                match submitted with
                | Accepted id
                | AlreadySubmitted id -> id

            let waiter = machine.Completions.Register identifier

            try
                let rec await () =
                    task {
                        let! settled = commandResult machine identifier ct

                        match settled with
                        | Error error -> return Error error
                        | Ok(Some CommandResult.Pending)
                        | Ok None ->
                            let delay = Task.Delay(machine.ResultPollInterval, machine.RuntimeConfig.TimeProvider, ct)
                            let! first = Task.WhenAny(waiter.Task :> Task, delay)

                            if Object.ReferenceEquals(first, waiter.Task :> Task) then
                                let! result = waiter.Task
                                return Ok result
                            else
                                return! await ()
                        | Ok(Some settled) -> return Ok settled
                    }

                return! await ()
            finally
                machine.Completions.Forget identifier
        }

    /// <summary>Returns the latest committed snapshot for an entity, or <c>None</c>.</summary>
    let state
        (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>)
        (entityId: 'EntityId)
        (ct: CancellationToken)
        : Task<Result<Snapshot<'State> option, MachineError<'Err>>> =
        (machine.RuntimeConfig.Store :> IStateReader<'EntityId, 'State>)
            .TryGetSnapshot(machine.MachineId, entityId, ct)
        |> TaskResult.mapError MachineError.Store

    /// <summary>The logical machine name the chart, inbox and store are keyed by.</summary>
    let machineId (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>) : MachineId = machine.MachineId

    /// <summary>The chart version every command this machine submits is pinned to.</summary>
    let chartVersion (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>) : ChartVersion = machine.ChartVersion

    /// <summary>Registers the chart and starts the machine's own work; repeated calls are harmless.</summary>
    let startAsync
        (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>)
        (registry: IChartRegistry)
        (ct: CancellationToken)
        : Task<Result<ChartRegistration, StoreError>> =
        machine.StartAsync(registry, ct)

    /// <summary>Stops the observer and both wake signals.</summary>
    let stopAsync (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>) (ct: CancellationToken) : Task =
        machine.StopAsync(ct)

    /// <summary>Builds the processor that drains this machine's inbox.</summary>
    let processor
        (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>)
        : CommandProcessor<'EntityId, 'State, 'Event, 'Action, 'Err> =
        CommandProcessor(
            machine.RuntimeConfig,
            machine.Completions,
            machine.ObserverBusValue,
            machine.CommandSignalValue
        )

    /// <summary>Builds the dispatcher that delivers this machine's actions through a handler.</summary>
    let dispatcher
        (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>)
        (handler: ActionHandler<'EntityId, 'Action, 'EffectError>)
        : ActionDispatcher<'EntityId, 'State, 'Event, 'Action, 'Err, 'EffectError> =
        ActionDispatcher(machine.RuntimeConfig, handler, machine.ActionSignalValue)
