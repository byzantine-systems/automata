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
    | Running of CancellationTokenSource * Task list
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
        (background: Task list)
        (ct: CancellationToken)
        =
        task {
            let stopComponents =
                task {
                    observerBus |> Option.iter _.Complete()
                    lifetime |> Option.iter _.Cancel()

                    do! (Task.WhenAll background).WaitAsync(ct)

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

    /// <summary>
    /// Keeps the wake signals fed from the store's notifications, for as long as the machine runs.
    ///
    /// Never a fault. A listener that fails costs the polling interval and nothing else, so it is
    /// logged, waited out for one polling interval, and started again; the machine keeps working
    /// on its polls in the meantime.
    /// </summary>
    let listenerFailed (error: StoreError) =
        config.Logger.LogWarning(
            EventId(1312, "NotificationListenerFailed"),
            "Listening for work on machine {MachineId} failed ({Failure}); polling alone until it is back.",
            MachineId.value config.MachineId,
            StoreError.caseName error
        )

    /// One listening session, and whether to start another. It ends Ok when it was cancelled or
    /// when the store listens to nothing, and neither is a reason to go round again.
    let listenOnce (notifications: IWorkNotifications) (ct: CancellationToken) () : Task<bool> =
        backgroundTask {
            match! notifications.Listen(config.MachineId, commandSignal.TrySignal, actionSignal.TrySignal, ct) with
            | Ok() -> return false
            | Error error ->
                listenerFailed error
                let policy = ValidatedProcessorPolicy.value config.Processor
                return! Recurring.pause policy.PollingInterval config.TimeProvider ct
        }

    /// <summary>
    /// Keeps the wake signals fed from the store's notifications, for as long as the machine runs.
    ///
    /// Never a fault. A listener that fails costs the polling interval and nothing else, so it is
    /// logged, waited out for one polling interval, and started again; the machine keeps working
    /// on its polls in the meantime.
    /// </summary>
    let listen (notifications: IWorkNotifications) (ct: CancellationToken) : Task =
        Recurring.repeat (listenOnce notifications ct) ct

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
    /// Boots the store, registers the chart's structure against its declared version, then starts
    /// the background work: observers, and the notification listener when the store offers one.
    ///
    /// The boot comes first because it is the one step allowed to say no. A refused boot starts
    /// nothing and registers nothing, and is reported rather than raised, like a fingerprint
    /// mismatch: the host decides what is fatal.
    ///
    /// Registration is the last chance to notice that a chart was edited without bumping its
    /// version. Nothing can be submitted under an unregistered version in any case, since the
    /// command's foreign key refuses it.
    /// </summary>
    member _.StartAsync(registry: IChartRegistry, ct: CancellationToken) : Task<Result<Startup, StoreError>> =
        let boot () =
            match Store.tryBoot config.Store with
            | Some store -> store.Boot(config.MachineId, ct)
            | None -> Task.FromResult(Ok BootReport.Ready)

        let register () =
            registry.Register(
                { MachineId = config.MachineId
                  Version = config.ChartVersion
                  Fingerprint = Chart.fingerprint config.Chart },
                ct
            )

        let run () =
            lock lifecycleGate (fun () ->
                match lifecycle with
                | Created ->
                    let lifetime = new CancellationTokenSource()

                    let background =
                        [ yield!
                              observerBus
                              |> Option.map (fun bus -> bus.RunAsync(lifetime.Token))
                              |> Option.toList
                          yield!
                              Store.tryNotifications config.Store
                              |> Option.map (fun notifications -> listen notifications lifetime.Token)
                              |> Option.toList ]

                    lifecycle <- Running(lifetime, background)
                | Running _ -> ()
                | Stopping _
                | Stopped _ -> invalidOp "A stopped machine cannot be started again.")

        taskResult {
            ct.ThrowIfCancellationRequested()

            match! boot () with
            | BootReport.Refused defects ->
                config.Logger.LogError(
                    EventId(1311, "StoreBootRefused"),
                    "The store for machine {MachineId} refused to boot: {Defects}. Nothing was started.",
                    MachineId.value config.MachineId,
                    defects |> List.map string |> String.concat "; "
                )

                return Startup.Refused defects
            | BootReport.Ready ->
                let! registration = register ()

                match registration with
                | ChartRegistration.Mismatched stored ->
                    config.Logger.LogError(
                        EventId(1310, "ChartFingerprintMismatch"),
                        "Machine {MachineId} declares chart version {Version}, which was first registered with a different structure ({Stored}). Bump the version, or reset the database while iterating.",
                        MachineId.value config.MachineId,
                        ChartVersion.value config.ChartVersion,
                        ChartFingerprint.value stored
                    )
                | _ -> ()

                run ()
                return Startup.Started registration
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
                finishStop completion None [] ct |> ignore
                completion.Task
            | Running(lifetime, background) ->
                let completion =
                    TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

                lifecycle <- Stopping completion.Task
                finishStop completion (Some lifetime) background ct |> ignore
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

    let private stateReaderOf (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>) =
        machine.RuntimeConfig.Store :> IStateReader<'EntityId, 'State, 'Event, 'Action>

    let private processorOf (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>) =
        machine.RuntimeConfig.Store :> ICommandProcessorStore<'EntityId, 'State, 'Event, 'Action, 'Err>

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
                (inboxOf machine).TryFind(machine.MachineId, entityId, key, ct)
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
                            let delay =
                                Task.Delay(machine.ResultPollInterval, machine.RuntimeConfig.TimeProvider, ct)

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
        (stateReaderOf machine).TryGetSnapshot(machine.MachineId, entityId, ct)
        |> TaskResult.mapError MachineError.Store

    /// <summary>
    /// One ascending page of an entity's transition log, oldest first.
    ///
    /// Always available, because history is a required capability rather than an optional one:
    /// any store that can append a log can page it.
    /// </summary>
    /// <example>
    /// <code lang="fsharp">
    /// let! firstPage = Machine.history machine orderId (Page.create 50) ct
    /// </code>
    /// </example>
    let history
        (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>)
        (entityId: 'EntityId)
        (paging: Page)
        (ct: CancellationToken)
        : Task<Result<CommittedTransition<'EntityId, 'State, 'Event, 'Action> list, MachineError<'Err>>> =
        (stateReaderOf machine).History(machine.MachineId, entityId, paging, ct)
        |> TaskResult.mapError MachineError.Store

    /// <summary>
    /// The store's ability to read the past, or <c>None</c> when this store does not offer one.
    ///
    /// An option rather than more functions on <c>Machine</c>. Mirroring the readers here would
    /// need a "capability unavailable" case on
    /// <see cref="T:ByzantineSystems.Automata.Core.MachineError`1" />, which every caller of every
    /// other machine function would then have to handle for a condition none of them can reach.
    /// Matching this once says exactly what is true.
    /// </summary>
    /// <example>
    /// <code lang="fsharp">
    /// match Machine.temporal machine with
    /// | Some reader -> reader.AsOf(Machine.machineId machine, orderId, validAt, knownAt, ct)
    /// | None -> failwith "this store does not keep history"
    /// </code>
    /// </example>
    let temporal
        (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>)
        : ITemporalReader<'EntityId, 'State> option =
        Store.tryTemporal machine.RuntimeConfig.Store

    /// <summary>
    /// The store's ability to change the past, or <c>None</c> when it does not offer one.
    ///
    /// Separate from <c>temporal</c> because reading what was believed and rewriting it are
    /// different rights, and a deployment may want one reachable where the other is not.
    /// </summary>
    let corrections
        (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>)
        : ICorrectionStore<'EntityId, 'State> option =
        Store.tryCorrections machine.RuntimeConfig.Store

    /// <summary>The logical machine name the chart, inbox and store are keyed by.</summary>
    let machineId (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>) : MachineId = machine.MachineId

    /// <summary>The chart version every command this machine submits is pinned to.</summary>
    let chartVersion (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>) : ChartVersion = machine.ChartVersion

    /// <summary>
    /// Boots the store, registers the chart and starts the machine's own work; repeated calls
    /// are harmless. A store that refuses to boot answers <c>Startup.Refused</c> and nothing
    /// starts.
    /// </summary>
    let startAsync
        (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>)
        (registry: IChartRegistry)
        (ct: CancellationToken)
        : Task<Result<Startup, StoreError>> =
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
