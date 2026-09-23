namespace ByzantineSystems.Automata.DependencyInjection

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Resilience
open ByzantineSystems.Automata.Runtime
open ByzantineSystems.Automata.Storage
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging

/// <summary>
/// Handles one action a commit emitted. Register implementations as scoped when they own scoped
/// dependencies; a fresh scope is created per delivery.
///
/// The whole leased action is handed over rather than just the payload, so a handler can give
/// its destination the <c>(CommandId, Ordinal)</c> pair and let that destination recognise a
/// repeat. Delivery is at-least-once by construction.
/// </summary>
type IActionHandler<'EntityId, 'Action, 'EffectError> =
    abstract HandleAsync:
        action: LeasedAction<'EntityId, 'Action> * ct: CancellationToken -> Task<Result<unit, 'EffectError>>

/// <summary>Settings for the root supervisor owned by the host.</summary>
type AutomataSupervisorOptions =
    { Name: SupervisorName
      Strategy: RestartStrategy
      Restart: RestartKind
      Intensity: int
      Period: TimeSpan
      RestartDelay: TimeSpan
      Shutdown: TimeSpan
      StartupRetry: StartupRetryConfig option }

/// <summary>
/// A typed machine registration. The factory runs once per supervised generation.
///
/// There is no resilience policy here any more. Short-horizon retry belongs to the store, which
/// is the layer that still has driver exceptions to retry, and durable retry belongs to the
/// inbox. A host configures the first through its
/// <see cref="T:ByzantineSystems.Automata.Storage.Postgres.PostgresContext" /> and the second
/// through the machine's processor policy.
/// </summary>
type AutomataOptions<'EntityId, 'State, 'Event, 'Action, 'Err, 'EffectError when 'EntityId: equality> =
    {
        MachineKey: string
        Supervisor: AutomataSupervisorOptions
        /// <summary>
        /// Whether this host delivers actions. A deployment may run processors on one fleet and
        /// dispatchers on another, because the queue is durable and neither needs the other in
        /// the same process.
        /// </summary>
        DispatchActions: bool
        MachineFactory:
            IServiceProvider -> Result<Machine<'EntityId, 'State, 'Event, 'Action, 'Err>, MachineConfigError list>
        /// <summary>
        /// Where the chart's structure is registered against its declared version, checked once
        /// per generation at startup.
        /// </summary>
        ChartRegistry: IServiceProvider -> IChartRegistry
        TimeProvider: TimeProvider
    }

/// <summary>Raised when a machine generation cannot be configured.</summary>
exception AutomataMachineConfigurationException of MachineConfigError list

/// <summary>
/// Raised when a command's disposition was to escalate. This is the boundary where a reported
/// escalation becomes a faulted child, which is what a supervisor restarts on.
/// </summary>
exception AutomataEscalationException of commandId: CommandId * reason: string

/// <summary>Raised when the chart registration could not be read or written.</summary>
exception AutomataChartRegistrationException of StoreError

/// <summary>Raised when supervision audit persistence fails.</summary>
exception AutomataAuditException of StoreError

/// <summary>
/// Completion captured temporarily by a host-owned task boundary so cleanup and startup
/// notifications happen before an unexpected exception is propagated.
/// </summary>
type private HostedTaskCompletion<'T> =
    | Completed of 'T
    | Faulted of exn

/// <summary>
/// Host lifecycle task helpers. This is the hosted-worker supervision boundary, the last of the
/// four places this library handles exceptions; workflow and worker code stay in typed results.
/// </summary>
[<RequireQualifiedAccess>]
module private HostedTask =

    let capture (work: Task<'T>) : Task<HostedTaskCompletion<'T>> =
        task {
            try
                let! value = work
                return Completed value
            with error ->
                return Faulted error
        }

    let captureUnit (work: Task) : Task<HostedTaskCompletion<unit>> =
        task {
            do! work
            return ()
        }
        |> capture

    /// Awaits work that is expected to end by the given token's cancellation, and lets anything
    /// else through.
    let awaitOwnedCancellation (owner: CancellationToken) (work: Task) : Task =
        task {
            try
                do! work
            with :? OperationCanceledException when owner.IsCancellationRequested ->
                ()
        }

/// <summary>
/// One supervised generation: a machine, the processor that drains its inbox, and optionally the
/// dispatcher that delivers its actions.
///
/// The processor reports why it stopped rather than throwing, so this is where that report turns
/// into the fault a supervisor acts on. An orderly drain is not a fault; an escalation is.
/// </summary>
type internal GenerationChild<'EntityId, 'State, 'Event, 'Action, 'Err, 'EffectError when 'EntityId: equality>
    (
        machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>,
        registry: IChartRegistry,
        scopeFactory: IServiceScopeFactory,
        dispatchActions: bool
    ) =

    let processorCancellation = new CancellationTokenSource()
    let actionCancellation = new CancellationTokenSource()
    let mutable stopping = 0

    let processorTask =
        task {
            match! Machine.processor(machine).RunAsync(processorCancellation.Token) with
            | ProcessorStop.Drained _ -> return ()
            | ProcessorStop.Escalated(commandId, reason, _) -> return raise (AutomataEscalationException(commandId, reason))
        }

    let actionTask =
        if dispatchActions then
            let handler (action: LeasedAction<'EntityId, 'Action>) ct =
                task {
                    let scope = scopeFactory.CreateAsyncScope()

                    let! outcome =
                        task {
                            let service =
                                scope.ServiceProvider.GetRequiredService<
                                    IActionHandler<'EntityId, 'Action, 'EffectError>
                                 >()

                            return! service.HandleAsync(action, ct)
                        }
                        |> HostedTask.capture

                    do! scope.DisposeAsync().AsTask()

                    match outcome with
                    | Completed result -> return result
                    | Faulted error -> return raise error
                }

            task {
                let! _ = Machine.dispatcher machine handler |> _.RunAsync(actionCancellation.Token)
                return ()
            }
        else
            task { do! Task.Delay(Timeout.InfiniteTimeSpan, actionCancellation.Token) }

    let completion =
        task {
            let! completed = Task.WhenAny(machine.Completion, processorTask, actionTask)

            if Volatile.Read(&stopping) = 0 then
                do! completed
        }

    /// Registers the chart before any work starts. A mismatch is reported by the store and is
    /// the host's call; this one treats it as fatal, because a fleet that boots with a chart its
    /// version no longer describes will write history nobody can replay.
    member _.StartAsync(ct: CancellationToken) : Task =
        task {
            match! Machine.startAsync machine registry ct with
            | Error error -> return raise (AutomataChartRegistrationException error)
            | Ok(ChartRegistration.Mismatched stored) ->
                return
                    raise (
                        AutomataChartRegistrationException(
                            StoreError.Serialization(
                                "ChartFingerprint",
                                InvalidOperationException
                                    $"the declared chart version was first registered with structure {ChartFingerprint.value stored}"
                            )
                        )
                    )
            | Ok _ -> return ()
        }

    interface ISupervisedChild with
        member _.Completion = completion

        member _.StopAsync(ct: CancellationToken) =
            task {
                Interlocked.Exchange(&stopping, 1) |> ignore

                actionCancellation.Cancel()
                do! HostedTask.awaitOwnedCancellation actionCancellation.Token (actionTask.WaitAsync(ct))

                processorCancellation.Cancel()
                do! HostedTask.awaitOwnedCancellation processorCancellation.Token (processorTask.WaitAsync(ct))

                do! Machine.stopAsync machine ct
            }


type internal AutomataHostedService<'EntityId, 'State, 'Event, 'Action, 'Err, 'EffectError when 'EntityId: equality>
    (
        services: IServiceProvider,
        scopeFactory: IServiceScopeFactory,
        auditStore: ISupervisionEventStore,
        options: AutomataOptions<'EntityId, 'State, 'Event, 'Action, 'Err, 'EffectError>,
        logger: ILogger<AutomataHostedService<'EntityId, 'State, 'Event, 'Action, 'Err, 'EffectError>>
    ) =
    inherit BackgroundService()

    let started =
        TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

    let mutable supervisor: ISupervisor option = None
    let mutable persistedEvents = 0

    let auditKind =
        function
        | SupervisionEventKind.Started -> SupervisionAuditKind.Started
        | SupervisionEventKind.Restarted -> SupervisionAuditKind.Restarted
        | SupervisionEventKind.Escalated -> SupervisionAuditKind.Escalated
        | SupervisionEventKind.Stopped -> SupervisionAuditKind.Stopped

    let auditStrategy =
        function
        | RestartStrategy.OneForOne -> SupervisionAuditStrategy.OneForOne
        | RestartStrategy.OneForAll -> SupervisionAuditStrategy.OneForAll
        | RestartStrategy.RestForOne -> SupervisionAuditStrategy.RestForOne

    let childSpec =
        { Id = options.MachineKey
          Start =
            fun ct ->
                task {
                    let machine =
                        options.MachineFactory services
                        |> Result.defaultWith (fun errors -> raise (AutomataMachineConfigurationException errors))

                    let child =
                        GenerationChild<'EntityId, 'State, 'Event, 'Action, 'Err, 'EffectError>(
                            machine,
                            options.ChartRegistry services,
                            scopeFactory,
                            options.DispatchActions
                        )

                    do! child.StartAsync(ct)
                    return child :> ISupervisedChild
                }
          Restart = options.Supervisor.Restart
          RestartDelay = options.Supervisor.RestartDelay
          Shutdown = options.Supervisor.Shutdown
          StartupRetry = options.Supervisor.StartupRetry }

    let spec =
        { Strategy = options.Supervisor.Strategy
          Intensity = options.Supervisor.Intensity
          Period = options.Supervisor.Period
          Children = [ childSpec ] }

    let validate () =
        match Supervisor.validate spec with
        | Ok _ -> ()
        | Error supervisorErrors -> invalidOp $"invalid automata supervisor configuration: %A{supervisorErrors}"

    let persistNewEvents (root: ISupervisor) fromIndex ct =
        task {
            let! events = root.EventsAsync(ct)
            let pending = events |> List.skip fromIndex

            for event in pending do
                let record: SupervisionRecord =
                    { Supervisor = options.Supervisor.Name
                      ChildId = SupervisedChildId.create event.ChildId
                      Kind = auditKind event.Kind
                      Strategy = auditStrategy options.Supervisor.Strategy
                      Reason = event.Reason
                      At = event.At }

                let! stored = auditStore.Record(record, ct)

                match stored with
                | Ok() -> AutomataLog.supervisionEvent logger options.MachineKey event
                | Error error -> raise (AutomataAuditException error)

            return events.Length
        }

    override this.StartAsync(ct: CancellationToken) : Task =
        validate ()
        let baseTask = base.StartAsync(ct)

        task {
            do! baseTask
            do! started.Task.WaitAsync(ct)
        }

    override _.ExecuteAsync(stoppingToken: CancellationToken) =
        task {
            let run =
                task {
                    let! root = Supervisor.start spec stoppingToken options.TimeProvider
                    supervisor <- Some root
                    started.TrySetResult() |> ignore

                    let mutable observing = true

                    while observing do
                        let! next = persistNewEvents root persistedEvents stoppingToken
                        persistedEvents <- next

                        if root.Completion.IsCompleted then
                            observing <- false
                        else
                            do! Task.Delay(TimeSpan.FromMilliseconds 10., options.TimeProvider, stoppingToken)

                    let! finalCount = persistNewEvents root persistedEvents stoppingToken
                    persistedEvents <- finalCount
                    do! root.Completion
                }

            let! outcome = run |> HostedTask.captureUnit

            match outcome with
            | Completed() -> return ()
            | Faulted error ->
                started.TrySetException(error) |> ignore
                AutomataLog.hostedServiceFailed logger options.MachineKey error
                return raise error
        }

    override this.StopAsync(ct: CancellationToken) : Task =
        let baseTask = base.StopAsync(ct)

        task {
            match supervisor with
            | Some root -> do! root.StopAsync(ct)
            | None -> ()

            match supervisor with
            | Some root when not this.ExecuteTask.IsFaulted ->
                let! finalCount = persistNewEvents root persistedEvents ct
                persistedEvents <- finalCount
            | Some _
            | None -> ()

            do! baseTask
        }
