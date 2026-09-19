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
open Polly

/// Handles one durable action. Register implementations as scoped when they own scoped dependencies.
type IActionHandler<'EntityId, 'Action, 'EffectError> =
    abstract HandleAsync:
        key: OutboxKey<'EntityId> * action: 'Action * ct: CancellationToken -> Task<Result<unit, 'EffectError>>

/// Settings for the root supervisor owned by the host.
type AutomataSupervisorOptions =
    { Name: SupervisorName
      Strategy: RestartStrategy
      Restart: RestartKind
      Intensity: int
      Period: TimeSpan
      RestartDelay: TimeSpan
      Shutdown: TimeSpan
      StartupRetry: StartupRetryConfig option }

/// A typed machine registration. The factory runs once per supervised generation.
type AutomataOptions<'EntityId, 'State, 'Event, 'Action, 'Err, 'EffectError when 'EntityId: equality> =
    { MachineKey: string
      Retry: RetryConfig<'Err>
      Supervisor: AutomataSupervisorOptions
      DispatchActions: bool
      MachineFactory:
          IServiceProvider
              -> ResiliencePipeline<PipelineResult<'Err>>
              -> Result<Machine<'EntityId, 'State, 'Event, 'Action, 'Err>, MachineConfigError list>
      TimeProvider: TimeProvider }

/// Raised when a machine generation cannot be configured.
exception AutomataMachineConfigurationException of MachineConfigError list

/// Raised when a durable worker returns an infrastructure error.
exception AutomataWorkerException of WorkerError

/// Raised when supervision audit persistence fails.
exception AutomataAuditException of StoreError

type internal GenerationChild<'EntityId, 'State, 'Event, 'Action, 'Err, 'EffectError when 'EntityId: equality>
    (
        machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>,
        scopeFactory: IServiceScopeFactory,
        dispatchActions: bool
    ) =

    let retryCancellation = new CancellationTokenSource()
    let actionCancellation = new CancellationTokenSource()
    let mutable stopping = 0

    let retryTask =
        task {
            let! result = RetryPump(machine).RunAsync(retryCancellation.Token)

            match result with
            | Ok() -> return ()
            | Error error -> return raise (AutomataWorkerException error)
        }

    let actionTask =
        if dispatchActions then
            let handler key action ct =
                task {
                    let scope = scopeFactory.CreateAsyncScope()

                    let! outcome =
                        task {
                            try
                                let service =
                                    scope.ServiceProvider.GetRequiredService<
                                        IActionHandler<'EntityId, 'Action, 'EffectError>
                                     >()

                                let! result = service.HandleAsync(key, action, ct)
                                return Choice1Of2 result
                            with error ->
                                return Choice2Of2 error
                        }

                    do! scope.DisposeAsync().AsTask()

                    match outcome with
                    | Choice1Of2 result -> return result
                    | Choice2Of2 error -> return raise error
                }

            task {
                let dispatcher = ActionDispatcher.forMachine handler machine
                let! result = dispatcher.RunAsync(actionCancellation.Token)

                match result with
                | Ok() -> return ()
                | Error error -> return raise (AutomataWorkerException error)
            }
        else
            task { do! Task.Delay(Timeout.InfiniteTimeSpan, actionCancellation.Token) }

    let completion =
        task {
            let! completed = Task.WhenAny(machine.Completion, retryTask, actionTask)

            if Volatile.Read(&stopping) = 0 then
                do! completed
        }

    member _.StartAsync(ct: CancellationToken) = Machine.startAsync machine ct

    interface ISupervisedChild with
        member _.Completion = completion

        member _.StopAsync(ct: CancellationToken) =
            task {
                Interlocked.Exchange(&stopping, 1) |> ignore

                actionCancellation.Cancel()

                try
                    do! actionTask.WaitAsync(ct)
                with :? OperationCanceledException when actionCancellation.IsCancellationRequested ->
                    ()

                retryCancellation.Cancel()

                try
                    do! retryTask.WaitAsync(ct)
                with :? OperationCanceledException when retryCancellation.IsCancellationRequested ->
                    ()

                do! Machine.stopAsync machine ct
            }

type internal AutomataHostedService<'EntityId, 'State, 'Event, 'Action, 'Err, 'EffectError when 'EntityId: equality>
    (
        services: IServiceProvider,
        scopeFactory: IServiceScopeFactory,
        auditStore: ISupervisionEventStore,
        pipeline: ResiliencePipeline<PipelineResult<'Err>>,
        options: AutomataOptions<'EntityId, 'State, 'Event, 'Action, 'Err, 'EffectError>
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
                        options.MachineFactory services pipeline
                        |> Result.defaultWith (fun errors -> raise (AutomataMachineConfigurationException errors))

                    let child =
                        GenerationChild<'EntityId, 'State, 'Event, 'Action, 'Err, 'EffectError>(
                            machine,
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
        match RetryConfig.validate options.Retry, Supervisor.validate spec with
        | Ok _, Ok _ -> ()
        | Error retryErrors, Ok _ -> invalidOp $"invalid automata retry configuration: %A{retryErrors}"
        | Ok _, Error supervisorErrors -> invalidOp $"invalid automata supervisor configuration: %A{supervisorErrors}"
        | Error retryErrors, Error supervisorErrors ->
            invalidOp $"invalid automata configuration: retry=%A{retryErrors}; supervisor=%A{supervisorErrors}"

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
                | Ok() -> ()
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
            try
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

                do! root.Completion
                let! finalCount = persistNewEvents root persistedEvents stoppingToken
                persistedEvents <- finalCount
            with error ->
                started.TrySetException(error) |> ignore
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
