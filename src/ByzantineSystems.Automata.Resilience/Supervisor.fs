namespace ByzantineSystems.Automata.Resilience

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open Polly
open Polly.Retry
open Polly.Timeout

/// <summary>Which children a restart affects, in Erlang's terms.</summary>
type RestartStrategy =

    /// <summary>Restart only the failed child.</summary>
    | OneForOne

    /// <summary>Terminate every child, then restart all of them.</summary>
    | OneForAll

    /// <summary>Restart the failed child and every child started after it.</summary>
    | RestForOne

/// <summary>When a child is restarted.</summary>
type RestartKind =

    /// <summary>Always restarted, even on a normal exit.</summary>
    | Permanent

    /// <summary>Restarted only on abnormal termination.</summary>
    | Transient

    /// <summary>Never restarted.</summary>
    | Temporary

/// <summary>
/// A supervised child. <see cref="P:ByzantineSystems.Automata.Resilience.ISupervisedChild.Completion" />
/// is the child's lifetime: it completes normally on an orderly stop and faults when the
/// child crashed. The supervisor observes it; nothing here polls.
/// </summary>
type ISupervisedChild =

    /// <summary>Completes when the child's work loop exits; faults on a crash.</summary>
    abstract Completion: Task

    /// <summary>Asks the child to stop orderly within the supervisor's shutdown budget.</summary>
    abstract StopAsync: ct: CancellationToken -> Task

/// <summary>
/// Optional bounded startup policy for a child whose <c>Start</c> may fail transiently:
/// a Polly retry with jitter around a per-attempt startup timeout. Startup failures are
/// distinct from runtime crashes; they never count toward restart intensity.
/// </summary>
type StartupRetryConfig =
    { MaxAttempts: int
      BaseDelay: TimeSpan
      MaxDelay: TimeSpan
      UseJitter: bool
      StartupTimeout: TimeSpan }

/// <summary>How the supervisor manages one child.</summary>
type ChildSpec =
    {
        Id: string
        Start: CancellationToken -> Task<ISupervisedChild>
        Restart: RestartKind
        /// <summary>Delay between a child's exit and its restart. Zero restarts immediately.</summary>
        RestartDelay: TimeSpan
        /// <summary>Shutdown budget granted to <c>StopAsync</c> on termination.</summary>
        Shutdown: TimeSpan
        StartupRetry: StartupRetryConfig option
    }

/// <summary>
/// Erlang restart-intensity semantics: at most <c>Intensity</c> child restarts within any
/// <c>Period</c> window, across all children. Exceeding it escalates; the supervisor stops
/// everything and faults. This is an exact sliding window the supervisor maintains itself;
/// a circuit breaker cannot express it, because a rarely-restarting child never reaches a
/// breaker's minimum throughput.
/// </summary>
type SupervisorSpec =
    { Strategy: RestartStrategy
      Intensity: int
      Period: TimeSpan
      Children: ChildSpec list }

/// <summary>What happened to one child under supervision. Mirrors <c>fsm.supervision_event</c>.</summary>
type SupervisionEventKind =

    /// <summary>The child's first instance started.</summary>
    | Started

    /// <summary>A new instance started after a restart.</summary>
    | Restarted

    /// <summary>Restart intensity was exceeded; the supervisor gave up on this child.</summary>
    | Escalated

    /// <summary>The child stopped and was not restarted.</summary>
    | Stopped

/// <summary>One auditable supervision fact.</summary>
type SupervisionEvent =
    { Kind: SupervisionEventKind
      ChildId: string
      Reason: string
      At: DateTimeOffset }

/// <summary>
/// Raised (as the fault of <see cref="P:ByzantineSystems.Automata.Resilience.ISupervisor.Completion" />
/// and of the initial start) when restart intensity is exceeded or a child cannot be started.
/// The parent (a host, or another supervisor) decides what an escalation means.
/// </summary>
exception SupervisorEscalated of childId: string * reason: string

/// <summary>Structural defects in a <see cref="T:ByzantineSystems.Automata.Resilience.SupervisorSpec" />.</summary>
type SupervisorError =
    | ChildIdEmpty
    | DuplicateChildId of childId: string
    | IntensityBelowOne of intensity: int
    | PeriodNotPositive of TimeSpan
    | RestartDelayNegative of childId: string * TimeSpan
    | ShutdownNegative of childId: string * TimeSpan
    | StartupMaxAttemptsBelowOne of childId: string * int
    | StartupDelayInvalid of childId: string * baseDelay: TimeSpan * maxDelay: TimeSpan
    | StartupTimeoutNotPositive of childId: string * TimeSpan
    | StartupTimeoutOutOfRange of childId: string * TimeSpan

/// <summary>A running supervisor.</summary>
type ISupervisor =

    /// <summary>
    /// Completes when every child has stopped. Faults with
    /// <see cref="T:ByzantineSystems.Automata.Resilience.SupervisorEscalated" /> when intensity
    /// was exceeded or a child could not be started.
    /// </summary>
    abstract Completion: Task

    /// <summary>
    /// Ordered shutdown: stop taking work, stop children in reverse start order, each with
    /// its own <c>Shutdown</c> budget, then settle. Best-effort beyond the total budget.
    /// </summary>
    abstract StopAsync: ct: CancellationToken -> Task<unit>

    /// <summary>Snapshot of the supervision event log, in occurrence order.</summary>
    abstract Events: unit -> SupervisionEvent list

    /// <summary>Asynchronously snapshots the event log, allowing replies during shutdown.</summary>
    abstract EventsAsync: ct: CancellationToken -> Task<SupervisionEvent list>

type private ExitKind =
    | ExitNormal
    | ExitCanceled
    | ExitAbnormal of exn

/// <summary>Starting and running supervisors.</summary>
[<RequireQualifiedAccess>]
module Supervisor =

    /// <summary>Accumulates every structural defect rather than stopping at the first.</summary>
    let validate (spec: SupervisorSpec) : Result<SupervisorSpec, SupervisorError list> =
        let minimumTimeout = TimeSpan.FromMilliseconds 10.
        let maximumTimeout = TimeSpan.FromDays 1.

        let initialErrors =
            [ if spec.Intensity < 1 then
                  IntensityBelowOne spec.Intensity

              if spec.Period <= TimeSpan.Zero then
                  PeriodNotPositive spec.Period ]

        let childErrors, _ =
            spec.Children
            |> List.fold
                (fun (errors, seenIds) child ->
                    let idErrors, nextSeenIds =
                        if String.IsNullOrWhiteSpace child.Id then
                            [ ChildIdEmpty ], seenIds
                        elif Set.contains child.Id seenIds then
                            [ DuplicateChildId child.Id ], seenIds
                        else
                            [], Set.add child.Id seenIds

                    let timingErrors =
                        [ if child.RestartDelay < TimeSpan.Zero then
                              RestartDelayNegative(child.Id, child.RestartDelay)

                          if child.Shutdown < TimeSpan.Zero then
                              ShutdownNegative(child.Id, child.Shutdown) ]

                    let startupErrors =
                        match child.StartupRetry with
                        | None -> []
                        | Some startup ->
                            [ if startup.MaxAttempts < 1 then
                                  StartupMaxAttemptsBelowOne(child.Id, startup.MaxAttempts)

                              if startup.BaseDelay < TimeSpan.Zero || startup.MaxDelay < startup.BaseDelay then
                                  StartupDelayInvalid(child.Id, startup.BaseDelay, startup.MaxDelay)

                              if startup.StartupTimeout <= TimeSpan.Zero then
                                  StartupTimeoutNotPositive(child.Id, startup.StartupTimeout)
                              elif
                                  startup.StartupTimeout < minimumTimeout
                                  || startup.StartupTimeout > maximumTimeout
                              then
                                  StartupTimeoutOutOfRange(child.Id, startup.StartupTimeout) ]

                    errors @ idErrors @ timingErrors @ startupErrors, nextSeenIds)
                (initialErrors, Set.empty)

        match childErrors with
        | [] -> Ok spec
        | errors -> Error errors

    type private StartPurpose =
        | InitialStart
        | RestartStart

    type private ChildInstance =
        { Generation: int
          Child: ISupervisedChild
          MonitorCancellation: CancellationTokenSource }

    type private AfterStop =
        | StrategyStop
        | ShutdownStop

    type private ChildLifecycle =
        | NotStarted
        | Starting of generation: int * purpose: StartPurpose
        | Running of ChildInstance
        | DelayingRestart of generation: int
        | AwaitingRestart of generation: int
        | Stopping of ChildInstance * AfterStop
        | Inactive

    type private ChildRuntime =
        { Spec: ChildSpec
          Index: int
          Generation: int
          Lifecycle: ChildLifecycle }

    type private StrategyBarrier =
        { PendingStops: int list
          RestartIndices: int list }

    type private Settlement =
        | CompleteSuccessfully
        | CompleteCanceled of CancellationToken
        | CompleteWithError of exn

    type private CoordinatorMode =
        | Active
        | TerminatingStrategy of StrategyBarrier
        | RestartingStrategy of pendingRestarts: int list
        | ShuttingDown of pendingStops: int list * Settlement
        | Settled

    type private CoordinatorState =
        { Children: Map<int, ChildRuntime>
          InitialNext: int option
          InitialComplete: bool
          Restarts: DateTimeOffset list
          EventsReversed: SupervisionEvent list
          Mode: CoordinatorMode
          OutstandingEffects: int
          StopWaiters: TaskCompletionSource<unit> list
          Registration: CancellationTokenRegistration option }

    type private CoordinatorMessage =
        | Begin
        | InstallRegistration of CancellationTokenRegistration
        | ParentCanceled
        | RequestStop of TaskCompletionSource<unit>
        | Snapshot of AsyncReplyChannel<SupervisionEvent list>
        | StartFinished of index: int * generation: int * purpose: StartPurpose * Result<ISupervisedChild, exn>
        | ChildExited of index: int * generation: int * ExitKind
        | RestartDelayElapsed of index: int * generation: int
        | ChildStopFinished of index: int * generation: int * AfterStop

    type private Effect =
        | StartChild of index: int * generation: int * purpose: StartPurpose * ChildSpec
        | MonitorChild of index: int * ChildInstance
        | StopChild of index: int * ChildSpec * ChildInstance * AfterStop
        | WaitForRestart of index: int * generation: int * TimeSpan
        | CancelChildren

    type private Decision =
        { State: CoordinatorState
          Effects: Effect list }

    let private noEffects (state: CoordinatorState) = { State = state; Effects = [] }

    let private addEvent (timeProvider: TimeProvider) kind childId reason (state: CoordinatorState) =
        { state with
            EventsReversed =
                { Kind = kind
                  ChildId = childId
                  Reason = reason
                  At = timeProvider.GetUtcNow() }
                :: state.EventsReversed }

    let private updateChild (child: ChildRuntime) (state: CoordinatorState) =
        { state with
            Children = Map.add child.Index child state.Children }

    let private withEffects effects (state: CoordinatorState) =
        let asynchronousCount =
            effects
            |> List.sumBy (function
                | CancelChildren -> 0
                | _ -> 1)

        { State =
            { state with
                OutstandingEffects = state.OutstandingEffects + asynchronousCount }
          Effects = effects }

    let private effectFinished (state: CoordinatorState) =
        { state with
            OutstandingEffects = state.OutstandingEffects - 1 }

    /// Starts a child, applying its optional startup policy: jittered Polly retry
    /// around a per-attempt startup timeout. Caller cancellation is never retried.
    let private startChild (childSpec: ChildSpec) (token: CancellationToken) : Task<ISupervisedChild> =
        match childSpec.StartupRetry with
        | None -> childSpec.Start token
        | Some startup ->
            let builder = ResiliencePipelineBuilder<ISupervisedChild>()

            if startup.MaxAttempts > 1 then
                let retry = RetryStrategyOptions<ISupervisedChild>()

                retry.ShouldHandle <-
                    fun (args: RetryPredicateArguments<ISupervisedChild>) ->
                        let handled =
                            match args.Outcome.Exception with
                            | null -> false
                            | :? OperationCanceledException -> false
                            | _ -> true

                        ValueTask<bool>(handled)

                retry.MaxRetryAttempts <- startup.MaxAttempts - 1
                retry.Delay <- startup.BaseDelay
                retry.MaxDelay <- startup.MaxDelay
                retry.UseJitter <- startup.UseJitter
                builder.AddRetry(retry) |> ignore

            let timeout = TimeoutStrategyOptions(Timeout = startup.StartupTimeout)
            let pipeline = builder.AddTimeout(timeout).Build()

            let callback =
                Func<CancellationToken, ValueTask<ISupervisedChild>>(fun callbackToken ->
                    ValueTask<ISupervisedChild>(childSpec.Start callbackToken))

            pipeline.ExecuteAsync(callback, token).AsTask()

    /// Best-effort child stop: give it its shutdown budget, then its completion a
    /// short grace. Canceling the monitor after that grace makes abandonment bounded.
    let private stopChild (timeProvider: TimeProvider) (childSpec: ChildSpec) (instance: ChildInstance) : Task =
        task {
            use budget = new CancellationTokenSource(childSpec.Shutdown)

            let stopTask =
                match TaskOutcome.captureSync (fun () -> instance.Child.StopAsync(budget.Token)) with
                | Ok work -> work
                | Error _ -> Task.CompletedTask

            let stopDeadline =
                Task.Delay(childSpec.Shutdown, timeProvider, CancellationToken.None)

            let! _ = Task.WhenAny(stopTask, stopDeadline)
            budget.Cancel()

            let graceDeadline =
                Task.Delay(TimeSpan.FromSeconds 1., timeProvider, CancellationToken.None)

            let! _ = Task.WhenAny(instance.Child.Completion, graceDeadline)
            instance.MonitorCancellation.Cancel()
        }

    type private SupervisorInstance(spec: SupervisorSpec, timeProvider: TimeProvider, ct: CancellationToken) =
        let childrenCancellation = new CancellationTokenSource()

        let startup =
            TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

        let completion =
            TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

        let initialChildren =
            spec.Children
            |> List.mapi (fun index childSpec ->
                index,
                { Spec = childSpec
                  Index = index
                  Generation = 0
                  Lifecycle = NotStarted })
            |> Map.ofList

        let initialState =
            { Children = initialChildren
              InitialNext = if List.isEmpty spec.Children then None else Some 0
              InitialComplete = List.isEmpty spec.Children
              Restarts = []
              EventsReversed = []
              Mode = Active
              OutstandingEffects = 0
              StopWaiters = []
              Registration = None }

        let agent =
            MailboxProcessor.Start(fun inbox ->
                let startEffect index generation purpose childSpec =
                    task {
                        let! result = startChild childSpec childrenCancellation.Token |> TaskOutcome.capture

                        inbox.Post(StartFinished(index, generation, purpose, result))
                    }
                    |> ignore<Task>

                let monitorEffect index instance =
                    task {
                        let! outcome =
                            instance.Child.Completion.WaitAsync(instance.MonitorCancellation.Token)
                            |> TaskOutcome.captureUnit

                        let exit =
                            match outcome with
                            | Ok() -> ExitNormal
                            | Error(CanceledBy instance.MonitorCancellation.Token) -> ExitCanceled
                            | Error error -> ExitAbnormal error

                        inbox.Post(ChildExited(index, instance.Generation, exit))
                    }
                    |> ignore<Task>

                let stopEffect index childSpec instance afterStop =
                    task {
                        let! outcome = stopChild timeProvider childSpec instance |> TaskOutcome.captureUnit

                        match outcome with
                        | Ok() -> ()
                        | Error _ -> instance.MonitorCancellation.Cancel()

                        inbox.Post(ChildStopFinished(index, instance.Generation, afterStop))
                    }
                    |> ignore<Task>

                let delayEffect index generation delay =
                    task {
                        let! outcome =
                            Task.Delay(delay, timeProvider, childrenCancellation.Token)
                            |> TaskOutcome.captureUnit

                        match outcome with
                        | Ok()
                        | Error(CanceledBy childrenCancellation.Token) -> ()
                        | Error unexpected -> return raise unexpected

                        inbox.Post(RestartDelayElapsed(index, generation))
                    }
                    |> ignore<Task>

                let executeEffect =
                    function
                    | StartChild(index, generation, purpose, childSpec) ->
                        startEffect index generation purpose childSpec
                    | MonitorChild(index, instance) -> monitorEffect index instance
                    | StopChild(index, childSpec, instance, afterStop) -> stopEffect index childSpec instance afterStop
                    | WaitForRestart(index, generation, delay) -> delayEffect index generation delay
                    | CancelChildren -> childrenCancellation.Cancel()

                let startRuntime purpose child state =
                    let generation = child.Generation

                    let starting =
                        { child with
                            Lifecycle = Starting(generation, purpose) }

                    updateChild starting state
                    |> withEffects [ StartChild(child.Index, generation, purpose, child.Spec) ]

                let rec beginNextInitial state =
                    match state.InitialNext, state.Mode with
                    | Some index, (Active | TerminatingStrategy _) ->
                        match Map.tryFind index state.Children with
                        | Some child when child.Lifecycle = NotStarted -> startRuntime InitialStart child state
                        | _ -> noEffects state
                    | _ -> noEffects state

                let settle settlement state =
                    state.Registration |> Option.iter (fun registration -> registration.Dispose())

                    match settlement with
                    | CompleteSuccessfully -> completion.TrySetResult() |> ignore
                    | CompleteCanceled token -> completion.TrySetCanceled(token) |> ignore
                    | CompleteWithError error -> completion.TrySetException(error) |> ignore

                    state.StopWaiters
                    |> List.iter (fun waiter ->
                        match settlement with
                        | CompleteWithError error -> waiter.TrySetException(error) |> ignore
                        | CompleteCanceled token -> waiter.TrySetCanceled(token) |> ignore
                        | CompleteSuccessfully -> waiter.TrySetResult() |> ignore)

                    { state with
                        Mode = Settled
                        StopWaiters = []
                        Registration = None }

                let trySettle state =
                    match state.Mode with
                    | ShuttingDown([], settlement) when state.OutstandingEffects = 0 -> settle settlement state
                    | Active when state.InitialComplete && state.OutstandingEffects = 0 ->
                        let allInactive =
                            state.Children |> Map.forall (fun _ child -> child.Lifecycle = Inactive)

                        if allInactive then
                            settle CompleteSuccessfully state
                        else
                            state
                    | _ -> state

                let beginShutdown settlement state =
                    match state.Mode with
                    | Settled -> noEffects state
                    | ShuttingDown _ -> noEffects state
                    | Active
                    | TerminatingStrategy _
                    | RestartingStrategy _ ->
                        let pending = state.Children |> Map.toList |> List.map fst |> List.sortDescending

                        { state with
                            Mode = ShuttingDown(pending, settlement) }
                        |> withEffects [ CancelChildren ]

                let escalate child reason state =
                    match state.Mode with
                    | ShuttingDown _
                    | Settled -> noEffects state
                    | Active
                    | TerminatingStrategy _
                    | RestartingStrategy _ ->
                        let error = SupervisorEscalated(child.Spec.Id, reason)
                        startup.TrySetException(error) |> ignore

                        state
                        |> addEvent timeProvider Escalated child.Spec.Id reason
                        |> beginShutdown (CompleteWithError error)

                let reserveRestart child state =
                    let now = timeProvider.GetUtcNow()
                    let horizon = now - spec.Period

                    let currentWindow =
                        state.Restarts |> List.filter (fun restartedAt -> restartedAt > horizon)

                    if List.length currentWindow >= spec.Intensity then
                        Error state
                    else
                        let generation = child.Generation + 1

                        let awaiting =
                            { child with
                                Generation = generation
                                Lifecycle = AwaitingRestart generation }

                        Ok(
                            generation,
                            { updateChild awaiting state with
                                Restarts = now :: currentWindow }
                        )

                let combine first second =
                    { State = second.State
                      Effects = first.Effects @ second.Effects }

                let rec advanceShutdown state =
                    match state.Mode with
                    | ShuttingDown(index :: remaining, settlement) ->
                        let child = state.Children[index]

                        match child.Lifecycle with
                        | Running instance ->
                            let stopping =
                                { child with
                                    Lifecycle = Stopping(instance, ShutdownStop) }

                            updateChild stopping state
                            |> withEffects [ StopChild(index, child.Spec, instance, ShutdownStop) ]
                        | Starting _
                        | Stopping _ -> noEffects state
                        | NotStarted
                        | DelayingRestart _
                        | AwaitingRestart _
                        | Inactive ->
                            let inactive = { child with Lifecycle = Inactive }

                            updateChild
                                inactive
                                { state with
                                    Mode = ShuttingDown(remaining, settlement) }
                            |> advanceShutdown
                    | ShuttingDown([], _) -> noEffects (trySettle state)
                    | _ -> noEffects state

                let escalateAndAdvance child reason state =
                    let beginning = escalate child reason state
                    combine beginning (advanceShutdown beginning.State)

                let startReserved index state =
                    let child = state.Children[index]

                    match child.Lifecycle with
                    | AwaitingRestart generation -> startRuntime RestartStart child state
                    | _ -> noEffects state

                let rec advanceStrategyRestarts state =
                    match state.Mode with
                    | RestartingStrategy [] -> noEffects { state with Mode = Active }
                    | RestartingStrategy(index :: _) ->
                        let child = state.Children[index]

                        match child.Lifecycle with
                        | AwaitingRestart _ -> startReserved index state
                        | Inactive ->
                            match reserveRestart child state with
                            | Ok(_, reservedState) -> advanceStrategyRestarts reservedState
                            | Error deniedState -> escalateAndAdvance child "restart intensity exceeded" deniedState
                        | Starting _ -> noEffects state
                        | NotStarted
                        | Running _
                        | DelayingRestart _
                        | Stopping _ -> noEffects state
                    | _ -> noEffects state

                let rec advanceStrategy state =
                    match state.Mode with
                    | TerminatingStrategy barrier ->
                        match barrier.PendingStops with
                        | [] ->
                            { state with
                                Mode = RestartingStrategy barrier.RestartIndices }
                            |> advanceStrategyRestarts
                        | index :: remaining ->
                            let child = state.Children[index]

                            match child.Lifecycle with
                            | Running instance ->
                                let stopping =
                                    { child with
                                        Lifecycle = Stopping(instance, StrategyStop) }

                                updateChild stopping state
                                |> withEffects [ StopChild(index, child.Spec, instance, StrategyStop) ]
                            | Starting _
                            | Stopping _
                            | NotStarted -> noEffects state
                            | DelayingRestart _
                            | AwaitingRestart _
                            | Inactive ->
                                let inactive = { child with Lifecycle = Inactive }

                                updateChild
                                    inactive
                                    { state with
                                        Mode =
                                            TerminatingStrategy
                                                { barrier with
                                                    PendingStops = remaining } }
                                |> advanceStrategy
                    | _ -> noEffects state

                let beginReservedRestart failedIndex state =
                    let failed = state.Children[failedIndex]

                    match spec.Strategy with
                    | OneForOne -> startReserved failedIndex state
                    | OneForAll
                    | RestForOne ->
                        let affected =
                            state.Children
                            |> Map.toList
                            |> List.map fst
                            |> List.filter (fun index -> spec.Strategy = OneForAll || index >= failedIndex)

                        let restartIndices =
                            affected
                            |> List.filter (fun index -> state.Children[index].Spec.Restart <> Temporary)

                        let barrier =
                            { PendingStops = affected |> List.sortDescending
                              RestartIndices = restartIndices }

                        { state with
                            Mode = TerminatingStrategy barrier }
                        |> advanceStrategy

                let requestRestart index state =
                    let child = state.Children[index]

                    match reserveRestart child state with
                    | Ok(_, reservedState) -> beginReservedRestart index reservedState
                    | Error deniedState -> escalateAndAdvance child "restart intensity exceeded" deniedState

                let afterNaturalExit child exit state =
                    let shouldRestart =
                        match child.Spec.Restart, exit with
                        | Temporary, _ -> false
                        | Transient, (ExitNormal | ExitCanceled) -> false
                        | Permanent, _
                        | Transient, ExitAbnormal _ -> true

                    if shouldRestart then
                        if child.Spec.RestartDelay > TimeSpan.Zero then
                            let delaying =
                                { child with
                                    Lifecycle = DelayingRestart child.Generation }

                            updateChild delaying state
                            |> withEffects [ WaitForRestart(child.Index, child.Generation, child.Spec.RestartDelay) ]
                        else
                            updateChild { child with Lifecycle = Inactive } state
                            |> requestRestart child.Index
                    else
                        let reason =
                            match exit with
                            | ExitAbnormal error -> $"abnormal exit, not restarted: {error.Message}"
                            | ExitNormal
                            | ExitCanceled -> "normal exit, not restarted"

                        updateChild { child with Lifecycle = Inactive } state
                        |> addEvent timeProvider Stopped child.Spec.Id reason
                        |> noEffects

                let handleMessage message state =
                    match message with
                    | Begin ->
                        if state.InitialComplete then
                            startup.TrySetResult() |> ignore
                            noEffects (trySettle state)
                        else
                            beginNextInitial state
                    | InstallRegistration registration ->
                        match state.Mode with
                        | Settled ->
                            registration.Dispose()
                            noEffects state
                        | _ ->
                            noEffects
                                { state with
                                    Registration = Some registration }
                    | Snapshot reply ->
                        reply.Reply(List.rev state.EventsReversed)
                        noEffects state
                    | RequestStop waiter ->
                        match state.Mode with
                        | Settled ->
                            waiter.TrySetResult() |> ignore
                            noEffects state
                        | ShuttingDown _ ->
                            noEffects
                                { state with
                                    StopWaiters = waiter :: state.StopWaiters }
                        | Active
                        | TerminatingStrategy _
                        | RestartingStrategy _ ->
                            let beginning =
                                beginShutdown
                                    CompleteSuccessfully
                                    { state with
                                        StopWaiters = waiter :: state.StopWaiters }

                            combine beginning (advanceShutdown beginning.State)
                    | ParentCanceled ->
                        match state.Mode with
                        | Settled
                        | ShuttingDown _ -> noEffects state
                        | Active
                        | TerminatingStrategy _
                        | RestartingStrategy _ ->
                            let settlement =
                                if state.InitialComplete then
                                    CompleteSuccessfully
                                else
                                    CompleteCanceled ct

                            if not state.InitialComplete then
                                startup.TrySetCanceled(ct) |> ignore

                            let beginning = beginShutdown settlement state
                            combine beginning (advanceShutdown beginning.State)
                    | StartFinished(index, generation, purpose, result) ->
                        let state = effectFinished state
                        let child = state.Children[index]

                        match child.Lifecycle with
                        | Starting(expectedGeneration, expectedPurpose) when
                            expectedGeneration = generation && expectedPurpose = purpose
                            ->
                            match result with
                            | Error error ->
                                let inactiveState = updateChild { child with Lifecycle = Inactive } state

                                match inactiveState.Mode with
                                | ShuttingDown _ -> advanceShutdown inactiveState
                                | Active
                                | TerminatingStrategy _
                                | RestartingStrategy _ ->
                                    escalateAndAdvance child $"start failed: {error.Message}" inactiveState
                                | Settled -> noEffects inactiveState
                            | Ok supervisedChild ->
                                let monitorCancellation = new CancellationTokenSource()

                                let instance =
                                    { Generation = generation
                                      Child = supervisedChild
                                      MonitorCancellation = monitorCancellation }

                                let eventKind, reason =
                                    match purpose with
                                    | InitialStart -> Started, "initial start"
                                    | RestartStart -> Restarted, "restart"

                                let runningState =
                                    updateChild
                                        { child with
                                            Lifecycle = Running instance }
                                        state
                                    |> addEvent timeProvider eventKind child.Spec.Id reason

                                let afterStart =
                                    match purpose, runningState.Mode with
                                    | RestartStart, RestartingStrategy(current :: remaining) when current = index ->
                                        noEffects
                                            { runningState with
                                                Mode = RestartingStrategy remaining }
                                    | RestartStart, _ -> noEffects runningState
                                    | InitialStart, _ ->
                                        let nextIndex = index + 1
                                        let complete = nextIndex >= spec.Children.Length

                                        if complete then
                                            startup.TrySetResult() |> ignore

                                        let advanced =
                                            { runningState with
                                                InitialNext = if complete then None else Some nextIndex
                                                InitialComplete = complete }

                                        if complete then
                                            noEffects advanced
                                        else
                                            beginNextInitial advanced

                                let monitored = withEffects [ MonitorChild(index, instance) ] afterStart.State

                                let advanced =
                                    match monitored.State.Mode with
                                    | ShuttingDown _ -> advanceShutdown monitored.State
                                    | TerminatingStrategy _ -> advanceStrategy monitored.State
                                    | RestartingStrategy _ -> advanceStrategyRestarts monitored.State
                                    | Active
                                    | Settled -> noEffects (trySettle monitored.State)

                                { State = advanced.State
                                  Effects = afterStart.Effects @ monitored.Effects @ advanced.Effects }
                        | _ ->
                            match result with
                            | Error _ -> noEffects (trySettle state)
                            | Ok supervisedChild ->
                                // A stale successful start still owns a live child; route it through
                                // the normal bounded stop path before allowing Completion to settle.
                                let monitorCancellation = new CancellationTokenSource()

                                let instance =
                                    { Generation = generation
                                      Child = supervisedChild
                                      MonitorCancellation = monitorCancellation }

                                withEffects
                                    [ MonitorChild(index, instance)
                                      StopChild(index, child.Spec, instance, ShutdownStop) ]
                                    state
                    | ChildExited(index, generation, exit) ->
                        let state = effectFinished state
                        let child = state.Children[index]

                        match child.Lifecycle with
                        | Running instance when instance.Generation = generation ->
                            instance.MonitorCancellation.Dispose()

                            match state.Mode with
                            | Active
                            | RestartingStrategy _ -> afterNaturalExit child exit state
                            | TerminatingStrategy _ ->
                                updateChild { child with Lifecycle = Inactive } state |> advanceStrategy
                            | ShuttingDown _ ->
                                updateChild { child with Lifecycle = Inactive } state
                                |> addEvent timeProvider Stopped child.Spec.Id "shutdown"
                                |> advanceShutdown
                            | Settled -> noEffects state
                        | Stopping(instance, _) when instance.Generation = generation -> noEffects (trySettle state)
                        | _ -> noEffects (trySettle state)
                    | RestartDelayElapsed(index, generation) ->
                        let state = effectFinished state
                        let child = state.Children[index]

                        match child.Lifecycle with
                        | DelayingRestart expectedGeneration when expectedGeneration = generation ->
                            let inactiveState = updateChild { child with Lifecycle = Inactive } state

                            match state.Mode with
                            | Active
                            | RestartingStrategy _ -> requestRestart index inactiveState
                            | TerminatingStrategy _ -> advanceStrategy inactiveState
                            | ShuttingDown _ -> advanceShutdown inactiveState
                            | Settled -> noEffects inactiveState
                        | _ -> noEffects (trySettle state)
                    | ChildStopFinished(index, generation, afterStop) ->
                        let state = effectFinished state
                        let child = state.Children[index]

                        match child.Lifecycle with
                        | Stopping(instance, expectedAfterStop) when
                            instance.Generation = generation && expectedAfterStop = afterStop
                            ->
                            instance.MonitorCancellation.Dispose()

                            let inactiveState = updateChild { child with Lifecycle = Inactive } state

                            match afterStop, inactiveState.Mode with
                            | StrategyStop, TerminatingStrategy _ -> advanceStrategy inactiveState
                            | StrategyStop, ShuttingDown _
                            | ShutdownStop, ShuttingDown _ ->
                                inactiveState
                                |> addEvent timeProvider Stopped child.Spec.Id "shutdown"
                                |> advanceShutdown
                            | _ -> noEffects (trySettle inactiveState)
                        | _ -> noEffects (trySettle state)

                let rec loop state =
                    async {
                        let! message = inbox.Receive()
                        let decision = handleMessage message state
                        decision.Effects |> List.iter executeEffect
                        return! loop (trySettle decision.State)
                    }

                loop initialState)

        let registration = ct.Register(fun () -> agent.Post ParentCanceled)
        do agent.Post(InstallRegistration registration)

        member _.Begin() : Task =
            agent.Post Begin
            startup.Task

        interface ISupervisor with

            member _.Completion: Task = completion.Task

            member _.StopAsync(stopToken: CancellationToken) : Task<unit> =
                task {
                    let waiter =
                        TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

                    agent.Post(RequestStop waiter)
                    do! waiter.Task.WaitAsync(stopToken)
                }

            member _.Events() : SupervisionEvent list = agent.PostAndReply Snapshot

            member _.EventsAsync(ct: CancellationToken) : Task<SupervisionEvent list> =
                agent.PostAndAsyncReply(fun reply -> Snapshot reply)
                |> fun operation -> Async.StartImmediateAsTask(operation, cancellationToken = ct)

    /// <summary>
    /// Starts a supervisor: children start sequentially in declaration order, and the
    /// returned task completes once every initial start succeeded. A child that cannot be
    /// first-started (after its startup policy) escalates: everything is stopped and the
    /// start task faults with
    /// <see cref="T:ByzantineSystems.Automata.Resilience.SupervisorEscalated" />.
    /// </summary>
    /// <exception cref="T:System.InvalidOperationException">The specification is invalid.</exception>
    let start (spec: SupervisorSpec) (ct: CancellationToken) (timeProvider: TimeProvider) : Task<ISupervisor> =
        match validate spec with
        | Error errors -> invalidOp $"cannot start a supervisor from an invalid specification: {errors}"
        | Ok spec ->
            let instance = SupervisorInstance(spec, timeProvider, ct)

            task {
                do! instance.Begin()
                return instance :> ISupervisor
            }
