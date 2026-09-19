namespace ByzantineSystems.Automata.Runtime

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Resilience
open ByzantineSystems.Automata.Storage
open Polly

/// <summary>One statement inside a <c>machine { ... }</c> block.</summary>
type MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> =
    | MachineChart of Chart<'State, 'Event, 'Action, 'Err>
    | MachineInitial of 'State
    | MachineStore of IMachineStore<'EntityId, 'State, 'Event, 'Action>
    | MachineRetry of RetryConfig<'Err>
    | MachinePipeline of ResiliencePipeline<PipelineResult<'Err>> * (MachineError<'Err> -> Disposition)
    | MachineRetryPolicy of RetryPolicy
    | MachineObserver of TransitionObserver<'EntityId, 'State, 'Event, 'Action>
    | MachineMailboxCapacity of int
    | MachineIdleTimeout of TimeSpan
    | MachineTimeProvider of TimeProvider

/// <summary>Shared assembly step behind the builder: accumulate defects, then construct.</summary>
module private MachineBuild =

    let private duplicateErrors parts =
        let declaration =
            function
            | MachineChart _ -> MachineDeclaration.Chart
            | MachineInitial _ -> MachineDeclaration.Initial
            | MachineStore _ -> MachineDeclaration.Store
            | MachineRetry _ -> MachineDeclaration.Retry
            | MachinePipeline _ -> MachineDeclaration.Retry
            | MachineRetryPolicy _ -> MachineDeclaration.RetryPolicy
            | MachineObserver _ -> MachineDeclaration.Observer
            | MachineMailboxCapacity _ -> MachineDeclaration.MailboxCapacity
            | MachineIdleTimeout _ -> MachineDeclaration.IdleTimeout
            | MachineTimeProvider _ -> MachineDeclaration.TimeProvider

        parts
        |> List.countBy declaration
        |> List.choose (fun (name, count) ->
            if count > 1 then
                Some(MachineConfigError.DuplicateDeclaration name)
            else
                None)

    let internal build
        (machineId: MachineId)
        (parts: MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> list)
        : Result<Machine<'EntityId, 'State, 'Event, 'Action, 'Err>, MachineConfigError list> =
        let chart =
            parts
            |> List.tryPick (function
                | MachineChart c -> Some c
                | _ -> None)

        let initial =
            parts
            |> List.tryPick (function
                | MachineInitial s -> Some s
                | _ -> None)

        let store =
            parts
            |> List.tryPick (function
                | MachineStore s -> Some s
                | _ -> None)

        let retry =
            parts
            |> List.tryPick (function
                | MachineRetry config -> Some(Choice1Of2 config)
                | MachinePipeline(pipeline, classify) -> Some(Choice2Of2(pipeline, classify))
                | _ -> None)

        let retryPolicy =
            parts
            |> List.tryPick (function
                | MachineRetryPolicy p -> Some p
                | _ -> None)
            |> Option.defaultValue RetryPolicy.defaults

        let observer =
            parts
            |> List.tryPick (function
                | MachineObserver o -> Some o
                | _ -> None)

        let mailboxCapacity =
            parts
            |> List.tryPick (function
                | MachineMailboxCapacity n -> Some n
                | _ -> None)
            |> Option.defaultValue 1024

        let idleTimeout =
            parts
            |> List.tryPick (function
                | MachineIdleTimeout t -> Some t
                | _ -> None)
            |> Option.defaultValue (TimeSpan.FromMinutes 5.)

        let timeProvider =
            parts
            |> List.tryPick (function
                | MachineTimeProvider tp -> Some tp
                | _ -> None)
            |> Option.defaultValue TimeProvider.System

        let missingErrors =
            [ if chart.IsNone then
                  MachineConfigError.MissingChart

              if initial.IsNone then
                  MachineConfigError.MissingInitialState

              if store.IsNone then
                  MachineConfigError.MissingStore

              if retry.IsNone then
                  MachineConfigError.MissingRetry

              if mailboxCapacity < 1 then
                  MachineConfigError.MailboxCapacityBelowOne mailboxCapacity

              if idleTimeout <= TimeSpan.Zero then
                  MachineConfigError.IdleTimeoutNotPositive idleTimeout ]

        let initialErrors =
            match chart, initial with
            | Some c, Some s ->
                let leaf = Chart.classifyOf c s

                match Chart.tryNode c leaf with
                | Some _ when not (Chart.isCompound c leaf) -> []
                | _ -> [ MachineConfigError.InitialStateUnknown leaf ]
            | _ -> []

        let retryErrors =
            match retry with
            | Some(Choice1Of2 r) ->
                match RetryConfig.validate r with
                | Ok _ -> []
                | Error errors -> [ MachineConfigError.InvalidRetry errors ]
            | Some(Choice2Of2 _) -> []
            | None -> []

        let validatedRetryPolicy, retryPolicyErrors =
            match RetryPolicy.validate retryPolicy with
            | Ok validated -> Some validated, []
            | Error errors -> None, [ MachineConfigError.InvalidRetryPolicy errors ]

        let errors =
            duplicateErrors parts
            @ missingErrors
            @ initialErrors
            @ retryErrors
            @ retryPolicyErrors

        match errors, chart, initial, store, retry, validatedRetryPolicy with
        | [], Some c, Some s, Some st, Some configuredRetry, Some policy ->
            let pipeline, classify =
                match configuredRetry with
                | Choice1Of2 retry -> RetryConfig.toPipeline retry ignore, retry.Classify
                | Choice2Of2(pipeline, classify) -> pipeline, classify

            let config =
                { MachineId = machineId
                  Chart = c
                  InitialState = s
                  Store = st :> IStateStore<'EntityId, 'State, 'Event, 'Action>
                  RetryQueue = st :> IRetryQueue<'EntityId, 'Event>
                  DeadLetter = st :> IDeadLetterStore<'EntityId, 'Event>
                  Outbox = st :> IActionOutbox<'EntityId, 'Action>
                  Pipeline = pipeline
                  Classify = classify
                  RetryPolicy = policy
                  TimeProvider = timeProvider
                  MailboxCapacity = mailboxCapacity
                  IdleTimeout = idleTimeout }

            let retrySignal = new WorkSignal()
            let outboxSignal = new WorkSignal()

            let observerBus =
                observer |> Option.map (fun o -> ObserverDispatcher(o, mailboxCapacity))

            let registry = Registry(config, observerBus, retrySignal, outboxSignal)
            Ok(Machine(config, registry, observerBus, retrySignal, outboxSignal))
        | _ -> Error errors

/// <summary>
/// Builder for <c>machine { ... }</c>, returning
/// <c>Result&lt;Machine, MachineConfigError list&gt;</c>. Defects accumulate instead of
/// throwing, and the shared resilience pipeline is built exactly once.
/// </summary>
type MachineBuilder<'EntityId, 'State, 'Event, 'Action, 'Err when 'EntityId: equality>(machineId: MachineId) =

    member _.Yield
        (part: MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err>)
        : MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> list =
        [ part ]

    member _.Yield(()) : MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> list = []
    member _.Zero() : MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> list = []

    member _.Combine
        (
            a: MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> list,
            b: MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> list
        ) : MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> list =
        a @ b

    member _.Delay
        (f: unit -> MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> list)
        : MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> list =
        f ()

    member _.Run
        (parts: MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> list)
        : Result<Machine<'EntityId, 'State, 'Event, 'Action, 'Err>, MachineConfigError list> =
        MachineBuild.build machineId parts

/// <summary>Syntax surface for the <c>machine</c> computation expression.</summary>
[<AutoOpen>]
module MachineCE =

    /// <summary>The machine builder value: <c>machine&lt;...&gt; (machineId "name") { ... }</c>.</summary>
    let machine<'EntityId, 'State, 'Event, 'Action, 'Err when 'EntityId: equality> (id: MachineId) =
        MachineBuilder<'EntityId, 'State, 'Event, 'Action, 'Err>(id)

    /// <summary>Declares the validated chart the machine runs.</summary>
    let chart (chart: Chart<'State, 'Event, 'Action, 'Err>) : MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> =
        MachineChart chart

    /// <summary>Declares the state a brand-new entity starts from.</summary>
    let initialState (state: 'State) : MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> = MachineInitial state

    /// <summary>Declares the durable store backing state, retry, dead-letter, and outbox.</summary>
    let store
        (store: IMachineStore<'EntityId, 'State, 'Event, 'Action>)
        : MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> =
        MachineStore store

    /// <summary>Declares the short-horizon Polly resilience policy.</summary>
    let retry (retry: RetryConfig<'Err>) : MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> = MachineRetry retry

    /// <summary>Uses a pre-built shared resilience pipeline, such as a keyed DI registration.</summary>
    let resiliencePipeline
        (pipeline: ResiliencePipeline<PipelineResult<'Err>>)
        (classify: MachineError<'Err> -> Disposition)
        : MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> =
        MachinePipeline(pipeline, classify)

    /// <summary>Declares the durable retry-queue policy.</summary>
    let retryPolicy (policy: RetryPolicy) : MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> =
        MachineRetryPolicy policy

    /// <summary>Declares a best-effort, post-commit transition observer.</summary>
    let onTransition
        (observer: TransitionObserver<'EntityId, 'State, 'Event, 'Action>)
        : MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> =
        MachineObserver observer

    /// <summary>Declares the per-entity mailbox capacity.</summary>
    let mailboxCapacity (capacity: int) : MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> =
        MachineMailboxCapacity capacity

    /// <summary>Declares how long an actor may sit idle before eviction.</summary>
    let idleTimeout (timeout: TimeSpan) : MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> =
        MachineIdleTimeout timeout

    /// <summary>Declares the clock the runtime uses for leases and stamps.</summary>
    let timeProvider (provider: TimeProvider) : MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> =
        MachineTimeProvider provider
