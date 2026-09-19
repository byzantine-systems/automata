namespace ByzantineSystems.Automata.Runtime

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Resilience
open ByzantineSystems.Automata.Storage

/// <summary>One statement inside a <c>machine { ... }</c> block.</summary>
type MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> =
    | MachineChart of Chart<'State, 'Event, 'Action, 'Err>
    | MachineInitial of 'State
    | MachineStore of IMachineStore<'EntityId, 'State, 'Event, 'Action>
    | MachineRetry of RetryConfig<'Err>
    | MachineRetryPolicy of RetryPolicy
    | MachineSupervise of SupervisorSpec
    | MachineObserver of TransitionObserver<'EntityId, 'State, 'Event, 'Action>
    | MachineMailboxCapacity of int
    | MachineIdleTimeout of TimeSpan
    | MachineTimeProvider of TimeProvider

/// <summary>Shared assembly step behind the builder: accumulate defects, then construct.</summary>
module private MachineBuild =

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
                | MachineRetry r -> Some r
                | _ -> None)

        let retryPolicy =
            parts
            |> List.tryPick (function
                | MachineRetryPolicy p -> Some p
                | _ -> None)
            |> Option.defaultValue RetryPolicy.defaults

        let supervise =
            parts
            |> List.tryPick (function
                | MachineSupervise s -> Some s
                | _ -> None)

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
            | Some r ->
                match RetryConfig.validate r with
                | Ok _ -> []
                | Error errors -> [ MachineConfigError.InvalidRetry errors ]
            | None -> []

        let retryPolicyErrors =
            match RetryPolicy.validate retryPolicy with
            | Ok _ -> []
            | Error errors -> [ MachineConfigError.InvalidRetryPolicy errors ]

        let superviseErrors =
            match supervise with
            | Some spec ->
                match Supervisor.validate spec with
                | Ok _ -> []
                | Error errors -> [ MachineConfigError.InvalidSupervision errors ]
            | None -> []

        let errors =
            missingErrors
            @ initialErrors
            @ retryErrors
            @ retryPolicyErrors
            @ superviseErrors

        match errors, chart, initial, store, retry with
        | [], Some c, Some s, Some st, Some r ->
            let pipeline = RetryConfig.toPipeline r ignore

            let config =
                { MachineId = machineId
                  Chart = c
                  InitialState = s
                  Store = st :> IStateStore<'EntityId, 'State, 'Event, 'Action>
                  RetryQueue = st :> IRetryQueue<'EntityId, 'Event>
                  DeadLetter = st :> IDeadLetterStore<'EntityId, 'Event>
                  Pipeline = pipeline
                  Classify = r.Classify
                  RetryPolicy = retryPolicy
                  Supervisor = supervise
                  TimeProvider = timeProvider
                  MailboxCapacity = mailboxCapacity
                  IdleTimeout = idleTimeout }

            let signal = new WorkSignal()

            let observerBus =
                observer |> Option.map (fun o -> ObserverDispatcher(o, mailboxCapacity))

            let registry = Registry(config, observerBus, signal)
            Ok(Machine(config, registry, observerBus, signal))
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

    /// <summary>Declares the durable retry-queue policy.</summary>
    let retryPolicy (policy: RetryPolicy) : MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> =
        MachineRetryPolicy policy

    /// <summary>Declares the supervision policy applied when actors are wired to a supervisor (M5).</summary>
    let supervise (spec: SupervisorSpec) : MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> = MachineSupervise spec

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
