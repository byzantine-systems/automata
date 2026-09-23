namespace ByzantineSystems.Automata.Runtime

open System
open System.Text.RegularExpressions
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Logging.Abstractions
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage

/// <summary>One declaration inside a <c>machine</c> expression.</summary>
type MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err when 'EntityId: equality> =
    | MachineChart of Chart<'State, 'Event, 'Action, 'Err>
    | MachineChartVersion of ChartVersion
    | MachineInitial of 'State
    | MachineStore of IMachineStore<'EntityId, 'State, 'Event, 'Action, 'Err>
    | MachineProcessor of ProcessorPolicy<'Err>
    | MachineActionQueue of string
    | MachineObserver of TransitionObserver<'EntityId, 'State, 'Event, 'Action> * capacity: int
    | MachineLogger of ILogger
    | MachineTimeProvider of TimeProvider

module private MachineBuild =

    /// <summary>
    /// Stricter than PostgreSQL's own rules, and deliberately: a queue name is interpolated into
    /// SQL rather than bound, so the only safe set is the one that never needs quoting.
    /// </summary>
    let queueNamePattern = Regex(@"^[a-z_][a-z0-9_]*$", RegexOptions.Compiled)

    /// Default: one observation channel deep enough to absorb a batch without dropping.
    let defaultObserverCapacity = 256

    /// Provisional, and documented as such on Machine.send. The real answer is a notification
    /// from the database, which lands with the maintenance work.
    let defaultResultPollInterval = TimeSpan.FromMilliseconds 50.

    /// Which declaration a part is, so repeats can be counted without knowing what they carry.
    let private declarationOf part =
        match part with
        | MachineChart _ -> MachineDeclaration.Chart
        | MachineChartVersion _ -> MachineDeclaration.ChartVersion
        | MachineInitial _ -> MachineDeclaration.Initial
        | MachineStore _ -> MachineDeclaration.Store
        | MachineProcessor _ -> MachineDeclaration.Processor
        | MachineActionQueue _ -> MachineDeclaration.ActionQueue
        | MachineObserver _ -> MachineDeclaration.Observer
        | MachineLogger _ -> MachineDeclaration.Logger
        | MachineTimeProvider _ -> MachineDeclaration.TimeProvider

    /// Collects every defect rather than stopping at the first, because a half-configured
    /// machine usually has more than one thing wrong with it and reporting them one build at a
    /// time is a poor way to find out.
    let build
        (machineId: MachineId)
        (parts: MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> list)
        : Result<Machine<'EntityId, 'State, 'Event, 'Action, 'Err>, MachineConfigError list> =

        // Every declaration is single-valued, so a repeat is a defect regardless of which one it
        // is. Counting says that once, rather than nine near-identical times.
        let duplicates =
            parts
            |> List.countBy declarationOf
            |> List.filter (fun (_, count) -> count > 1)
            |> List.map (fst >> DuplicateDeclaration)

        let chart =
            parts
            |> List.tryPick (function
                | MachineChart chart -> Some chart
                | _ -> None)

        let version =
            parts
            |> List.tryPick (function
                | MachineChartVersion version -> Some version
                | _ -> None)

        let initial =
            parts
            |> List.tryPick (function
                | MachineInitial state -> Some state
                | _ -> None)

        let store =
            parts
            |> List.tryPick (function
                | MachineStore store -> Some store
                | _ -> None)

        let processor =
            parts
            |> List.tryPick (function
                | MachineProcessor policy -> Some policy
                | _ -> None)

        let actionQueue =
            parts
            |> List.tryPick (function
                | MachineActionQueue name -> Some name
                | _ -> None)

        let observer =
            parts
            |> List.tryPick (function
                | MachineObserver(observer, capacity) -> Some(observer, capacity)
                | _ -> None)

        let logger =
            parts
            |> List.tryPick (function
                | MachineLogger logger -> Some logger
                | _ -> None)

        let timeProvider =
            parts
            |> List.tryPick (function
                | MachineTimeProvider provider -> Some provider
                | _ -> None)

        let missing =
            [ if Option.isNone chart then
                  MissingChart

              if Option.isNone version then
                  MissingChartVersion

              if Option.isNone initial then
                  MissingInitialState

              if Option.isNone store then
                  MissingStore

              match actionQueue with
              | None -> MissingActionQueue
              | Some name when not (queueNamePattern.IsMatch name) -> InvalidActionQueueName name
              | Some _ -> () ]

        let policy =
            processor |> Option.defaultValue ProcessorPolicy.defaults

        let policyErrors =
            match ProcessorPolicy.validate policy with
            | Ok _ -> []
            | Error errors -> [ InvalidProcessorPolicy errors ]

        // The initial state has to classify to a declared leaf, or the machine's very first
        // command resolves against a state the chart does not contain. Resolution starts and
        // finishes only at a leaf, so a compound node is as wrong here as an undeclared one.
        let initialStateErrors =
            match chart, initial with
            | Some chart, Some initial ->
                let leaf = Chart.classifyOf chart initial

                let declaredLeaf =
                    Chart.tryNode chart leaf |> Option.isSome
                    && Chart.children chart leaf |> List.isEmpty

                if declaredLeaf then [] else [ InitialStateUnknown leaf ]
            | _ -> []

        match duplicates @ missing @ policyErrors @ initialStateErrors with
        | [] ->
            match chart, version, initial, store, actionQueue, ProcessorPolicy.validate policy with
            | Some chart, Some version, Some initial, Some store, Some actionQueue, Ok validatedPolicy ->
                let config: RuntimeConfig<'EntityId, 'State, 'Event, 'Action, 'Err> =
                    { MachineId = machineId
                      Chart = chart
                      ChartVersion = version
                      InitialState = initial
                      Store = store
                      Processor = validatedPolicy
                      ActionQueue = actionQueue
                      Logger = logger |> Option.defaultValue (NullLogger.Instance :> ILogger)
                      TimeProvider = timeProvider |> Option.defaultValue TimeProvider.System }

                let observerBus =
                    observer
                    |> Option.map (fun (observer, capacity) -> ObserverDispatcher(observer, capacity))

                Ok(
                    Machine(
                        config,
                        Completions(),
                        observerBus,
                        new WorkSignal(),
                        new WorkSignal(),
                        defaultResultPollInterval
                    )
                )
            | _ ->
                // Unreachable: the lists above are empty only when every required part is
                // present. Reported rather than asserted, on the principle that a machine that
                // refuses to build is better than one that builds wrong.
                Error [ MissingStore ]
        | errors -> Error errors

/// <summary>The builder behind the <c>machine</c> expression.</summary>
type MachineBuilder<'EntityId, 'State, 'Event, 'Action, 'Err when 'EntityId: equality>(machineId: MachineId) =

    member _.Yield(_: unit) : MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> list = []

    member _.Zero() : MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> list = []

    member _.Combine
        (
            left: MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> list,
            right: MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> list
        ) =
        left @ right

    member _.Delay(f: unit -> MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> list) = f ()

    member _.Run(parts: MachinePart<'EntityId, 'State, 'Event, 'Action, 'Err> list) =
        MachineBuild.build machineId parts

    [<CustomOperation "chart">]
    member _.Chart(parts, chart: Chart<'State, 'Event, 'Action, 'Err>) = parts @ [ MachineChart chart ]

    [<CustomOperation "chartVersion">]
    member _.ChartVersion(parts, version: int) =
        parts @ [ MachineChartVersion(ChartVersion.create version) ]

    [<CustomOperation "initialState">]
    member _.InitialState(parts, state: 'State) = parts @ [ MachineInitial state ]

    [<CustomOperation "store">]
    member _.Store(parts, store: IMachineStore<'EntityId, 'State, 'Event, 'Action, 'Err>) =
        parts @ [ MachineStore store ]

    [<CustomOperation "processor">]
    member _.Processor(parts, policy: ProcessorPolicy<'Err>) = parts @ [ MachineProcessor policy ]

    [<CustomOperation "actionQueue">]
    member _.ActionQueue(parts, name: string) = parts @ [ MachineActionQueue name ]

    [<CustomOperation "onTransition">]
    member _.OnTransition(parts, observer: TransitionObserver<'EntityId, 'State, 'Event, 'Action>) =
        parts @ [ MachineObserver(observer, MachineBuild.defaultObserverCapacity) ]

    [<CustomOperation "logger">]
    member _.Logger(parts, logger: ILogger) = parts @ [ MachineLogger logger ]

    [<CustomOperation "timeProvider">]
    member _.TimeProvider(parts, provider: TimeProvider) = parts @ [ MachineTimeProvider provider ]

/// <summary>The <c>machine</c> computation expression.</summary>
[<AutoOpen>]
module MachineCE =

    /// <summary>
    /// Declares one machine: a chart, the version commands pin to it, the state a new entity
    /// starts from, the durable store, and the queue its actions are delivered through.
    /// </summary>
    /// <example>
    /// <code lang="fsharp">
    /// let payments =
    ///     machine&lt;PaymentId, PaymentState, PaymentEvent, PaymentAction, PaymentError&gt; (machineId "payments") {
    ///         chart        paymentChart
    ///         chartVersion 3
    ///         initialState Pending
    ///         store        postgresStore
    ///         actionQueue  "payment_actions"
    ///     }
    /// </code>
    /// </example>
    let machine<'EntityId, 'State, 'Event, 'Action, 'Err when 'EntityId: equality> (id: MachineId) =
        MachineBuilder<'EntityId, 'State, 'Event, 'Action, 'Err>(id)
