namespace ByzantineSystems.Automata.Runtime

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage

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
        signal: WorkSignal
    ) =

    let lifetime = new CancellationTokenSource()

    let observerTask =
        observerBus |> Option.map (fun bus -> bus.RunAsync(lifetime.Token))

    member internal _.RuntimeConfig = config
    member internal _.RegistryValue = registry
    member internal _.SignalValue = signal
    member internal _.ObserverBusValue = observerBus

    member _.MachineId: MachineId = config.MachineId

    /// <summary>Stops the observer loop, the wake signal, and every entity actor.</summary>
    member _.StopAsync(ct: CancellationToken) : Task =
        task {
            match observerBus with
            | Some bus -> bus.Complete()
            | None -> ()

            lifetime.Cancel()

            match observerTask with
            | Some t ->
                try
                    do! t.WaitAsync(ct)
                with _ ->
                    ()
            | None -> ()

            signal.Complete()
            do! registry.StopAsync(ct)
        }

    interface IAsyncDisposable with

        member this.DisposeAsync() : ValueTask =
            ValueTask(this.StopAsync(CancellationToken.None))

/// <summary>The public surface of a <see cref="T:ByzantineSystems.Automata.Runtime.Machine`5" />.</summary>
[<RequireQualifiedAccess>]
module Machine =

    /// <summary>
    /// Sends one event to an entity. The result distinguishes a fresh commit, an already
    /// applied idempotency key, a durable deferral, and an explicit ignore. Cancellation
    /// before mailbox admission never enqueues the event.
    /// </summary>
    let send
        (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>)
        (entityId: 'EntityId)
        (envelope: EventEnvelope<'Event>)
        (ct: CancellationToken)
        : Task<Result<SendOutcome, MachineError<'Err>>> =
        machine.RegistryValue.Send(entityId, envelope, ct)

    /// <summary>Returns the latest committed snapshot for an entity, or <c>None</c>.</summary>
    let state
        (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>)
        (entityId: 'EntityId)
        (ct: CancellationToken)
        : Task<Result<Snapshot<'State> option, MachineError<'Err>>> =
        machine.RegistryValue.State(entityId, ct)

    /// <summary>The logical machine name the chart and store are keyed by.</summary>
    let machineId (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>) : MachineId = machine.MachineId

    /// <summary>Stops the machine: observer, signal, then every actor loop.</summary>
    let stopAsync (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>) (ct: CancellationToken) : Task =
        machine.StopAsync(ct)

    let internal config (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>) = machine.RuntimeConfig
    let internal signal (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>) = machine.SignalValue

    let internal observerBus
        (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>)
        : ObserverDispatcher<'EntityId, 'State, 'Event, 'Action> option =
        machine.ObserverBusValue
