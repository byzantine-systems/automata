module ByzantineSystems.Automata.DependencyInjection.Tests.TestSupport

open System
open System.Collections.Concurrent
open System.Diagnostics
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Resilience
open ByzantineSystems.Automata.Runtime
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.DependencyInjection
open Expecto
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting

let noCancellation = CancellationToken.None
let waitTimeout = TimeSpan.FromSeconds 10.

/// Phantom marker so entity ids are tied to a domain type in tests.
type TestEntity = class end

type TestState =
    | Idle
    | Active of int

type TestEvent = Start of int

type TestAction = Log of string

let private chartResult =
    statechart<TestState, TestEvent, TestAction, string> {
        root "root"

        classify (function
            | Idle -> stateId "idle"
            | Active _ -> stateId "active")

        state "idle" {
            on (fun _ _ -> true) (fun _ e ->
                match e with
                | Start n -> [ Log "first"; Log "second" ], Active n)
        }

        state "active" { on (fun _ _ -> false) (fun s _ -> [], s) }
    }

let testChart =
    match chartResult with
    | Ok c -> c
    | Error errors -> failtestf "test chart failed to construct: %A" errors

type Entity = EntityId<TestEntity>
type MachineStore = IMachineStore<Entity, TestState, TestEvent, TestAction, string>
type TestMachine = Machine<Entity, TestState, TestEvent, TestAction, string>
type TestOptions = AutomataOptions<Entity, TestState, TestEvent, TestAction, string, string>

/// <summary>
/// A store that answers but stores nothing.
///
/// These tests are about the host wiring: that a hosted service is registered, that a
/// generation starts and stops, that a handler is resolved from a scope. None of that needs a
/// store that works, and one that pretended to would only invite assertions this project has no
/// business making.
/// </summary>
type TestStore() =

    let actions = ConcurrentQueue<LeasedAction<Entity, TestAction>>()

    member val Delivered = ResizeArray<int>() with get

    /// Scripts one action for the dispatcher to pick up.
    member _.OfferAction(action) = actions.Enqueue action

    interface ICommandInbox<Entity, TestEvent> with
        member _.Submit(_, _) =
            Task.FromResult(Ok(Accepted(CommandId.ofInt64 1L)))

        member _.Claim(_, _, _, _) = Task.FromResult(Ok [])
        member _.Reschedule(_, _, _, _) = Task.FromResult(Ok Updated)
        member _.ExtendLease(_, _, _, _) = Task.FromResult(Ok Updated)
        member _.TryGet(_, _) = Task.FromResult(Ok None)
        member _.TryFind(_, _, _, _) = Task.FromResult(Ok None)

    interface IStateReader<Entity, TestState, TestEvent, TestAction> with
        member _.TryGetSnapshot(_, _, _) = Task.FromResult(Ok None)
        member _.History(_, _, _, _) = Task.FromResult(Ok [])

    interface ICommandProcessorStore<Entity, TestState, TestEvent, TestAction, string> with
        member _.Commit(_, _, _, _, _) =
            Task.FromResult(Ok(Finalized(Epoch.ofUInt64 1UL)))

        member _.Reject(_, _, _, _) =
            Task.FromResult(Ok(Finalized Epoch.initial))

        member _.DeadLetter(_, _, _, _) =
            Task.FromResult(Ok(Finalized Epoch.initial))

        member _.TryGetResult(_, _) = Task.FromResult(Ok None)

    interface IActionQueue<Entity, TestAction> with

        member _.Claim(_, batch, _, _) =
            let drained = ResizeArray()
            let mutable taking = true

            while taking && drained.Count < batch do
                match actions.TryDequeue() with
                | true, leased -> drained.Add leased
                | _ -> taking <- false

            Task.FromResult(Ok(List.ofSeq drained))

        member this.Complete(action, _) =
            lock this.Delivered (fun () -> this.Delivered.Add action.Work.Ordinal)
            Task.FromResult(Ok Updated)

        member _.Reschedule(_, _, _) = Task.FromResult(Ok Updated)
        member _.Abandon(_, _, _) = Task.FromResult(Ok Updated)

    interface MachineStore

/// <summary>A registry that agrees with whatever a chart claims.</summary>
type TestRegistry() =
    interface IChartRegistry with
        member _.Register(_, _) =
            Task.FromResult(Ok ChartRegistration.Registered)

        member _.TryGet(_, _, _) = Task.FromResult(Ok None)

/// <summary>Builds one leased action for the dispatcher to deliver.</summary>
let leasedAction (ordinal: int) : LeasedAction<Entity, TestAction> =
    { Work =
        { MachineId = machineId "di-tests"
          EntityId = entityId "E-1"
          CommandId = CommandId.ofInt64 1L
          Epoch = Epoch.ofUInt64 1UL
          Ordinal = ordinal
          Action = Log $"effect-{ordinal}" }
      Token = LeaseToken.ofInt64 (int64 ordinal + 1L)
      DeliveryCount = 1
      ExpiresAt = DateTimeOffset.UtcNow.AddSeconds 30. }

/// Short, deterministic supervisor settings: a 250 ms shutdown budget and no restart delay.
let supervisorDefaults: AutomataSupervisorOptions =
    { Name = SupervisorName.create "di-tests"
      Strategy = RestartStrategy.OneForOne
      Restart = RestartKind.Permanent
      Intensity = 3
      Period = TimeSpan.FromMinutes 1.
      RestartDelay = TimeSpan.Zero
      Shutdown = TimeSpan.FromMilliseconds 250.
      StartupRetry = None }

/// A machine over the given store, polling fast enough that a test does not wait on it.
let testMachineOver (storeArg: MachineStore) : Result<TestMachine, MachineConfigError list> =
    machine<Entity, TestState, TestEvent, TestAction, string> (machineId "di-tests") {
        chart testChart
        chartVersion 1
        initialState Idle
        store storeArg
        actionQueue "di_test_actions"

        processor
            { ProcessorPolicy.defaults with
                PollingInterval = TimeSpan.FromMilliseconds 10. }

        timeProvider TimeProvider.System
    }

/// Unwraps a machine result, failing the test on any accumulated configuration error.
let expectMachine (result: Result<TestMachine, MachineConfigError list>) : TestMachine =
    match result with
    | Ok m -> m
    | Error errors -> failtestf "machine build failed: %A" errors

/// Options whose factory builds the local chart/store machine and reports it to the test.
let testOptions (machineKey: string) (store: TestStore) (onMachine: TestMachine -> unit) : TestOptions =
    { MachineKey = machineKey
      Supervisor = supervisorDefaults
      DispatchActions = false
      MachineFactory =
        fun _ ->
            match testMachineOver (store :> MachineStore) with
            | Ok built ->
                onMachine built
                Ok built
            | Error errors -> Error errors
      ChartRegistry = fun _ -> TestRegistry()
      TimeProvider = TimeProvider.System }

/// Resolves the single registered hosted service and its BackgroundService surface.
let resolveHostedService (provider: IServiceProvider) : IHostedService * BackgroundService =
    let hosted = provider.GetServices<IHostedService>() |> Seq.head
    hosted, hosted :?> BackgroundService

/// Polls until the predicate holds, failing the test after the shared timeout.
let pollUntil (description: string) (predicate: unit -> bool) : Task =
    let sw = Stopwatch.StartNew()

    let rec loop () =
        task {
            if predicate () then
                return ()
            elif sw.Elapsed > waitTimeout then
                return failtestf "timed out waiting for %s" description
            else
                do! Task.Delay 20
                return! loop ()
        }

    loop ()

/// An audit store that fails every record with the given error, for the fault proof.
type FailingAuditStore(error: StoreError) =
    interface ISupervisionEventStore with

        member _.Record(_record, _ct) =
            Task.FromResult(Result<unit, StoreError>.Error error)
