module ByzantineSystems.Automata.DependencyInjection.Tests.TestSupport

open System
open System.Diagnostics
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Resilience
open ByzantineSystems.Automata.Runtime
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.InMemory
open ByzantineSystems.Automata.DependencyInjection
open Expecto
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Polly

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
type TestStore = InMemoryStore<Entity, TestState, TestEvent, TestAction>
type MachineStore = IMachineStore<Entity, TestState, TestEvent, TestAction>
type TestMachine = Machine<Entity, TestState, TestEvent, TestAction, string>
type TestOptions = AutomataOptions<Entity, TestState, TestEvent, TestAction, string, string>

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

/// Builds a machine on the shared pipeline the host injects, like a real registration would.
let testMachineWithPipeline
    (pipeline: ResiliencePipeline<PipelineResult<string>>)
    (storeArg: MachineStore)
    : Result<TestMachine, MachineConfigError list> =
    machine<Entity, TestState, TestEvent, TestAction, string> (machineId "di-tests") {
        chart testChart
        initialState Idle
        store storeArg
        resiliencePipeline pipeline RetryConfig.defaults<string>.Classify
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
      Retry = RetryConfig.defaults<string>
      Supervisor = supervisorDefaults
      DispatchActions = false
      MachineFactory =
        fun _ pipeline ->
            match testMachineWithPipeline pipeline (store :> MachineStore) with
            | Ok built ->
                onMachine built
                Ok built
            | Error errors -> Error errors
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
