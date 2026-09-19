module ByzantineSystems.Automata.Runtime.Tests.TestSupport

open System
open System.Threading
open System.Threading.Channels
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Resilience
open ByzantineSystems.Automata.Runtime
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.InMemory
open Expecto

/// Phantom marker so entity ids are tied to a domain type in tests.
type TestEntity = class end

type TestState =
    | Idle
    | Active of int
    | Done

type TestEvent =
    | Start of int
    | Finish
    | Cancel

type TestAction = Log of string

let private chartResult =
    statechart<TestState, TestEvent, TestAction, string> {
        root "root"

        classify (function
            | Idle -> stateId "idle"
            | Active _ -> stateId "active"
            | Done -> stateId "done")

        state "idle" {
            on
                (fun _ e ->
                    match e with
                    | Start _ -> true
                    | _ -> false)
                (fun _ e ->
                    match e with
                    | Start n -> [ Log "started" ], Active n
                    | _ -> [], Idle)
        }

        state "active" { on (fun _ e -> e = Finish) (fun _ _ -> [], Done) }

        state "done" { terminal }
    }

let testChart =
    match chartResult with
    | Ok c -> c
    | Error errors -> failtestf "test chart failed to construct: %A" errors

type Entity = EntityId<TestEntity>
type TestStore = InMemoryStore<Entity, TestState, TestEvent, TestAction>
type MachineStore = IMachineStore<Entity, TestState, TestEvent, TestAction>
type TestMachine = ByzantineSystems.Automata.Runtime.Machine<Entity, TestState, TestEvent, TestAction, string>

/// Deterministic clock: leases and due times advance only when the test says so.
type FakeTime(start: DateTimeOffset) =
    inherit TimeProvider()
    let mutable current = start

    override _.GetUtcNow() : DateTimeOffset = current
    member _.Advance(span: TimeSpan) = current <- current.Add span

let startTime = DateTimeOffset(2026, 9, 19, 12, 0, 0, TimeSpan.Zero)
let newTime () = FakeTime(startTime)

/// Unwraps a machine result, failing the test on any accumulated configuration error.
let expectMachine (result: Result<TestMachine, MachineConfigError list>) : TestMachine =
    match result with
    | Ok m -> m
    | Error errors -> failtestf "machine build failed: %A" errors

/// Disposition that rejects domain and machine-level rejections, defers transient store
/// failures, and routes unhandled events to the supplied policy.
let classifyWith (onUnhandled: Disposition) (error: MachineError<string>) : Disposition =
    match error with
    | MachineError.Transition(TransitionError.Unhandled _) -> onUnhandled
    | MachineError.Transition(TransitionError.Rejected _) -> Disposition.Reject
    | MachineError.Rejected _ -> Disposition.Reject
    | MachineError.Store(StoreError.Unavailable _)
    | MachineError.Store(StoreError.Concurrency _) -> Disposition.Defer
    | _ -> Disposition.Escalate

let deferUnhandled = classifyWith Disposition.Defer
let ignoreUnhandled = classifyWith Disposition.Ignore
let rejectUnhandled = classifyWith Disposition.Reject

/// Builds a machine with the given classify hook, store, and durable retry policy.
let buildMachineWithPolicy
    (classify: MachineError<string> -> Disposition)
    (policy: RetryPolicy)
    (storeArg: MachineStore)
    (time: TimeProvider)
    : TestMachine =
    let retryConfig =
        { RetryConfig.defaults<string> with
            Classify = classify }

    let result =
        machine<Entity, TestState, TestEvent, TestAction, string> (machineId "test") {
            chart testChart
            initialState Idle
            store storeArg
            retry retryConfig
            retryPolicy policy
            timeProvider time
        }

    expectMachine result

/// Builds a machine with the given classify hook and the default durable retry policy.
let buildMachine
    (classify: MachineError<string> -> Disposition)
    (storeArg: MachineStore)
    (time: TimeProvider)
    : TestMachine =
    buildMachineWithPolicy classify RetryPolicy.defaults storeArg time

/// A store wrapper that can gate and count concurrent commits, for the parallelism proof.
type TrackingStore<'EntityId, 'State, 'Event, 'Action when 'EntityId: equality>
    (inner: IMachineStore<'EntityId, 'State, 'Event, 'Action>) =

    let innerState = inner :> IStateStore<'EntityId, 'State, 'Event, 'Action>
    let innerRetry = inner :> IRetryQueue<'EntityId, 'Event>
    let innerDead = inner :> IDeadLetterStore<'EntityId, 'Event>
    let innerOutbox = inner :> IActionOutbox<'EntityId, 'Action>

    let inFlight = ref 0
    let maxInFlight = ref 0
    let gate = new SemaphoreSlim(0, Int32.MaxValue)
    let gated = ref false
    let entries = Channel.CreateUnbounded<unit>()

    let enter () =
        let observed = Interlocked.Increment(&inFlight.contents)

        let rec bump () =
            let current = Volatile.Read(&maxInFlight.contents)

            if
                observed > current
                && Interlocked.CompareExchange(&maxInFlight.contents, observed, current) <> current
            then
                bump ()

        bump ()
        entries.Writer.TryWrite(()) |> ignore

    let exit () =
        Interlocked.Decrement(&inFlight.contents) |> ignore

    member _.EnableGate() = gated.Value <- true
    member _.Release(permits: int) = gate.Release(permits)
    member _.InFlight = Volatile.Read(&inFlight.contents)
    member _.MaxInFlight = Volatile.Read(&maxInFlight.contents)
    member _.EntrySignals = entries.Reader

    interface IStateStore<'EntityId, 'State, 'Event, 'Action> with
        member _.TryGet(machineId, entityId, ct) =
            innerState.TryGet(machineId, entityId, ct)

        member _.FindReceipt(machineId, entityId, key, ct) =
            innerState.FindReceipt(machineId, entityId, key, ct)

        member _.Commit(transition, expected, ct) =
            task {
                enter ()

                try
                    if gated.Value then
                        do! gate.WaitAsync(ct)

                    return! innerState.Commit(transition, expected, ct)
                finally
                    exit ()
            }

        member _.History(machineId, entityId, paging, ct) =
            innerState.History(machineId, entityId, paging, ct)

    interface IRetryQueue<'EntityId, 'Event> with
        member _.Enqueue(request, ct) = innerRetry.Enqueue(request, ct)
        member _.Claim(batch, lease, ct) = innerRetry.Claim(batch, lease, ct)
        member _.Complete(retryId, ct) = innerRetry.Complete(retryId, ct)

        member _.Fail(retryId, nextAttemptAt, error, ct) =
            innerRetry.Fail(retryId, nextAttemptAt, error, ct)

    interface IDeadLetterStore<'EntityId, 'Event> with
        member _.Record(deadLetter, ct) = innerDead.Record(deadLetter, ct)

    interface IActionOutbox<'EntityId, 'Action> with
        member _.Claim(batch, lease, ct) = innerOutbox.Claim(batch, lease, ct)
        member _.Complete(actionKey, ct) = innerOutbox.Complete(actionKey, ct)

        member _.Fail(actionKey, nextAttemptAt, ct) =
            innerOutbox.Fail(actionKey, nextAttemptAt, ct)

    interface IMachineStore<'EntityId, 'State, 'Event, 'Action>

/// Reads exactly <c>n</c> commit-entry signals from the tracking store.
let rec waitForEntries (reader: ChannelReader<unit>) (n: int) (ct: CancellationToken) : Task =
    task {
        if n > 0 then
            let! _ = reader.ReadAsync(ct)
            return! waitForEntries reader (n - 1) ct
    }

/// Maps a task's result so values can be unwrapped inside bind pipelines.
let mapTask (mapping: 'T -> 'U) (source: Task<'T>) : Task<'U> =
    task {
        let! value = source
        return mapping value
    }
