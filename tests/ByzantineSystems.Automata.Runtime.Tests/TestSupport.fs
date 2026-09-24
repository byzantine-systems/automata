module ByzantineSystems.Automata.Runtime.Tests.TestSupport

open System
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Runtime
open Expecto

/// <summary>Phantom tag for the entity these tests use.</summary>
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

type TestError = Refused of string

let testChart: Chart<TestState, TestEvent, TestAction, TestError> =
    statechart<TestState, TestEvent, TestAction, TestError> {
        root "machine"

        classify (fun state ->
            match state with
            | Idle -> stateId "idle"
            | Active _ -> stateId "active"
            | Done -> stateId "done")

        state "idle" {
            on
                (fun _ event ->
                    match event with
                    | Start _ -> true
                    | _ -> false)
                (fun _ event ->
                    match event with
                    | Start amount -> [ Log "started" ], Active amount
                    | _ -> [], Idle)
        }

        state "active" { on (fun _ event -> event = Finish) (fun _ _ -> [ Log "finished" ], Done) }

        state "done" { terminal }
    }
    |> function
        | Ok chart -> chart
        | Error errors -> failwithf "the test chart is invalid: %A" errors

/// <summary>
/// A clock the test drives. Durable ordering is the database's business now, so this is only
/// here for the few places the runtime still reads a clock: poll waits and elapsed durations.
/// </summary>
type FakeTime(start: DateTimeOffset) =
    inherit TimeProvider()
    let mutable now = start
    override _.GetUtcNow() = now
    member _.Advance(span: TimeSpan) = now <- now.Add span

let startTime = DateTimeOffset(2026, 9, 23, 12, 0, 0, TimeSpan.Zero)
let newTime () = FakeTime(startTime)

let testMachine = machineId "tests"

/// <summary>
/// A scripted stand-in for a store.
///
/// It is deliberately not an implementation of one. It does not order anything, does not
/// enforce the per-entity head invariant, and does not fence a lease; it hands back whatever a
/// test told it to and records what it was asked. Every property that depends on the store
/// actually behaving belongs in the integration suite against real PostgreSQL, and no test here
/// may assert one through this type.
/// </summary>
type StubStore<'Err>() =

    let pending = ConcurrentQueue<LeasedCommand<EntityId<TestEntity>, TestEvent>>()
    let actions = ConcurrentQueue<LeasedAction<EntityId<TestEntity>, TestAction>>()

    member val Snapshot: Snapshot<TestState> option = None with get, set

    /// What History hands back. Scripted, like everything else here: this stub orders nothing.
    member val Log: CommittedTransition<EntityId<TestEntity>, TestState, TestEvent, TestAction> list = [] with get, set

    /// What the next Commit answers with.
    member val CommitOutcome: Result<FinalizeOutcome, StoreError> = Ok(Finalized(Epoch.ofUInt64 1UL)) with get, set

    member val RejectOutcome: Result<FinalizeOutcome, StoreError> = Ok(Finalized Epoch.initial) with get, set

    member val DeadLetterOutcome: Result<FinalizeOutcome, StoreError> = Ok(Finalized Epoch.initial) with get, set

    member val RescheduleOutcome: Result<LeaseUpdateOutcome, StoreError> = Ok Updated with get, set

    member val ExtendOutcome: Result<LeaseUpdateOutcome, StoreError> = Ok Updated with get, set

    member val SubmitOutcome: Result<SubmissionOutcome, StoreError> = Ok(Accepted(CommandId.ofInt64 1L)) with get, set

    member val ResultOutcome: Result<
        CommandResult<EntityId<TestEntity>, TestState, TestEvent, TestAction, 'Err> option,
        StoreError
     > = Ok None with get, set

    member val ClaimOutcome: Result<unit, StoreError> = Ok() with get, set

    member val Commits = ResizeArray<CommandId>() with get
    member val Rejections = ResizeArray<CommandId * CommandFailure<'Err>>() with get
    member val DeadLetters = ResizeArray<CommandId * CommandFailure<'Err>>() with get
    member val Reschedules = ResizeArray<CommandId>() with get
    member val Extensions = ResizeArray<CommandId>() with get
    member val Deliveries = ResizeArray<CommandId * int>() with get
    member val Abandonments = ResizeArray<CommandId * string>() with get

    /// Scripts one command for the next claim.
    member _.Offer(leased) = pending.Enqueue leased

    /// Scripts one action for the next action claim.
    member _.OfferAction(leased) = actions.Enqueue leased

    interface ICommandInbox<EntityId<TestEntity>, TestEvent> with

        member this.Submit(_, _) = Task.FromResult this.SubmitOutcome

        member this.Claim(_, batch, _, _) =
            match this.ClaimOutcome with
            | Error error -> Task.FromResult(Error error)
            | Ok() ->
                let drained = ResizeArray()
                let mutable taking = true

                while taking && drained.Count < batch do
                    match pending.TryDequeue() with
                    | true, leased -> drained.Add leased
                    | _ -> taking <- false

                Task.FromResult(Ok(List.ofSeq drained))

        member this.Reschedule(commandId, _, _, _) =
            lock this.Reschedules (fun () -> this.Reschedules.Add commandId)
            Task.FromResult this.RescheduleOutcome

        member this.ExtendLease(commandId, _, _, _) =
            lock this.Extensions (fun () -> this.Extensions.Add commandId)
            Task.FromResult this.ExtendOutcome

        member _.TryGet(_, _) = Task.FromResult(Ok None)
        member _.TryFind(_, _, _, _) = Task.FromResult(Ok None)

    interface IStateReader<EntityId<TestEntity>, TestState, TestEvent, TestAction> with
        member this.TryGetSnapshot(_, _, _) = Task.FromResult(Ok this.Snapshot)
        member this.History(_, _, _, _) = Task.FromResult(Ok this.Log)

    interface ICommandProcessorStore<EntityId<TestEntity>, TestState, TestEvent, TestAction, 'Err> with

        member this.Commit(commandId, _, _, _, _) =
            lock this.Commits (fun () -> this.Commits.Add commandId)
            Task.FromResult this.CommitOutcome

        member this.Reject(commandId, _, failure, _) =
            lock this.Rejections (fun () -> this.Rejections.Add(commandId, failure))
            Task.FromResult this.RejectOutcome

        member this.DeadLetter(commandId, _, failure, _) =
            lock this.DeadLetters (fun () -> this.DeadLetters.Add(commandId, failure))
            Task.FromResult this.DeadLetterOutcome

        member this.TryGetResult(_, _) = Task.FromResult this.ResultOutcome

    interface IActionQueue<EntityId<TestEntity>, TestAction> with

        member _.Claim(_, batch, _, _) =
            let drained = ResizeArray()
            let mutable taking = true

            while taking && drained.Count < batch do
                match actions.TryDequeue() with
                | true, leased -> drained.Add leased
                | _ -> taking <- false

            Task.FromResult(Ok(List.ofSeq drained))

        member this.Complete(action, _) =
            lock this.Deliveries (fun () -> this.Deliveries.Add(action.Work.CommandId, action.Work.Ordinal))
            Task.FromResult(Ok Updated)

        member _.Reschedule(_, _, _) = Task.FromResult(Ok Updated)

        member this.Abandon(action, reason, _) =
            lock this.Abandonments (fun () -> this.Abandonments.Add(action.Work.CommandId, reason))
            Task.FromResult(Ok Updated)

    interface IMachineStore<EntityId<TestEntity>, TestState, TestEvent, TestAction, 'Err>

/// <summary>
/// A store that also offers the two temporal capabilities.
///
/// A separate type rather than a flag on StubStore, so both answers to "does this store offer
/// time travel" are reachable: A provider implementing only the required four still works, and
/// a test that could not construct such a provider would not be testing it.
/// </summary>
type TemporalStubStore<'Err>() =
    inherit StubStore<'Err>()

    member val Beliefs: Belief<EntityId<TestEntity>, TestState> option = None with get, set

    interface ITemporalReader<EntityId<TestEntity>, TestState> with
        member this.ValidAt(_, _, _, _) = Task.FromResult(Ok this.Beliefs)
        member this.AsOf(_, _, _, _, _) = Task.FromResult(Ok this.Beliefs)

    interface ICorrectionStore<EntityId<TestEntity>, TestState> with
        member _.Correct(_, _, _, _, _) = Task.FromResult(Ok NothingSuperseded)

/// <summary>A store with a boot check that answers whatever the test scripted.</summary>
type BootingStubStore<'Err>(report: BootReport) =
    inherit StubStore<'Err>()

    interface IStoreBoot with
        member _.Boot(_, _) = Task.FromResult(Ok report)

/// <summary>
/// A store that can announce work. <c>Announce</c> calls the listener's command callback, the way
/// a notification from another process would; a scripted failure makes <c>Listen</c> fail that
/// many times before it listens properly.
/// </summary>
type NotifyingStubStore<'Err>(failures: int) =
    inherit StubStore<'Err>()

    let mutable remainingFailures = failures
    let mutable onCommands: (unit -> unit) option = None

    member val Listens = 0 with get, set

    member _.Announce() =
        onCommands |> Option.iter (fun signal -> signal ())

    interface IWorkNotifications with
        member this.Listen(_, commands, _, ct) =
            this.Listens <- this.Listens + 1

            if remainingFailures > 0 then
                remainingFailures <- remainingFailures - 1
                Task.FromResult(Error(StoreError.Unavailable(InvalidOperationException "scripted")))
            else
                onCommands <- Some commands

                task {
                    try
                        do! Task.Delay(Timeout.InfiniteTimeSpan, ct)
                    with :? OperationCanceledException ->
                        ()

                    return Ok()
                }

/// <summary>A chart registry that always agrees, for tests that are not about registration.</summary>
type StubRegistry(?outcome: ChartRegistration) =
    let answer = defaultArg outcome ChartRegistration.Registered

    member val Registrations = 0 with get, set

    interface IChartRegistry with
        member this.Register(_, _) =
            this.Registrations <- this.Registrations + 1
            Task.FromResult(Ok answer)

        member _.TryGet(_, _, _) = Task.FromResult(Ok None)

/// <summary>Builds one leased command for a stub to hand out.</summary>
let leasedCommand (id: int64) (entity: string) (event: TestEvent) (attempts: int) =
    { Work =
        { CommandId = CommandId.ofInt64 id
          MachineId = testMachine
          EntityId = entityId entity
          Sequence = id
          IdempotencyKey = $"key-{id}"
          ChartVersion = ChartVersion.create 1
          Event = event
          Status = CommandStatus.Leased
          Blocked = false
          VisibleAt = startTime
          Attempts = attempts
          ReceivedAt = startTime
          Audit = AuditContext.empty }
      Token = LeaseToken.ofInt64 id
      DeliveryCount = attempts + 1
      ExpiresAt = startTime.AddSeconds 30. }

/// <summary>Builds one leased action for a stub to hand out.</summary>
let leasedAction (msgId: int64) (commandId: int64) (ordinal: int) (deliveries: int) =
    { Work =
        { MachineId = testMachine
          EntityId = entityId "E-1"
          CommandId = CommandId.ofInt64 commandId
          Epoch = Epoch.ofUInt64 1UL
          Ordinal = ordinal
          Action = Log "effect" }
      Token = LeaseToken.ofInt64 msgId
      DeliveryCount = deliveries
      ExpiresAt = startTime.AddSeconds 30. }

/// <summary>Builds a machine over a stub store, with the fast settings a test wants.</summary>
let buildMachine
    (stub: StubStore<TestError>)
    (time: TimeProvider)
    (adjust: ProcessorPolicy<TestError> -> ProcessorPolicy<TestError>)
    =
    let policy: ProcessorPolicy<TestError> =
        adjust
            { ProcessorPolicy.defaults with
                Batch = 4
                Lease = TimeSpan.FromSeconds 30.
                RenewAfter = TimeSpan.FromSeconds 20.
                PollingInterval = TimeSpan.FromMilliseconds 10.
                Concurrency = 2
                MaxAttempts = 3 }

    machine<EntityId<TestEntity>, TestState, TestEvent, TestAction, TestError> testMachine {
        chart testChart
        chartVersion 1
        initialState Idle
        store (stub :> IMachineStore<_, _, _, _, _>)
        processor policy
        timeProvider time
    }

let expectMachine result =
    match result with
    | Ok value -> value
    | Error errors -> failtestf "the machine should have built, but reported %A" errors
