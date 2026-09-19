module ByzantineSystems.Automata.Runtime.Tests.RuntimeRefactorTests

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Resilience
open ByzantineSystems.Automata.Runtime
open ByzantineSystems.Automata.Storage
open Expecto
open TestSupport

let private noCancellation = CancellationToken.None

let private retryConfig = RetryConfig.defaults<string>

let private observer (_: Transition<Entity, TestState, TestEvent, TestAction>) (_: CancellationToken) : Task<unit> =
    task { return () }

let private duplicateResult (storeArg: MachineStore) (time: TimeProvider) =
    machine<Entity, TestState, TestEvent, TestAction, string> (machineId "duplicates") {
        chart testChart
        chart testChart
        initialState Idle
        initialState Idle
        store storeArg
        store storeArg
        retry retryConfig
        retry retryConfig
        retryPolicy RetryPolicy.defaults
        retryPolicy RetryPolicy.defaults
        onTransition observer
        onTransition observer
        mailboxCapacity 8
        mailboxCapacity 8
        idleTimeout (TimeSpan.FromMinutes 1.)
        idleTimeout (TimeSpan.FromMinutes 1.)
        timeProvider time
        timeProvider time
    }

type private GatedStore(inner: MachineStore) =
    let state = inner :> IStateStore<Entity, TestState, TestEvent, TestAction>
    let retryQueue = inner :> IRetryQueue<Entity, TestEvent>
    let deadLetter = inner :> IDeadLetterStore<Entity, TestEvent>
    let outbox = inner :> IActionOutbox<Entity, TestAction>

    let entered =
        TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

    let release =
        TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

    let firstRead = ref 0

    member _.Entered = entered.Task
    member _.Release() = release.TrySetResult() |> ignore

    interface IStateStore<Entity, TestState, TestEvent, TestAction> with
        member _.TryGet(machineId, entityId, ct) =
            task {
                if Interlocked.Increment(&firstRead.contents) = 1 then
                    entered.TrySetResult() |> ignore
                    do! release.Task.WaitAsync(ct)

                return! state.TryGet(machineId, entityId, ct)
            }

        member _.FindReceipt(machineId, entityId, key, ct) =
            state.FindReceipt(machineId, entityId, key, ct)

        member _.Commit(transition, expected, ct) = state.Commit(transition, expected, ct)

        member _.History(machineId, entityId, page, ct) =
            state.History(machineId, entityId, page, ct)

    interface IRetryQueue<Entity, TestEvent> with
        member _.Enqueue(request, ct) = retryQueue.Enqueue(request, ct)
        member _.Claim(batchSize, lease, ct) = retryQueue.Claim(batchSize, lease, ct)
        member _.Complete(retryId, ct) = retryQueue.Complete(retryId, ct)

        member _.Fail(retryId, nextAttemptAt, error, ct) =
            retryQueue.Fail(retryId, nextAttemptAt, error, ct)

    interface IDeadLetterStore<Entity, TestEvent> with
        member _.Record(letter, ct) = deadLetter.Record(letter, ct)

    interface IActionOutbox<Entity, TestAction> with
        member _.Claim(batchSize, lease, ct) = outbox.Claim(batchSize, lease, ct)
        member _.Complete(key, ct) = outbox.Complete(key, ct)
        member _.Fail(key, nextAttemptAt, ct) = outbox.Fail(key, nextAttemptAt, ct)

    interface IMachineStore<Entity, TestState, TestEvent, TestAction>

let private expectEscalated (work: Task) =
    task {
        try
            let! _ = work
            return failtest "expected EscalatedSend"
        with :? EscalatedSend ->
            return ()
    }

let private expectCanceled (work: Task) =
    task {
        try
            do! work
            return failtest "expected cancellation"
        with :? OperationCanceledException ->
            return ()
    }

let configurationTests =
    testList
        "machine configuration"
        [ test "all singleton declarations report typed duplicates" {
              let time = newTime ()
              let store = TestStore(time)

              match duplicateResult (store :> MachineStore) time with
              | Ok _ -> failtest "expected duplicate declaration errors"
              | Error errors ->
                  let duplicates =
                      errors
                      |> List.choose (function
                          | DuplicateDeclaration declaration -> Some declaration
                          | _ -> None)
                      |> Set.ofList

                  Expect.equal
                      (set
                          [ MachineDeclaration.Chart
                            MachineDeclaration.Initial
                            MachineDeclaration.Store
                            MachineDeclaration.Retry
                            MachineDeclaration.RetryPolicy
                            MachineDeclaration.Observer
                            MachineDeclaration.MailboxCapacity
                            MachineDeclaration.IdleTimeout
                            MachineDeclaration.TimeProvider ])
                      duplicates
                      "every duplicate identifies its declaration"
          }

          test "duplicate and structural errors accumulate" {
              let result =
                  machine<Entity, TestState, TestEvent, TestAction, string> (machineId "invalid") {
                      mailboxCapacity 0
                      mailboxCapacity 0
                  }

              match result with
              | Ok _ -> failtest "expected accumulated errors"
              | Error errors ->
                  Expect.contains errors (DuplicateDeclaration MachineDeclaration.MailboxCapacity) "duplicate retained"
                  Expect.contains errors MissingChart "missing chart retained"
                  Expect.contains errors (MailboxCapacityBelowOne 0) "invalid value retained"
          }

          test "retry validation produces an opaque validated policy" {
              match RetryPolicy.validate RetryPolicy.defaults with
              | Ok _ -> ()
              | Error errors -> failtestf "default policy should validate: %A" errors
          } ]

let lifecycleTests =
    testList
        "machine lifecycle"
        [ testTask "send and state require start; start and stop are idempotent" {
              let time = newTime ()
              let store = TestStore(time)
              let machine = buildUnstartedMachine rejectUnhandled (store :> MachineStore) time
              let entity = entityId "LIFE-1"

              Expect.throwsT<MachineNotStarted>
                  (fun () ->
                      Machine.send machine entity (EventEnvelope.create "one" (Start 1)) noCancellation
                      |> ignore)
                  "send before start fails deterministically"

              Expect.throwsT<MachineNotStarted>
                  (fun () -> Machine.state machine entity noCancellation |> ignore)
                  "state before start fails deterministically"

              do! Machine.startAsync machine noCancellation
              do! Machine.startAsync machine noCancellation

              let! sent = Machine.send machine entity (EventEnvelope.create "one" (Start 1)) noCancellation
              Expect.equal (Ok Committed) sent "started machine accepts work"

              let firstStop = Machine.stopAsync machine noCancellation
              let secondStop = Machine.stopAsync machine noCancellation
              Expect.isTrue (obj.ReferenceEquals(firstStop, secondStop)) "stop returns the same completion"
              do! firstStop

              Expect.throwsT<MachineStopped>
                  (fun () -> Machine.state machine entity noCancellation |> ignore)
                  "state after stop fails deterministically"
          } ]

let signalTests =
    testList
        "independent machine signals"
        [ testTask "committed actions signal only outbox work" {
              let time = newTime ()
              let store = TestStore(time)
              let machine = buildMachine rejectUnhandled (store :> MachineStore) time

              let! _ = Machine.send machine (entityId "SIGNAL-1") (EventEnvelope.create "one" (Start 1)) noCancellation

              let! _ = machine.OutboxSignalValue.WaitAsync(noCancellation)
              let timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds 50.)
              do! expectCanceled (machine.RetrySignalValue.WaitAsync(timeout.Token))
              timeout.Dispose()
          }

          testTask "deferred sends signal only retry work" {
              let time = newTime ()
              let store = TestStore(time)
              let machine = buildMachine deferUnhandled (store :> MachineStore) time

              let! _ = Machine.send machine (entityId "SIGNAL-2") (EventEnvelope.create "one" Cancel) noCancellation

              let! _ = machine.RetrySignalValue.WaitAsync(noCancellation)
              let timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds 50.)
              do! expectCanceled (machine.OutboxSignalValue.WaitAsync(timeout.Token))
              timeout.Dispose()
          } ]

let actorTests =
    testList
        "actor failure"
        [ testTask "escalation faults completion and pending replies without removing a replacement" {
              let time = newTime ()
              let inner = TestStore(time)
              let gated = GatedStore(inner :> MachineStore)

              let machine =
                  buildMachine (classifyWith Disposition.Escalate) (gated :> MachineStore) time

              let entity = entityId "ACTOR-1"
              let oldActor = machine.RegistryValue.GetOrCreate entity

              let current = oldActor.Send(EventEnvelope.create "bad" Cancel, noCancellation)
              do! gated.Entered
              let pending = oldActor.Send(EventEnvelope.create "queued" (Start 1), noCancellation)

              time.Advance(TimeSpan.FromMinutes 10.)
              machine.RegistryValue.TryEvictIdle entity
              let replacement = machine.RegistryValue.GetOrCreate entity
              Expect.isFalse (obj.ReferenceEquals(oldActor, replacement)) "a replacement actor was installed"

              gated.Release()
              do! expectEscalated current
              do! expectEscalated pending
              do! expectEscalated oldActor.Completion

              Expect.isTrue oldActor.Completion.IsFaulted "actor completion faults"
              Expect.equal 1 machine.RegistryValue.ActorCount "stale completion preserves the exact replacement"
          } ]

let tests =
    testList "M5 runtime refactor" [ configurationTests; lifecycleTests; signalTests; actorTests ]
