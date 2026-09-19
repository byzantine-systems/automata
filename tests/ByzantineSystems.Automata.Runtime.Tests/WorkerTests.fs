module ByzantineSystems.Automata.Runtime.Tests.WorkerTests

open System
open System.Threading
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Resilience
open ByzantineSystems.Automata.Runtime
open ByzantineSystems.Automata.Storage
open Expecto
open TestSupport

let private noCancellation = CancellationToken.None
let private testMachineId = machineId "test"

let private outboxKeyFor (entity: Entity) (eventKey: string) (ordinal: int) : OutboxKey<Entity> =
    { MachineId = testMachineId
      EntityId = entity
      EventIdempotencyKey = eventKey
      Ordinal = ordinal }

let private mkRequest entity key event : RetryRequest<Entity, TestEvent> =
    { MachineId = testMachineId
      EntityId = entity
      IdempotencyKey = key
      Event = event
      NextAttemptAt = startTime
      LastError = Some "transient failure" }

let retryPumpTests =
    testList
        "retry pump"
        [ testTask "a claimed retry that commits is completed" {
              let time = newTime ()
              let store = TestStore(time)

              let machine =
                  buildMachine (RetryConfig.defaults<string>.Classify) (store :> MachineStore) time

              let entity = entityId "PUMP-1"

              let q = store :> IRetryQueue<Entity, TestEvent>
              let! _ = q.Enqueue(mkRequest entity "r-1" (Start 1), noCancellation)

              let pump = RetryPump(machine)
              do! pump.PollAsync(noCancellation)

              Expect.isEmpty (store.PendingRetries()) "the retry was completed"

              let! snapshot = Machine.state machine entity noCancellation

              match snapshot with
              | Ok(Some s) -> Expect.equal (Active 1) s.State "the retried event applied"
              | other -> failtestf "expected a snapshot, got %A" other
          }

          testTask "a recoverable failure reschedules and keeps the item leased-free" {
              let time = newTime ()
              let store = TestStore(time)
              let machine = buildMachine deferUnhandled (store :> MachineStore) time
              let entity = entityId "PUMP-2"

              let q = store :> IRetryQueue<Entity, TestEvent>
              let! _ = q.Enqueue(mkRequest entity "r-1" Cancel, noCancellation)

              let pump = RetryPump(machine)
              do! pump.PollAsync(noCancellation)

              match store.PendingRetries() with
              | [ item ] ->
                  Expect.equal 1 item.Attempts "the claim counted the attempt"
                  Expect.isNone item.LockedUntil "the item is not left leased"
                  Expect.isGreaterThan item.NextAttemptAt startTime "the item was rescheduled"
              | other -> failtestf "expected one pending item, got %A" other
          }

          testTask "the durable maximum dead-letters and completes the item" {
              let time = newTime ()
              let store = TestStore(time)

              let policy =
                  { RetryPolicy.defaults with
                      MaxAttempts = 1 }

              let machine =
                  buildMachineWithPolicy deferUnhandled policy (store :> MachineStore) time

              let entity = entityId "PUMP-3"

              let q = store :> IRetryQueue<Entity, TestEvent>
              let! _ = q.Enqueue(mkRequest entity "r-1" Cancel, noCancellation)

              let pump = RetryPump(machine)
              do! pump.PollAsync(noCancellation)

              Expect.isEmpty (store.PendingRetries()) "the item left the queue"
              Expect.equal 1 (store.DeadLetterLog().Length) "a dead letter was recorded"
          } ]

let actionDispatcherTests =
    testList
        "action dispatcher"
        [ testTask "a delivered action is completed with its stable key" {
              let time = newTime ()
              let store = TestStore(time)
              let entity = entityId "DISP-1"
              let delivered = ref None

              let handler (key: OutboxKey<Entity>) (_action: TestAction) (_ct: CancellationToken) =
                  task {
                      delivered.Value <- Some key
                      return Ok()
                  }

              let s = store :> IStateStore<Entity, TestState, TestEvent, TestAction>

              let transition =
                  { MachineId = testMachineId
                    EntityId = entity
                    IdempotencyKey = "d-1"
                    OccurredAt = startTime
                    Epoch = Epoch.next Epoch.initial
                    Event = Start 1
                    Actions = [ Log "hello" ]
                    FromState = Idle
                    ToState = Active 1
                    Status = InstanceStatus.Running
                    HandledBy = stateId "idle"
                    Exited = []
                    Entered = [] }

              let! _ = s.Commit(transition, Epoch.initial, noCancellation)

              let dispatcher =
                  ActionDispatcher(
                      store :> IActionOutbox<Entity, TestAction>,
                      handler,
                      RetryPolicy.defaults,
                      time,
                      new WorkSignal()
                  )

              do! dispatcher.PollAsync(noCancellation)

              Expect.equal (Some(outboxKeyFor entity "d-1" 0)) delivered.Value "the stable key was passed through"
              Expect.isEmpty (store.PendingOutbox()) "the delivered action is removed"
          } ]

let tests = testList "workers" [ retryPumpTests; actionDispatcherTests ]
