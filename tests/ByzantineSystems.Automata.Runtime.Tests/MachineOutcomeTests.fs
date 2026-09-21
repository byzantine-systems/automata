module ByzantineSystems.Automata.Runtime.Tests.MachineOutcomeTests

open System
open System.Threading
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Resilience
open ByzantineSystems.Automata.Runtime
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.InMemory
open Expecto
open TestSupport

let private noCancellation = CancellationToken.None

let private newStoreAndMachine classify =
    let time = newTime ()
    let store = TestStore(time)
    buildMachine classify (store :> MachineStore) time, store, time

let private expectOk label result =
    match result with
    | Ok value -> value
    | Error error -> failtestf "%s: expected Ok, got %A" label error

let tests =
    testList
        "machine outcomes"
        [ testTask "a new transition returns Committed" {
              let machine, _, _ = newStoreAndMachine (RetryConfig.defaults<string>.Classify)
              let entity = entityId "OUT-1"

              let! outcome =
                  Machine.send machine entity (EventEnvelope.create "start-1" (Start 1)) noCancellation
                  |> mapTask (expectOk "send")

              Expect.equal Committed outcome "the first event commits"

              let! snapshot = Machine.state machine entity noCancellation |> mapTask (expectOk "state")

              match snapshot with
              | Some s ->
                  Expect.equal (Active 1) s.State "the state advanced"
                  Expect.equal 1UL (Epoch.value s.Epoch) "epoch advanced"
              | None -> failtest "expected a snapshot"
          }

          testTask "a repeated idempotency key returns AlreadyApplied" {
              let machine, _, _ = newStoreAndMachine (RetryConfig.defaults<string>.Classify)
              let entity = entityId "OUT-2"
              let envelope = EventEnvelope.create "start-1" (Start 1)

              let! first =
                  Machine.send machine entity envelope noCancellation
                  |> mapTask (expectOk "first")

              let! second =
                  Machine.send machine entity envelope noCancellation
                  |> mapTask (expectOk "second")

              Expect.equal Committed first "the first send commits"
              Expect.equal AlreadyApplied second "the replay is already applied"

              let! (snapshot: Snapshot<TestState> option) =
                  Machine.state machine entity noCancellation |> mapTask (expectOk "state")

              match snapshot with
              | Some s -> Expect.equal 1UL (Epoch.value s.Epoch) "the epoch did not advance on replay"
              | None -> failtest "expected a snapshot"
          }

          testTask "a Defer disposition enqueues durably and returns Deferred" {
              let machine, store, _ = newStoreAndMachine deferUnhandled
              let entity = entityId "OUT-3"

              let! outcome =
                  Machine.send machine entity (EventEnvelope.create "cancel-1" Cancel) noCancellation
                  |> mapTask (expectOk "send")

              match outcome with
              | Deferred retryId -> Expect.isGreaterThan (RetryId.value retryId) 0L "a durable retry id is assigned"
              | other -> failtestf "expected Deferred, got %A" other

              Expect.equal 1 (store.PendingRetries().Length) "the event is durably queued"
          }

          testTask "an Ignore disposition returns Ignored and writes nothing durable" {
              let machine, store, _ = newStoreAndMachine ignoreUnhandled
              let entity = entityId "OUT-4"

              let! outcome =
                  Machine.send machine entity (EventEnvelope.create "cancel-1" Cancel) noCancellation
                  |> mapTask (expectOk "send")

              Expect.equal Ignored outcome "unhandled is ignored"
              Expect.isEmpty (store.PendingRetries()) "nothing is queued"
          }

          testTask "a Reject disposition returns the original error and writes nothing" {
              let machine, store, _ = newStoreAndMachine rejectUnhandled
              let entity = entityId "OUT-5"

              let! result = Machine.send machine entity (EventEnvelope.create "cancel-1" Cancel) noCancellation

              match result with
              | Error(MachineError.Transition(TransitionError.Unhandled _)) -> ()
              | other -> failtestf "expected an unhandled transition error, got %A" other

              Expect.isEmpty (store.PendingRetries()) "rejection writes no retry"
          }

          testTask "a terminal entity rejects later events" {
              let classify error =
                  match error with
                  | MachineError.Rejected _ -> Disposition.Reject
                  | _ -> RetryConfig.defaults<string>.Classify error

              let machine, _, _ = newStoreAndMachine classify
              let entity = entityId "OUT-6"

              let! _ =
                  Machine.send machine entity (EventEnvelope.create "start-1" (Start 1)) noCancellation
                  |> mapTask (expectOk "start")

              let! _ =
                  Machine.send machine entity (EventEnvelope.create "finish-1" Finish) noCancellation
                  |> mapTask (expectOk "finish")

              let! result = Machine.send machine entity (EventEnvelope.create "start-2" (Start 2)) noCancellation

              match result with
              | Error(MachineError.Rejected(MachineRejection.InstanceNotRunning InstanceStatus.Terminated)) -> ()
              | other -> failtestf "expected a refusal, got %A" other
          } ]
