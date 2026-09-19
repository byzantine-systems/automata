module ByzantineSystems.Automata.Storage.Postgres.Tests.PostgresStoreTests

open System
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open Expecto
open TestContext

let tests =
    testList
        "Postgres IStateStore"
        [ testTask "TryGet returns None for an unknown entity" {
              do! reset ()
              let time = newTime ()
              let s = newStore time :> IStateStore<Entity, TestState, TestEvent, TestAction>

              let! found = s.TryGet(machine, entityId "ORD-404", noCancellation)
              Expect.equal (Ok None) found "unknown entity has no snapshot"
          }

          testTask "first commit creates the instance and advances the epoch" {
              do! reset ()
              let time = newTime ()
              let s = newStore time :> IStateStore<Entity, TestState, TestEvent, TestAction>
              let entity = entityId "ORD-1"

              let! (receipt: CommitReceipt) =
                  s.Commit(
                      mkTransition
                          entity
                          "pay-1"
                          (Epoch.next Epoch.initial)
                          Idle
                          (Active 1)
                          []
                          InstanceStatus.Running
                          startTime,
                      Epoch.initial,
                      noCancellation
                  )
                  |> mapTask (expectOk "commit")

              Expect.equal 1UL (Epoch.value receipt.Epoch) "first commit lands at epoch 1"

              let! (snapshot: Snapshot<TestState> option) =
                  s.TryGet(machine, entity, noCancellation) |> mapTask (expectOk "tryGet")

              match snapshot with
              | Some snapshot ->
                  Expect.equal (Active 1) snapshot.State "snapshot holds the target state"
                  Expect.equal 1UL (Epoch.value snapshot.Epoch) "snapshot holds the committed epoch"
                  Expect.equal InstanceStatus.Running snapshot.Status "a new instance is running"
              | None -> failtest "expected a snapshot after the first commit"
          }

          testTask "a second commit advances the epoch" {
              do! reset ()
              let time = newTime ()
              let s = newStore time :> IStateStore<Entity, TestState, TestEvent, TestAction>
              let entity = entityId "ORD-2"

              let! (first: CommitReceipt) =
                  s.Commit(
                      mkTransition
                          entity
                          "pay-1"
                          (Epoch.next Epoch.initial)
                          Idle
                          (Active 1)
                          []
                          InstanceStatus.Running
                          startTime,
                      Epoch.initial,
                      noCancellation
                  )
                  |> mapTask (expectOk "first")

              let! (second: CommitReceipt) =
                  s.Commit(
                      mkTransition
                          entity
                          "pay-2"
                          (Epoch.next first.Epoch)
                          (Active 1)
                          Done
                          []
                          InstanceStatus.Running
                          (startTime.AddSeconds 1.),
                      first.Epoch,
                      noCancellation
                  )
                  |> mapTask (expectOk "second")

              Expect.equal 2UL (Epoch.value second.Epoch) "the epoch advanced exactly once per commit"
          }

          testTask "a stale expected epoch is rejected as Concurrency" {
              do! reset ()
              let time = newTime ()
              let s = newStore time :> IStateStore<Entity, TestState, TestEvent, TestAction>
              let entity = entityId "ORD-3"

              let! _ =
                  s.Commit(
                      mkTransition
                          entity
                          "pay-1"
                          (Epoch.next Epoch.initial)
                          Idle
                          (Active 1)
                          []
                          InstanceStatus.Running
                          startTime,
                      Epoch.initial,
                      noCancellation
                  )
                  |> mapTask (expectOk "first")

              let! result =
                  s.Commit(
                      mkTransition
                          entity
                          "pay-2"
                          (Epoch.next (Epoch.next Epoch.initial))
                          (Active 1)
                          Done
                          []
                          InstanceStatus.Running
                          startTime,
                      Epoch.initial,
                      noCancellation
                  )

              match result with
              | Error(StoreError.Concurrency(expected, actual)) ->
                  Expect.equal
                      (0UL, 1UL)
                      (Epoch.value expected, Epoch.value actual)
                      "expected and actual epochs reported"
              | other -> failtestf "expected a concurrency error, got %A" other
          }

          testTask "a duplicate idempotency key returns the original receipt without writing" {
              do! reset ()
              let time = newTime ()
              let s = newStore time :> IStateStore<Entity, TestState, TestEvent, TestAction>
              let entity = entityId "ORD-4"

              let! (original: CommitReceipt) =
                  s.Commit(
                      mkTransition
                          entity
                          "pay-1"
                          (Epoch.next Epoch.initial)
                          Idle
                          (Active 1)
                          []
                          InstanceStatus.Running
                          startTime,
                      Epoch.initial,
                      noCancellation
                  )
                  |> mapTask (expectOk "original")

              let! (replay: CommitReceipt) =
                  s.Commit(
                      mkTransition
                          entity
                          "pay-1"
                          (Epoch.next original.Epoch)
                          (Active 1)
                          Done
                          []
                          InstanceStatus.Running
                          (startTime.AddMinutes 5.),
                      original.Epoch,
                      noCancellation
                  )
                  |> mapTask (expectOk "replay")

              Expect.equal original replay "the original receipt is returned verbatim"

              let! (snapshot: Snapshot<TestState> option) =
                  s.TryGet(machine, entity, noCancellation) |> mapTask (expectOk "tryGet")

              match snapshot with
              | Some snapshot ->
                  Expect.equal (Active 1) snapshot.State "the replay wrote nothing"
                  Expect.equal 1UL (Epoch.value snapshot.Epoch) "the epoch did not advance"
              | None -> failtest "expected a snapshot"

              let! (page: Transition<Entity, TestState, TestEvent, TestAction> list) =
                  s.History(machine, entity, Page.create 100, noCancellation)
                  |> mapTask (expectOk "history")

              Expect.equal 1 page.Length "exactly one log entry exists"
          }

          testTask "history pages ascend by epoch with an exclusive cursor" {
              do! reset ()
              let time = newTime ()
              let s = newStore time :> IStateStore<Entity, TestState, TestEvent, TestAction>
              let entity = entityId "ORD-5"

              let rec seed ordinal =
                  task {
                      if ordinal <= 3 then
                          let key = $"pay-%d{ordinal}"
                          let epoch = Epoch.ofUInt64 (uint64 ordinal)
                          let occurredAt = startTime.AddSeconds(float ordinal)

                          let! committed =
                              s.Commit(
                                  mkTransition
                                      entity
                                      key
                                      epoch
                                      Idle
                                      (Active ordinal)
                                      []
                                      InstanceStatus.Running
                                      occurredAt,
                                  Epoch.ofUInt64 (uint64 (ordinal - 1)),
                                  noCancellation
                              )

                          match committed with
                          | Ok _ -> return! seed (ordinal + 1)
                          | Error error -> return failtestf "seed commit %d failed: %A" ordinal error
                  }

              do! seed 1

              let! (firstPage: Transition<Entity, TestState, TestEvent, TestAction> list) =
                  s.History(machine, entity, Page.create 2, noCancellation)
                  |> mapTask (expectOk "first page")

              Expect.equal [ 1UL; 2UL ] (firstPage |> List.map (fun t -> Epoch.value t.Epoch)) "limit bounds the page"

              let! (secondPage: Transition<Entity, TestState, TestEvent, TestAction> list) =
                  s.History(machine, entity, Page.after (Epoch.ofUInt64 2UL) 10, noCancellation)
                  |> mapTask (expectOk "second page")

              Expect.equal
                  [ 3UL ]
                  (secondPage |> List.map (fun t -> Epoch.value t.Epoch))
                  "cursor excludes earlier rows"
          }

          testTask "a terminal status is persisted and read back through history" {
              do! reset ()
              let time = newTime ()
              let s = newStore time :> IStateStore<Entity, TestState, TestEvent, TestAction>
              let entity = entityId "ORD-6"

              let! _ =
                  s.Commit(
                      mkTransition
                          entity
                          "pay-1"
                          (Epoch.next Epoch.initial)
                          Idle
                          Done
                          []
                          InstanceStatus.Terminated
                          startTime,
                      Epoch.initial,
                      noCancellation
                  )
                  |> mapTask (expectOk "commit")

              let! (page: Transition<Entity, TestState, TestEvent, TestAction> list) =
                  s.History(machine, entity, Page.create 10, noCancellation)
                  |> mapTask (expectOk "history")

              match page with
              | [ transition ] ->
                  Expect.equal InstanceStatus.Terminated transition.Status "termination is durable in the log"
              | other -> failtestf "expected one transition, got %A" other
          } ]
