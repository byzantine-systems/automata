module ByzantineSystems.Automata.Storage.Postgres.Tests.PostgresOutboxTests

open System
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open Expecto
open TestContext

let private outboxKey entity eventKey ordinal : OutboxKey<Entity> =
    { MachineId = machine
      EntityId = entity
      EventIdempotencyKey = eventKey
      Ordinal = ordinal }

let tests =
    testList
        "Postgres IActionOutbox"
        [ testTask "committing actions writes outbox rows with ordinal keys" {
              do! reset ()
              let time = newTime ()
              let store = newStore time
              let s = store :> IStateStore<Entity, TestState, TestEvent, TestAction>
              let entity = entityId "ORD-11"

              let! _ =
                  s.Commit(
                      mkTransition
                          entity
                          "pay-1"
                          (Epoch.next Epoch.initial)
                          Idle
                          (Active 1)
                          [ Log "notify"; Log "reserve" ]
                          InstanceStatus.Running
                          startTime,
                      Epoch.initial,
                      noCancellation
                  )
                  |> mapTask (expectOk "commit")

              let o = store :> IActionOutbox<Entity, TestAction>

              let! (claimed: OutboxItem<Entity, TestAction> list) =
                  o.Claim(10, TimeSpan.FromSeconds 5., noCancellation)
                  |> mapTask (expectOk "claim")

              Expect.equal
                  [ outboxKey entity "pay-1" 0; outboxKey entity "pay-1" 1 ]
                  (claimed |> List.map (fun item -> item.ActionKey))
                  "action keys include their full machine-instance scope"
          }

          testTask "complete removes an action and fail reschedules it with the same key" {
              do! reset ()
              let time = newTime ()
              let store = newStore time
              let s = store :> IStateStore<Entity, TestState, TestEvent, TestAction>
              let entity = entityId "ORD-12"

              let! _ =
                  s.Commit(
                      mkTransition
                          entity
                          "pay-1"
                          (Epoch.next Epoch.initial)
                          Idle
                          (Active 1)
                          [ Log "notify"; Log "reserve" ]
                          InstanceStatus.Running
                          startTime,
                      Epoch.initial,
                      noCancellation
                  )
                  |> mapTask (expectOk "commit")

              let o = store :> IActionOutbox<Entity, TestAction>

              let! (claimed: OutboxItem<Entity, TestAction> list) =
                  o.Claim(10, TimeSpan.FromSeconds 5., noCancellation)
                  |> mapTask (expectOk "claim")

              Expect.equal 2 claimed.Length "both actions are claimable"

              do!
                  o.Complete(outboxKey entity "pay-1" 0, noCancellation)
                  |> mapTask (expectOkUnit "complete")

              do!
                  o.Fail(outboxKey entity "pay-1" 1, startTime.AddSeconds 20., noCancellation)
                  |> mapTask (expectOkUnit "fail")

              let! (notDue: OutboxItem<Entity, TestAction> list) =
                  o.Claim(10, TimeSpan.FromSeconds 5., noCancellation)
                  |> mapTask (expectOk "claim before due")

              Expect.isEmpty notDue "the failed action is rescheduled"

              time.Advance(TimeSpan.FromSeconds 21.)

              let! (due: OutboxItem<Entity, TestAction> list) =
                  o.Claim(10, TimeSpan.FromSeconds 5., noCancellation)
                  |> mapTask (expectOk "claim after due")

              match due with
              | [ item ] ->
                  Expect.equal (outboxKey entity "pay-1" 1) item.ActionKey "the same action key is redelivered"
                  Expect.equal 2 item.Attempts "attempts accumulate"
              | other -> failtestf "expected the failed action back, got %A" other
          } ]
