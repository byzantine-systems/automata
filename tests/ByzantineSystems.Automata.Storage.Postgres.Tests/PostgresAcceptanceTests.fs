module ByzantineSystems.Automata.Storage.Postgres.Tests.PostgresAcceptanceTests

open System
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open Expecto
open TestContext

let tests =
    testList
        "Postgres acceptance"
        [ testTask "the fsm schema contains no nullable columns" {
              do! reset ()

              Expect.isEmpty (nullableColumns ()) "the schema uses no NULLs"
          }

          testTask "machine_chart_version enforces WITHOUT OVERLAPS with an infinity upper bound" {
              do! reset ()

              let _ = exec "INSERT INTO fsm.machine (id, created_at) VALUES ('payments', now())"

              let v1 =
                  exec
                      "INSERT INTO fsm.machine_chart_version (machine_id, version, chart, valid_during) VALUES ('payments', 1, '{}', tstzrange('2026-01-01', '2026-02-01'))"

              Expect.isOk v1 "the first version inserts"

              let v2 =
                  exec
                      "INSERT INTO fsm.machine_chart_version (machine_id, version, chart, valid_during) VALUES ('payments', 2, '{}', tstzrange('2026-02-01', 'infinity'))"

              Expect.isOk v2 "an open upper bound of 'infinity' is not NULL"

              let overlap =
                  exec
                      "INSERT INTO fsm.machine_chart_version (machine_id, version, chart, valid_during) VALUES ('payments', 3, '{}', tstzrange('2026-01-15', '2026-03-01'))"

              Expect.isError overlap "an overlapping validity period is rejected"
          }

          testTask "an outbox collision rolls back the whole commit atomically" {
              do! reset ()
              let time = newTime ()
              let store = newStore time
              let s = store :> IStateStore<Entity, TestState, TestEvent, TestAction>
              let entity = entityId "ORD-ATOMIC"

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
                  |> mapTask (expectOk "first commit")

              let _ =
                  exec
                      "INSERT INTO fsm.outbox (machine_id, entity_id, event_idempotency_key, ordinal, action, attempts, next_attempt_at, locked_until) VALUES ('pg-tests', 'ORD-ATOMIC', 'pay-2', 0, '{}', 0, now(), '-infinity')"

              let! second =
                  s.Commit(
                      mkTransition
                          entity
                          "pay-2"
                          (Epoch.next (Epoch.next Epoch.initial))
                          (Active 1)
                          Done
                          [ Log "x" ]
                          InstanceStatus.Running
                          (startTime.AddSeconds 1.),
                      Epoch.next Epoch.initial,
                      noCancellation
                  )

              Expect.isError second "the outbox collision fails the commit"

              let! (snapshot: Snapshot<TestState> option) =
                  s.TryGet(machine, entity, noCancellation) |> mapTask (expectOk "tryGet")

              match snapshot with
              | Some snapshot ->
                  Expect.equal 1UL (Epoch.value snapshot.Epoch) "the epoch was not advanced (rolled back)"
              | None -> failtest "expected the first snapshot"

              let! (page: Transition<Entity, TestState, TestEvent, TestAction> list) =
                  s.History(machine, entity, Page.create 10, noCancellation)
                  |> mapTask (expectOk "history")

              Expect.equal 1 page.Length "the history was not appended (rolled back)"
          }

          testTask "simultaneous first commits: exactly one winner" {
              do! reset ()
              let time = newTime ()
              let store = newStore time
              let s = store :> IStateStore<Entity, TestState, TestEvent, TestAction>
              let entity = entityId "RACE-1"

              let send key =
                  s.Commit(
                      mkTransition
                          entity
                          key
                          (Epoch.next Epoch.initial)
                          Idle
                          (Active 1)
                          []
                          InstanceStatus.Running
                          startTime,
                      Epoch.initial,
                      noCancellation
                  )

              let! results = Task.WhenAll(send "race-a", send "race-b")

              let winners = results |> Array.filter Result.isOk |> Array.length
              Expect.equal 1 winners "exactly one first commit wins"

              let! (snapshot: Snapshot<TestState> option) =
                  s.TryGet(machine, entity, noCancellation) |> mapTask (expectOk "tryGet")

              match snapshot with
              | Some snapshot -> Expect.equal 1UL (Epoch.value snapshot.Epoch) "the epoch advanced exactly once"
              | None -> failtest "expected a snapshot"
          }

          testTask "two distinct writers at the same epoch: one winner, one Concurrency" {
              do! reset ()
              let time = newTime ()
              let store = newStore time
              let s = store :> IStateStore<Entity, TestState, TestEvent, TestAction>
              let entity = entityId "RACE-2"

              let! _ =
                  s.Commit(
                      mkTransition
                          entity
                          "seed"
                          (Epoch.next Epoch.initial)
                          Idle
                          (Active 1)
                          []
                          InstanceStatus.Running
                          startTime,
                      Epoch.initial,
                      noCancellation
                  )
                  |> mapTask (expectOk "seed")

              let epochOne = Epoch.next Epoch.initial

              let send key =
                  s.Commit(
                      mkTransition entity key (Epoch.next epochOne) (Active 1) Done [] InstanceStatus.Running startTime,
                      epochOne,
                      noCancellation
                  )

              let! results = Task.WhenAll(send "write-a", send "write-b")

              let wins = results |> Array.filter Result.isOk |> Array.length
              let conflicts = results |> Array.filter Result.isError |> Array.length
              Expect.equal 1 wins "exactly one writer advances the epoch"
              Expect.equal 1 conflicts "the other writer observes a conflict"
          }

          testTask "a durable retry survives a store restart" {
              do! reset ()
              let time = newTime ()
              let store1 = newStore time
              let q1 = store1 :> IRetryQueue<Entity, TestEvent>
              let entity = entityId "RESTART-1"

              let! _ =
                  q1.Enqueue(mkRequest entity "defer-1" startTime, noCancellation)
                  |> mapTask (expectOk "enqueue")

              // A brand-new store over the same database sees the pending work.
              let store2 = newStore time
              let q2 = store2 :> IRetryQueue<Entity, TestEvent>

              let! (claimed: RetryItem<Entity, TestEvent> list) =
                  q2.Claim(5, TimeSpan.FromSeconds 10., noCancellation)
                  |> mapTask (expectOk "claim")

              match claimed with
              | [ item ] ->
                  Expect.equal entity item.EntityId "the entity survives"
                  Expect.equal "defer-1" item.IdempotencyKey "the key survives"
              | other -> failtestf "expected the durable retry, got %A" other
          }

          testTask "two pumps never claim the same leased item" {
              do! reset ()
              let time = newTime ()
              let store = newStore time
              let q = store :> IRetryQueue<Entity, TestEvent>
              let entity = entityId "PUMPS-1"

              let! _ =
                  q.Enqueue(mkRequest entity "defer-1" startTime, noCancellation)
                  |> mapTask (expectOk "enqueue")

              let lease = TimeSpan.FromSeconds 10.
              let! results = Task.WhenAll(q.Claim(5, lease, noCancellation), q.Claim(5, lease, noCancellation))

              let claimed =
                  results
                  |> Array.collect (fun r ->
                      match r with
                      | Ok items -> List.toArray items
                      | Error _ -> [||])

              let distinctIds =
                  claimed
                  |> Array.map (fun item -> item.RetryId)
                  |> Array.distinct
                  |> Array.length

              Expect.equal 1 distinctIds "the single item is claimed exactly once across both pumps"
          } ]
