module ByzantineSystems.Automata.Storage.Postgres.Tests.PostgresRetryQueueTests

open System
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open Expecto
open TestContext

let tests =
    testList
        "Postgres IRetryQueue"
        [ testTask "enqueue returns an id and claim leases the due item" {
              do! reset ()
              let time = newTime ()
              let store = newStore time
              let q = store :> IRetryQueue<Entity, TestEvent>
              let entity = entityId "ORD-7"
              let lease = TimeSpan.FromSeconds 10.

              let! retryId =
                  q.Enqueue(mkRequest entity "defer-1" startTime, noCancellation)
                  |> mapTask (expectOk "enqueue")

              Expect.isGreaterThan (RetryId.value retryId) 0L "a durable id is assigned"

              let! (claimed: RetryItem<Entity, TestEvent> list) =
                  q.Claim(5, lease, noCancellation) |> mapTask (expectOk "claim")

              match claimed with
              | [ item ] ->
                  Expect.equal retryId item.RetryId "the claimed item matches the enqueued id"
                  Expect.equal 1 item.Attempts "claiming counts the attempt"
                  Expect.equal (Some(startTime.Add lease)) item.LockedUntil "the lease deadline is set"
              | other -> failtestf "expected exactly one claimed item, got %A" other
          }

          testTask "a leased item is invisible until the lease expires" {
              do! reset ()
              let time = newTime ()
              let store = newStore time
              let q = store :> IRetryQueue<Entity, TestEvent>
              let entity = entityId "ORD-8"
              let lease = TimeSpan.FromSeconds 10.

              let! _ =
                  q.Enqueue(mkRequest entity "defer-1" startTime, noCancellation)
                  |> mapTask (expectOk "enqueue")

              let! (first: RetryItem<Entity, TestEvent> list) =
                  q.Claim(5, lease, noCancellation) |> mapTask (expectOk "first claim")

              Expect.equal 1 first.Length "the first claim succeeds"

              let! (locked: RetryItem<Entity, TestEvent> list) =
                  q.Claim(5, lease, noCancellation) |> mapTask (expectOk "second claim")

              Expect.isEmpty locked "a leased item cannot be claimed twice"

              time.Advance(TimeSpan.FromSeconds 11.)

              let! (reclaimed: RetryItem<Entity, TestEvent> list) =
                  q.Claim(5, lease, noCancellation) |> mapTask (expectOk "reclaim")

              match reclaimed with
              | [ item ] -> Expect.equal 2 item.Attempts "attempts accumulate across leases"
              | other -> failtestf "expected the expired item back, got %A" other
          }

          testTask "complete removes the item; fail unlocks and reschedules" {
              do! reset ()
              let time = newTime ()
              let store = newStore time
              let q = store :> IRetryQueue<Entity, TestEvent>
              let entity = entityId "ORD-9"
              let lease = TimeSpan.FromSeconds 10.

              let! _ =
                  q.Enqueue(mkRequest entity "defer-1" startTime, noCancellation)
                  |> mapTask (expectOk "enqueue")

              let! _ =
                  q.Enqueue(mkRequest entity "defer-2" startTime, noCancellation)
                  |> mapTask (expectOk "enqueue")

              let! (claimed: RetryItem<Entity, TestEvent> list) =
                  q.Claim(5, lease, noCancellation) |> mapTask (expectOk "claim")

              let completed, failed =
                  match claimed with
                  | [ a; b ] -> a, b
                  | other -> failtestf "expected two claimed items, got %A" other

              do!
                  q.Complete(completed.RetryId, noCancellation)
                  |> mapTask (expectOkUnit "complete")

              do!
                  q.Fail(failed.RetryId, startTime.AddSeconds 30., "gateway down", noCancellation)
                  |> mapTask (expectOkUnit "fail")

              let! (remaining: RetryItem<Entity, TestEvent> list) =
                  q.Claim(5, lease, noCancellation) |> mapTask (expectOk "claim after")

              Expect.isEmpty remaining "the completed item is gone and the failed item is not yet due"

              time.Advance(TimeSpan.FromSeconds 31.)

              let! (due: RetryItem<Entity, TestEvent> list) =
                  q.Claim(5, lease, noCancellation) |> mapTask (expectOk "claim after due")

              match due with
              | [ item ] ->
                  Expect.equal failed.RetryId item.RetryId "the failed item becomes claimable again"
                  Expect.equal 2 item.Attempts "attempts accumulate across the retry cycle"
              | other -> failtestf "expected the failed item back, got %A" other
          }

          testTask "re-enqueueing a pending key refreshes the schedule and returns the same id" {
              do! reset ()
              let time = newTime ()
              let store = newStore time
              let q = store :> IRetryQueue<Entity, TestEvent>
              let entity = entityId "ORD-10"

              let! first =
                  q.Enqueue(mkRequest entity "defer-1" startTime, noCancellation)
                  |> mapTask (expectOk "first enqueue")

              let! second =
                  q.Enqueue(mkRequest entity "defer-1" (startTime.AddHours 1.), noCancellation)
                  |> mapTask (expectOk "second enqueue")

              Expect.equal first second "the refresh returns the original id"

              let! (claimed: RetryItem<Entity, TestEvent> list) =
                  q.Claim(5, TimeSpan.FromSeconds 10., noCancellation)
                  |> mapTask (expectOk "claim")

              Expect.isEmpty claimed "the refreshed item is not due yet"
          } ]
