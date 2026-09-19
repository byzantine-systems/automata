module ByzantineSystems.Automata.Runtime.Tests.InMemoryStoreTests

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.InMemory
open Expecto

/// Phantom marker so entity ids are tied to a domain type in tests.
type Payment = class end

/// Minimal task mapper so results can be unwrapped inside bind pipelines.
let mapTask (mapping: 'T -> 'U) (source: Task<'T>) : Task<'U> =
    task {
        let! value = source
        return mapping value
    }

let machine = machineId "payments"
let ct = CancellationToken.None
let startTime = DateTimeOffset(2026, 9, 19, 12, 0, 0, TimeSpan.Zero)

type Transition = Transition<EntityId<Payment>, string, string, string>
type RetryList = RetryItem<EntityId<Payment>, string> list
type OutboxList = OutboxItem<EntityId<Payment>, string> list

/// Deterministic clock: leases and due times advance only when the test says so.
type FakeTime(start: DateTimeOffset) =
    inherit TimeProvider()
    let mutable current = start

    override _.GetUtcNow() : DateTimeOffset = current

    member _.Advance(span: TimeSpan) = current <- current.Add span

let newStore () : InMemoryStore<EntityId<Payment>, string, string, string> * FakeTime =
    let time = FakeTime(startTime)
    InMemoryStore<EntityId<Payment>, string, string, string>(time), time

let asStateStore
    (store: InMemoryStore<'EntityId, 'State, 'Event, 'Action>)
    : IStateStore<'EntityId, 'State, 'Event, 'Action> =
    store :> IStateStore<'EntityId, 'State, 'Event, 'Action>

let asRetryQueue (store: InMemoryStore<'EntityId, 'State, 'Event, 'Action>) : IRetryQueue<'EntityId, 'Event> =
    store :> IRetryQueue<'EntityId, 'Event>

let asDeadLetterStore (store: InMemoryStore<'EntityId, 'State, 'Event, 'Action>) : IDeadLetterStore<'EntityId, 'Event> =
    store :> IDeadLetterStore<'EntityId, 'Event>

let asOutbox (store: InMemoryStore<'EntityId, 'State, 'Event, 'Action>) : IActionOutbox<'EntityId, 'Action> =
    store :> IActionOutbox<'EntityId, 'Action>

let mkTransition
    (entity: EntityId<Payment>)
    (key: string)
    (epoch: Epoch)
    (fromState: string)
    (toState: string)
    (actions: string list)
    (status: InstanceStatus)
    (occurredAt: DateTimeOffset)
    : Transition =
    { MachineId = machine
      EntityId = entity
      IdempotencyKey = key
      OccurredAt = occurredAt
      Epoch = epoch
      Event = "GatewayProcessed"
      Actions = actions
      FromState = fromState
      ToState = toState
      Status = status
      HandledBy = stateId "active.processing"
      Exited = []
      Entered = [] }

let mkRequest
    (entity: EntityId<Payment>)
    (key: string)
    (nextAttemptAt: DateTimeOffset)
    : RetryRequest<EntityId<Payment>, string> =
    { MachineId = machine
      EntityId = entity
      IdempotencyKey = key
      Event = "GatewayProcessed"
      NextAttemptAt = nextAttemptAt
      LastError = Some "transient store failure" }

let expectOk label result =
    match result with
    | Ok value -> value
    | Error error -> failtestf "%s: expected Ok, got %A" label error

let expectOkUnit label result =
    match result with
    | Ok() -> ()
    | Error error -> failtestf "%s: expected Ok, got %A" label error

let stateStoreTests =
    testList
        "IStateStore"
        [ testTask "TryGet returns None for an unknown entity" {
              let store, _ = newStore ()
              let s = asStateStore store

              let! found = s.TryGet(machine, entityId "ORD-404", ct)
              Expect.equal (Ok None) found "unknown entity has no snapshot"
          }

          testTask "first commit creates the instance and advances the epoch" {
              let store, _ = newStore ()
              let s = asStateStore store
              let entity = entityId "ORD-1"

              let! (receipt: CommitReceipt) =
                  s.Commit(
                      mkTransition entity "pay-1" (Epoch.next Epoch.initial) "idle" "active" [] Running startTime,
                      Epoch.initial,
                      ct
                  )
                  |> mapTask (expectOk "commit")

              Expect.equal 1UL (Epoch.value receipt.Epoch) "first commit lands at epoch 1"
              Expect.equal "pay-1" receipt.IdempotencyKey "receipt carries the event key"

              let! (snapshot: Snapshot<string> option) = s.TryGet(machine, entity, ct) |> mapTask (expectOk "tryGet")

              match snapshot with
              | Some snapshot ->
                  Expect.equal "active" snapshot.State "snapshot holds the target state"
                  Expect.equal 1UL (Epoch.value snapshot.Epoch) "snapshot holds the committed epoch"
                  Expect.equal Running snapshot.Status "a new instance is running"
              | None -> failtest "expected a snapshot after the first commit"
          }

          testTask "a second commit advances the epoch" {
              let store, _ = newStore ()
              let s = asStateStore store
              let entity = entityId "ORD-2"

              let! (first: CommitReceipt) =
                  s.Commit(
                      mkTransition entity "pay-1" (Epoch.next Epoch.initial) "idle" "active" [] Running startTime,
                      Epoch.initial,
                      ct
                  )
                  |> mapTask (expectOk "first")

              let! (second: CommitReceipt) =
                  s.Commit(
                      mkTransition
                          entity
                          "pay-2"
                          (Epoch.next first.Epoch)
                          "active"
                          "awaiting"
                          []
                          Running
                          (startTime.AddSeconds 1.),
                      first.Epoch,
                      ct
                  )
                  |> mapTask (expectOk "second")

              Expect.equal 2UL (Epoch.value second.Epoch) "the epoch advanced exactly once per commit"
          }

          testTask "a stale expected epoch is rejected as Concurrency" {
              let store, _ = newStore ()
              let s = asStateStore store
              let entity = entityId "ORD-3"

              let! _ =
                  s.Commit(
                      mkTransition entity "pay-1" (Epoch.next Epoch.initial) "idle" "active" [] Running startTime,
                      Epoch.initial,
                      ct
                  )
                  |> mapTask (expectOk "first")

              let! result =
                  s.Commit(
                      mkTransition
                          entity
                          "pay-2"
                          (Epoch.next (Epoch.next Epoch.initial))
                          "active"
                          "failed"
                          []
                          Running
                          startTime,
                      Epoch.initial,
                      ct
                  )

              match result with
              | Error(Concurrency(expected, actual)) ->
                  Expect.equal
                      (0UL, 1UL)
                      (Epoch.value expected, Epoch.value actual)
                      "expected and actual epochs are reported"
              | other -> failtestf "expected a concurrency error, got %A" other
          }

          testTask "a duplicate idempotency key returns the original receipt without writing" {
              let store, _ = newStore ()
              let s = asStateStore store
              let entity = entityId "ORD-4"

              let! (original: CommitReceipt) =
                  s.Commit(
                      mkTransition entity "pay-1" (Epoch.next Epoch.initial) "idle" "active" [] Running startTime,
                      Epoch.initial,
                      ct
                  )
                  |> mapTask (expectOk "original")

              let! (replay: CommitReceipt) =
                  s.Commit(
                      mkTransition
                          entity
                          "pay-1"
                          (Epoch.next original.Epoch)
                          "active"
                          "zombie"
                          []
                          Running
                          (startTime.AddMinutes 5.),
                      original.Epoch,
                      ct
                  )
                  |> mapTask (expectOk "replay")

              Expect.equal original replay "the original receipt is returned verbatim"

              let! (snapshot: Snapshot<string> option) = s.TryGet(machine, entity, ct) |> mapTask (expectOk "tryGet")

              match snapshot with
              | Some snapshot ->
                  Expect.equal "active" snapshot.State "the replay wrote nothing"
                  Expect.equal 1UL (Epoch.value snapshot.Epoch) "the epoch did not advance"
              | None -> failtest "expected a snapshot"

              let! (page: Transition list) =
                  s.History(machine, entity, Page.create 100, ct) |> mapTask (expectOk "history")

              Expect.equal 1 page.Length "exactly one log entry exists"
          }

          testTask "committing a malformed epoch is rejected as Concurrency" {
              let store, _ = newStore ()
              let s = asStateStore store
              let entity = entityId "ORD-5"

              let! result =
                  s.Commit(
                      mkTransition entity "pay-1" (Epoch.ofUInt64 7UL) "idle" "active" [] Running startTime,
                      Epoch.initial,
                      ct
                  )

              match result with
              | Error(Concurrency(expected, actual)) ->
                  Expect.equal (0UL, 0UL) (Epoch.value expected, Epoch.value actual) "the gapless invariant is enforced"
              | other -> failtestf "expected a concurrency error, got %A" other
          }

          testTask "FindReceipt answers None before and Some after a commit" {
              let store, _ = newStore ()
              let s = asStateStore store
              let entity = entityId "ORD-6"

              let! (before: CommitReceipt option) =
                  s.FindReceipt(machine, entity, "pay-1", ct) |> mapTask (expectOk "before")

              Expect.isNone before "no receipt before the commit"

              let! _ =
                  s.Commit(
                      mkTransition entity "pay-1" (Epoch.next Epoch.initial) "idle" "active" [] Running startTime,
                      Epoch.initial,
                      ct
                  )
                  |> mapTask (expectOk "commit")

              let! (after: CommitReceipt option) =
                  s.FindReceipt(machine, entity, "pay-1", ct) |> mapTask (expectOk "after")

              match after with
              | Some receipt -> Expect.equal 1UL (Epoch.value receipt.Epoch) "the receipt records the commit epoch"
              | None -> failtest "expected a receipt after the commit"
          }

          testTask "history pages ascend by epoch with an exclusive cursor" {
              let store, _ = newStore ()
              let s = asStateStore store
              let entity = entityId "ORD-7"

              let! seed =
                  task {
                      for ordinal in 1..3 do
                          let key = $"pay-%d{ordinal}"
                          let epoch = Epoch.ofUInt64 (uint64 ordinal)
                          let occurredAt = startTime.AddSeconds(float ordinal)

                          let! committed =
                              s.Commit(
                                  mkTransition entity key epoch "idle" $"s%d{ordinal}" [] Running occurredAt,
                                  Epoch.ofUInt64 (uint64 (ordinal - 1)),
                                  ct
                              )

                          match committed with
                          | Ok _ -> ()
                          | Error error -> return failtestf "seed commit %d failed: %A" ordinal error
                  }

              ignore seed

              let! (firstPage: Transition list) =
                  s.History(machine, entity, Page.create 2, ct) |> mapTask (expectOk "first page")

              Expect.equal [ 1UL; 2UL ] (firstPage |> List.map (fun t -> Epoch.value t.Epoch)) "limit bounds the page"

              let! (secondPage: Transition list) =
                  s.History(machine, entity, Page.after (Epoch.ofUInt64 2UL) 10, ct)
                  |> mapTask (expectOk "second page")

              Expect.equal
                  [ 3UL ]
                  (secondPage |> List.map (fun t -> Epoch.value t.Epoch))
                  "cursor excludes earlier rows"

              let! (unknown: Transition list) =
                  s.History(machine, entityId "ORD-404", Page.create 10, ct)
                  |> mapTask (expectOk "unknown entity")

              Expect.isEmpty unknown "an unknown entity has an empty log"
          }

          testTask "a terminal status is persisted with the snapshot" {
              let store, _ = newStore ()
              let s = asStateStore store
              let entity = entityId "ORD-8"

              let! _ =
                  s.Commit(
                      mkTransition entity "pay-1" (Epoch.next Epoch.initial) "idle" "done" [] Terminated startTime,
                      Epoch.initial,
                      ct
                  )
                  |> mapTask (expectOk "commit")

              let! (snapshot: Snapshot<string> option) = s.TryGet(machine, entity, ct) |> mapTask (expectOk "tryGet")

              match snapshot with
              | Some snapshot -> Expect.equal Terminated snapshot.Status "termination is durable"
              | None -> failtest "expected a snapshot"
          }

          testTask "concurrent first commits: exactly one winner" {
              let store, _ = newStore ()
              let s = asStateStore store
              let entity = entityId "RACE-1"

              let send key =
                  s.Commit(
                      mkTransition entity key (Epoch.next Epoch.initial) "idle" "active" [] Running startTime,
                      Epoch.initial,
                      ct
                  )

              let! results = Task.WhenAll(send "race-a", send "race-b")

              let winners =
                  results
                  |> Array.filter (fun r ->
                      match r with
                      | Ok _ -> true
                      | Error _ -> false)
                  |> Array.length

              Expect.equal 1 winners "exactly one commit wins the epoch race"

              let! (snapshot: Snapshot<string> option) = s.TryGet(machine, entity, ct) |> mapTask (expectOk "tryGet")

              match snapshot with
              | Some snapshot -> Expect.equal 1UL (Epoch.value snapshot.Epoch) "the epoch advanced exactly once"
              | None -> failtest "expected a snapshot"
          } ]

let retryQueueTests =
    testList
        "IRetryQueue"
        [ testTask "claim leases a due item and counts the attempt" {
              let store, _ = newStore ()
              let q = asRetryQueue store
              let entity = entityId "ORD-9"
              let lease = TimeSpan.FromSeconds 10.

              do!
                  q.Enqueue(mkRequest entity "defer-1" startTime, ct)
                  |> mapTask (expectOkUnit "enqueue")

              let! (claimed: RetryList) = q.Claim(5, lease, ct) |> mapTask (expectOk "claim")

              match claimed with
              | [ item ] ->
                  Expect.equal 1 item.Attempts "claiming counts the attempt"
                  Expect.equal (Some(startTime.Add lease)) item.LockedUntil "the lease deadline is set"
                  Expect.equal (Some "transient store failure") item.LastError "the deferral reason travels"
              | other -> failtestf "expected exactly one claimed item, got %A" other
          }

          testTask "a leased item is invisible until the lease expires" {
              let store, time = newStore ()
              let q = asRetryQueue store
              let entity = entityId "ORD-10"

              do!
                  q.Enqueue(mkRequest entity "defer-1" startTime, ct)
                  |> mapTask (expectOkUnit "enqueue")

              let! (first: RetryList) = q.Claim(5, TimeSpan.FromSeconds 10., ct) |> mapTask (expectOk "first claim")
              Expect.equal 1 first.Length "the first claim succeeds"

              let! (locked: RetryList) = q.Claim(5, TimeSpan.FromSeconds 10., ct) |> mapTask (expectOk "second claim")
              Expect.isEmpty locked "a leased item cannot be claimed twice"

              time.Advance(TimeSpan.FromSeconds 11.)

              let! (reclaimed: RetryList) = q.Claim(5, TimeSpan.FromSeconds 10., ct) |> mapTask (expectOk "reclaim")

              match reclaimed with
              | [ item ] -> Expect.equal 2 item.Attempts "attempts accumulate across leases"
              | other -> failtestf "expected the expired item back, got %A" other
          }

          testTask "an item that is not yet due is not claimed" {
              let store, _ = newStore ()
              let q = asRetryQueue store
              let entity = entityId "ORD-11"

              do!
                  q.Enqueue(mkRequest entity "defer-1" (startTime.AddHours 1.), ct)
                  |> mapTask (expectOkUnit "enqueue")

              let! (claimed: RetryList) = q.Claim(5, TimeSpan.FromSeconds 10., ct) |> mapTask (expectOk "claim")
              Expect.isEmpty claimed "future items stay put"
          }

          testTask "complete removes the item" {
              let store, _ = newStore ()
              let q = asRetryQueue store
              let entity = entityId "ORD-12"

              do!
                  q.Enqueue(mkRequest entity "defer-1" startTime, ct)
                  |> mapTask (expectOkUnit "enqueue")

              let! (claimed: RetryList) = q.Claim(5, TimeSpan.FromSeconds 10., ct) |> mapTask (expectOk "claim")

              match claimed with
              | [ item ] -> do! q.Complete(item.RetryId, ct) |> mapTask (expectOkUnit "complete")
              | other -> failtestf "expected one item, got %A" other

              let! (empty: RetryList) =
                  q.Claim(5, TimeSpan.FromSeconds 10., ct)
                  |> mapTask (expectOk "claim after complete")

              Expect.isEmpty empty "a completed item is gone"

              do!
                  q.Complete(RetryId.create 999L, ct)
                  |> mapTask (expectOkUnit "complete unknown id")
          }

          testTask "re-enqueueing a pending key refreshes its schedule without duplicating" {
              let store, _ = newStore ()
              let q = asRetryQueue store
              let entity = entityId "ORD-13"
              let later = startTime.AddMinutes 2.

              do!
                  q.Enqueue(mkRequest entity "defer-1" startTime, ct)
                  |> mapTask (expectOkUnit "first enqueue")

              do!
                  q.Enqueue(
                      { mkRequest entity "defer-1" later with
                          LastError = Some "still failing" },
                      ct
                  )
                  |> mapTask (expectOkUnit "second enqueue")

              let pending = store.PendingRetries()
              Expect.equal 1 pending.Length "the item is not duplicated"

              match pending with
              | [ item ] ->
                  Expect.equal later item.NextAttemptAt "the schedule is refreshed"
                  Expect.equal (Some "still failing") item.LastError "the latest error wins"
              | other -> failtestf "expected one item, got %A" other
          }

          testTask "fail unlocks, reschedules, and records the error" {
              let store, time = newStore ()
              let q = asRetryQueue store
              let entity = entityId "ORD-14"
              let next = startTime.AddSeconds 30.

              do!
                  q.Enqueue(mkRequest entity "defer-1" startTime, ct)
                  |> mapTask (expectOkUnit "enqueue")

              let! (claimed: RetryList) = q.Claim(5, TimeSpan.FromSeconds 10., ct) |> mapTask (expectOk "claim")

              match claimed with
              | [ item ] ->
                  do!
                      q.Fail(item.RetryId, next, "gateway still down", ct)
                      |> mapTask (expectOkUnit "fail")
              | other -> failtestf "expected one item, got %A" other

              match store.PendingRetries() with
              | [ item ] ->
                  Expect.equal next item.NextAttemptAt "rescheduled to the given time"
                  Expect.equal None item.LockedUntil "the lease is released"
                  Expect.equal (Some "gateway still down") item.LastError "the error is recorded"
              | other -> failtestf "expected one item, got %A" other

              let! (notDue: RetryList) =
                  q.Claim(5, TimeSpan.FromSeconds 10., ct)
                  |> mapTask (expectOk "claim before due")

              Expect.isEmpty notDue "the rescheduled item is not yet due"

              time.Advance(TimeSpan.FromSeconds 31.)

              let! (due: RetryList) = q.Claim(5, TimeSpan.FromSeconds 10., ct) |> mapTask (expectOk "claim after due")
              Expect.equal 1 due.Length "the item becomes claimable again"
          } ]

let outboxTests =
    testList
        "IActionOutbox"
        [ testTask "committing actions writes outbox rows with ordinal keys" {
              let store, _ = newStore ()
              let s = asStateStore store
              let entity = entityId "ORD-15"

              let! _ =
                  s.Commit(
                      mkTransition
                          entity
                          "pay-1"
                          (Epoch.next Epoch.initial)
                          "idle"
                          "active"
                          [ "notify"; "reserve" ]
                          Running
                          startTime,
                      Epoch.initial,
                      ct
                  )
                  |> mapTask (expectOk "commit")

              let pending = store.PendingOutbox() |> List.sortBy (fun item -> item.ActionKey)

              Expect.equal
                  [ "pay-1#0"; "pay-1#1" ]
                  (pending |> List.map (fun item -> item.ActionKey))
                  "action keys derive from the event key and ordinal"

              Expect.equal
                  [ "notify"; "reserve" ]
                  (pending |> List.map (fun item -> item.Action))
                  "actions keep their order"

              Expect.equal 0 (pending |> List.sumBy (fun item -> item.Attempts)) "nothing has been attempted yet"
          }

          testTask "claim leases due actions; complete and fail drive their lifecycle" {
              let store, time = newStore ()
              let s = asStateStore store
              let o = asOutbox store
              let entity = entityId "ORD-16"

              let! _ =
                  s.Commit(
                      mkTransition
                          entity
                          "pay-1"
                          (Epoch.next Epoch.initial)
                          "idle"
                          "active"
                          [ "notify"; "reserve" ]
                          Running
                          startTime,
                      Epoch.initial,
                      ct
                  )
                  |> mapTask (expectOk "commit")

              let! (claimed: OutboxList) = o.Claim(10, TimeSpan.FromSeconds 5., ct) |> mapTask (expectOk "claim")
              Expect.equal 2 claimed.Length "both actions are claimable"
              Expect.isTrue (claimed |> List.forall (fun item -> item.Attempts = 1)) "claiming counts the attempt"

              do! o.Complete("pay-1#0", ct) |> mapTask (expectOkUnit "complete")
              Expect.equal 1 (store.PendingOutbox().Length) "the delivered action is removed"

              do! o.Fail("pay-1#1", startTime.AddSeconds 20., ct) |> mapTask (expectOkUnit "fail")

              let! (notDue: OutboxList) =
                  o.Claim(10, TimeSpan.FromSeconds 5., ct)
                  |> mapTask (expectOk "claim before due")

              Expect.isEmpty notDue "the failed action is rescheduled"

              time.Advance(TimeSpan.FromSeconds 21.)

              let! (due: OutboxList) = o.Claim(10, TimeSpan.FromSeconds 5., ct) |> mapTask (expectOk "claim after due")

              match due with
              | [ item ] ->
                  Expect.equal "pay-1#1" item.ActionKey "the same action key is redelivered"
                  Expect.equal 2 item.Attempts "attempts accumulate"
              | other -> failtestf "expected the failed action back, got %A" other
          } ]

let deadLetterTests =
    testList
        "IDeadLetterStore"
        [ testTask "record appends an inspectable dead letter" {
              let store, _ = newStore ()
              let d = asDeadLetterStore store
              let entity = entityId "ORD-17"

              let letter: DeadLetter<EntityId<Payment>, string> =
                  { MachineId = machine
                    EntityId = entity
                    IdempotencyKey = "pay-1"
                    Event = "GatewayProcessed"
                    FinalError = "max attempts exceeded"
                    Attempts = 5
                    DiedAt = startTime }

              do! d.Record(letter, ct) |> mapTask (expectOkUnit "record")

              match store.DeadLetterLog() with
              | [ recorded ] ->
                  Expect.equal "max attempts exceeded" recorded.FinalError "the final error survives"
                  Expect.equal 5 recorded.Attempts "the attempt count survives"
              | other -> failtestf "expected one dead letter, got %A" other
          } ]

let contractTypeTests =
    testList
        "contract types"
        [ test "EventEnvelope requires a non-empty idempotency key" {
              Expect.throws (fun _ -> EventEnvelope.create "" "e" |> ignore) "an empty key raises"
              Expect.throws (fun _ -> EventEnvelope.create "   " "e" |> ignore) "a whitespace key raises"
          }

          test "EventEnvelope carries event, tracing metadata, and a trimmed key" {
              let envelope =
                  EventEnvelope.create " pay-1 " "GatewayProcessed"
                  |> EventEnvelope.withCausation "cmd-9"
                  |> EventEnvelope.withCorrelation "wf-4"

              Expect.equal "pay-1" (EventEnvelope.idempotencyKey envelope) "the key is trimmed"
              Expect.equal "GatewayProcessed" (EventEnvelope.event envelope) "the payload is readable"
              Expect.equal (Some "cmd-9") (EventEnvelope.causationId envelope) "causation is attached"
              Expect.equal (Some "wf-4") (EventEnvelope.correlationId envelope) "correlation is attached"
          }

          test "Page validates its limit and exposes its cursor" {
              Expect.throws (fun _ -> Page.create 0 |> ignore) "a zero limit raises"
              Expect.throws (fun _ -> Page.after Epoch.initial 0 |> ignore) "a zero limit raises with a cursor"

              let first: Page = Page.create 25
              Expect.equal (None, 25) (Page.cursor first, Page.limit first) "the first page has no cursor"

              let next: Page = Page.after (Epoch.ofUInt64 3UL) 25
              Expect.equal (Some(Epoch.ofUInt64 3UL), 25) (Page.cursor next, Page.limit next) "the cursor is exclusive"
          }

          test "RetryId validates positivity and roundtrips" {
              Expect.throws (fun _ -> RetryId.create 0L |> ignore) "zero raises"
              Expect.throws (fun _ -> RetryId.create -1L |> ignore) "a negative counter raises"
              Expect.equal 42L (RetryId.value (RetryId.create 42L)) "the counter roundtrips"
          } ]

let tests =
    testList
        "in-memory store"
        [ stateStoreTests
          retryQueueTests
          outboxTests
          deadLetterTests
          contractTypeTests ]
