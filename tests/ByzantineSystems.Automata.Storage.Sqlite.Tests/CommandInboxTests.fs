module ByzantineSystems.Automata.Storage.Sqlite.Tests.CommandInboxTests

open System
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Sqlite.Tests.TestContext
open Expecto

let private entity name : Entity = entityId name

let private backoff =
    Backoff.create (TimeSpan.FromMilliseconds 50.0) (TimeSpan.FromSeconds 1.0)

let private claimOne (inbox: Inbox) : Task<Claimed> =
    task {
        match! claim inbox 1 with
        | [ single ] -> return single
        | other -> return failtestf "expected exactly one claimed command, got %i" (List.length other)
    }

let private reject (fixture: Fixture) (held: Claimed) =
    task {
        let! outcome =
            (processorOf fixture)
                .Reject(held.Work.CommandId, held.Token, CommandFailure.Domain(Refused "done"), noCancellation)

        return outcome |> expectOk "reject"
    }

let private readBack (inbox: Inbox) (commandId: CommandId) =
    task {
        match! inbox.TryGet(commandId, noCancellation) with
        | Ok(Some record) -> return record
        | other -> return failtestf "the command should exist, got %A" other
    }

let tests =
    testList
        "ICommandInbox"
        [ testTask "a repeated idempotency key returns the original command" {
              let db = fixture "inbox"
              let inbox = inboxOf db

              let! first = inbox.Submit(submission (entity "e1") "k1" (Start 1), noCancellation)
              let! again = inbox.Submit(submission (entity "e1") "k1" (Start 1), noCancellation)

              match first, again with
              | Ok(Accepted original), Ok(AlreadySubmitted existing) ->
                  Expect.equal existing original "the repeat carries the original command's id"
              | other -> failtestf "expected Accepted then AlreadySubmitted, got %A" other

              Expect.equal (scalar db.Db "SELECT count(*) FROM fsm_command;") "1" "only one command exists"
          }

          testTask "sequence numbers are gapless per entity and independent across entities" {
              let db = fixture "inbox"
              let inbox = inboxOf db

              for index in 1..3 do
                  let! _ = submit inbox (entity "e1") $"a{index}" (Start index)
                  ()

              for index in 1..2 do
                  let! _ = submit inbox (entity "e2") $"b{index}" (Start index)
                  ()

              let sequenceOf name =
                  column
                      db.Db
                      "SELECT seq FROM fsm_command WHERE entity_id = @entity ORDER BY seq;"
                      [ "@entity", box name ]

              Expect.equal (sequenceOf "e1") [ "1"; "2"; "3" ] "the first entity numbers its own commands"
              Expect.equal (sequenceOf "e2") [ "1"; "2" ] "the second numbers independently"
          }

          testTask "only the first command of an entity is unblocked" {
              let db = fixture "inbox"
              let inbox = inboxOf db

              let! _ = submit inbox (entity "e1") "k1" (Start 1)
              let! _ = submit inbox (entity "e1") "k2" (Start 2)
              let! _ = submit inbox (entity "e1") "k3" Finish

              Expect.equal
                  (column db.Db "SELECT blocked FROM fsm_command ORDER BY seq;" [])
                  [ "0"; "1"; "1" ]
                  "the head is claimable and everything behind it waits"
          }

          testTask "a command cannot pin a version no chart declared" {
              let db = fixture "inbox"
              let inbox = inboxOf db

              let unregistered =
                  { submission (entity "e1") "k1" (Start 1) with
                      ChartVersion = ChartVersion.create 7 }

              match! inbox.Submit(unregistered, noCancellation) with
              | Error(StoreError.NotFound what) -> Expect.stringContains what "7" "the error names the missing version"
              | other -> failtestf "expected NotFound, got %A" other

              Expect.equal (scalar db.Db "SELECT count(*) FROM fsm_command;") "0" "nothing was written"
          }

          testTask "claiming one command claims exactly one" {
              let db = fixture "inbox"
              let inbox = inboxOf db

              for index in 1..10 do
                  let! _ = submit inbox (entity $"e{index}") $"k{index}" (Start index)
                  ()

              let! claimed = claim inbox 1

              Expect.hasLength claimed 1 "a batch of one leases exactly one command"

              Expect.equal
                  (scalar db.Db "SELECT count(*) FROM fsm_command WHERE status = 'ready';")
                  "9"
                  "the rest of the queue is untouched"
          }

          testTask "a batch never contains two commands for one entity" {
              let db = fixture "inbox"
              let inbox = inboxOf db

              for index in 1..4 do
                  let! _ = submit inbox (entity "e1") $"k{index}" (Start index)
                  ()

              for index in 1..3 do
                  let! _ = submit inbox (entity $"other{index}") $"o{index}" Finish
                  ()

              let! claimed = claim inbox 50
              let entities = claimed |> List.map _.Work.EntityId

              Expect.equal (List.distinct entities) entities "each entity appears at most once"
              Expect.hasLength claimed 4 "one head per entity, and there are four entities"
          }

          testTask "a command is not claimable before it is visible" {
              let db = fixture "inbox"
              let inbox = inboxOf db

              let later =
                  { submission (entity "e1") "k1" (Start 1) with
                      VisibleAt = Some(startTime.AddMinutes 5.0) }

              let! _ = inbox.Submit(later, noCancellation)
              let! early = claim inbox 10
              Expect.isEmpty early "nothing is claimable before its instant"

              db.Clock.Advance(TimeSpan.FromMinutes 5.0)
              let! due = claim inbox 10
              Expect.hasLength due 1 "and it is claimable from that instant"
          }

          testTask "commands for one entity are delivered in submission order" {
              let db = fixture "inbox"
              let inbox = inboxOf db

              let! _ = submit inbox (entity "e1") "first" (Start 1)
              let! _ = submit inbox (entity "e1") "second" Finish

              let! head = claimOne inbox
              Expect.equal head.Work.IdempotencyKey "first" "the earliest command comes first"

              let! blocked = claim inbox 10
              Expect.isEmpty blocked "nothing else for the entity is claimable while its head is leased"

              let! outcome = reject db head
              Expect.equal outcome (Finalized Epoch.initial) "the fenced terminal write succeeds"

              let! next = claimOne inbox
              Expect.equal next.Work.IdempotencyKey "second" "finishing the head releases the entity"
          }

          testTask "an expired lease is redelivered with a higher token" {
              let db = fixture "inbox"
              let inbox = inboxOf db

              let! _ = submit inbox (entity "e1") "k1" (Start 1)
              let! first = claimOne inbox

              db.Clock.Advance(TimeSpan.FromSeconds 31.0)
              let! second = claimOne inbox

              Expect.equal second.Work.CommandId first.Work.CommandId "the same command comes back"
              Expect.equal second.DeliveryCount 2 "the delivery count counts redeliveries"

              Expect.isGreaterThan
                  (LeaseToken.value second.Token)
                  (LeaseToken.value first.Token)
                  "tokens only climb, so a stale one is strictly lower"

              let! stale = reject db first
              Expect.equal stale FinalizeOutcome.LeaseLost "the reclaimed worker cannot finish the command"
          }

          testTask "a lease the clock has not reached stays claimed" {
              let db = fixture "inbox"
              let inbox = inboxOf db

              let! _ = submit inbox (entity "e1") "k1" (Start 1)
              let! _ = claimOne inbox

              db.Clock.Advance(TimeSpan.FromSeconds 29.0)
              let! again = claim inbox 10
              Expect.isEmpty again "a live lease is not redelivered"
          }

          testTask "a stale token cannot finalize, reschedule or extend" {
              let db = fixture "inbox"
              let inbox = inboxOf db

              let! _ = submit inbox (entity "e1") "k1" (Start 1)
              let! head = claimOne inbox

              let stale: LeaseToken<CommandWork> =
                  LeaseToken.ofInt64 (LeaseToken.value head.Token - 1L)

              let! finalized =
                  (processorOf db)
                      .Reject(head.Work.CommandId, stale, CommandFailure.Domain(Refused "stale"), noCancellation)

              Expect.equal (finalized |> expectOk "finalize") FinalizeOutcome.LeaseLost "finishing is fenced"

              let! rescheduled = inbox.Reschedule(head.Work.CommandId, stale, backoff, noCancellation)
              Expect.equal (rescheduled |> expectOk "reschedule") LeaseLost "rescheduling is fenced"

              let! extended = inbox.ExtendLease(head.Work.CommandId, stale, TimeSpan.FromMinutes 1.0, noCancellation)
              Expect.equal (extended |> expectOk "extend") LeaseLost "extending is fenced"

              let! record = readBack inbox head.Work.CommandId
              Expect.equal record.Status CommandStatus.Leased "the stale worker changed nothing"
              Expect.equal record.Attempts 0 "a refused reschedule counts no attempt"
          }

          testTask "rescheduling defers the command within the ceiling and counts the attempt" {
              let db = fixture "inbox"
              let inbox = inboxOf db

              let! _ = submit inbox (entity "e1") "k1" (Start 1)
              let! head = claimOne inbox

              let! outcome = inbox.Reschedule(head.Work.CommandId, head.Token, backoff, noCancellation)
              Expect.equal (outcome |> expectOk "reschedule") Updated "the holder may reschedule"

              let! record = readBack inbox head.Work.CommandId
              Expect.equal record.Status CommandStatus.Ready "a rescheduled command is ready again"
              Expect.equal record.Attempts 1 "the failed attempt is recorded"
              Expect.isFalse record.Blocked "it keeps its place at the head of its entity"

              Expect.isLessThan
                  record.VisibleAt
                  (startTime + Backoff.ceiling backoff)
                  "the jittered delay stays below the ceiling"

              db.Clock.Advance(Backoff.ceiling backoff)
              let! retried = claim inbox 10
              Expect.hasLength retried 1 "the retry is claimable once the backoff elapses"
          }

          testTask "a huge backoff does not overflow the delay" {
              let db = fixture "inbox"
              let inbox = inboxOf db

              let! _ = submit inbox (entity "e1") "k1" (Start 1)
              let huge = Backoff.create (TimeSpan.FromDays 3650.0) (TimeSpan.FromDays 3650.0)

              // Enough failed attempts that base << attempts would pass int64 many times over.
              for _ in 1..40 do
                  db.Clock.Advance(TimeSpan.FromDays 3651.0)
                  let! head = claimOne inbox
                  let! outcome = inbox.Reschedule(head.Work.CommandId, head.Token, huge, noCancellation)
                  Expect.equal (outcome |> expectOk "reschedule") Updated "every reschedule lands"

              let! record = readBack inbox (CommandId.ofInt64 1L)

              Expect.isGreaterThanOrEqual
                  record.VisibleAt
                  (db.Clock.GetUtcNow())
                  "the delay is never negative, however large the shift would have been"
          }

          testTask "extending a held lease keeps the command claimed" {
              let db = fixture "inbox"
              let inbox = inboxOf db

              let! _ = submit inbox (entity "e1") "k1" (Start 1)
              let! head = claimOne inbox

              let! outcome =
                  inbox.ExtendLease(head.Work.CommandId, head.Token, TimeSpan.FromMinutes 5.0, noCancellation)

              Expect.equal (outcome |> expectOk "extend") Updated "the holder may extend"

              db.Clock.Advance(TimeSpan.FromMinutes 4.0)
              let! none = claim inbox 10
              Expect.isEmpty none "the deadline moved out"

              let! record = readBack inbox head.Work.CommandId
              Expect.equal record.VisibleAt (startTime.AddMinutes 5.0) "to the instant asked for"
          }

          testTask "a command can be found by the key it was submitted under" {
              let db = fixture "inbox"
              let inbox = inboxOf db

              let! expectedId = submit inbox (entity "e1") "k1" (Start 7)
              let! found = inbox.TryFind(testMachine, entity "e1", "k1", noCancellation)

              match found |> expectOk "find" with
              | Some record ->
                  Expect.equal record.CommandId expectedId "the same command"
                  Expect.equal record.Event (Start 7) "the event round-trips"
                  Expect.equal record.ChartVersion version "the pinned chart version round-trips"
                  Expect.equal record.ReceivedAt startTime "arrival is stamped from the store's clock"
              | None -> failtest "the command should be found"

              let! missing = inbox.TryFind(testMachine, entity "e1", "absent", noCancellation)
              Expect.isNone (missing |> expectOk "find missing") "an unknown key finds nothing"
          }

          testTask "audit context round-trips, and absent fields stay absent" {
              let db = fixture "inbox"
              let inbox = inboxOf db

              let request =
                  { submission (entity "e1") "k1" (Start 1) with
                      Audit =
                          { AuditContext.empty with
                              Tenant = Some "acme"
                              Principal = Some "operator@example.test" } }

              let! outcome = inbox.Submit(request, noCancellation)
              let! record = readBack inbox (outcome |> expectOk "submit" |> commandIdOf)

              Expect.equal record.Audit.Tenant (Some "acme") "a supplied field comes back"
              Expect.equal record.Audit.Source None "an unsupplied field reads as absent, not as an empty string"
          }

          testTask "a correction's details round-trip through a claim" {
              let db = fixture "inbox"
              let inbox = inboxOf db
              let at = startTime.AddDays -2.0

              let correction =
                  { submission (entity "e1") "c1" (Start 3) with
                      Kind =
                          CommandKind.Correction(
                              at,
                              { CorrectionPolicy.defaults with
                                  ReplayLimit = 42 }
                          ) }

              let! _ = inbox.Submit(correction, noCancellation)
              let! head = claimOne inbox

              match head.Work.Kind with
              | CommandKind.Correction(effective, policy) ->
                  Expect.equal effective at "the instant it corrects"
                  Expect.equal policy.ReplayLimit 42 "and its policy"
              | CommandKind.Event -> failtest "the claim lost the correction's details"
          }

          // A serial test cannot prove ordering holds under contention. Many submitters race on a
          // handful of entities while several workers claim and finish, all through the write gate.
          testTask "a concurrent workload keeps every entity ordered" {
              let db = fixture "inbox"
              let inbox = inboxOf db
              let entities = [ for index in 1..8 -> $"e{index}" ]
              let perEntity = 6

              let! results =
                  [ for name in entities do
                        for index in 1..perEntity ->
                            inbox.Submit(submission (entity name) $"{name}-{index}" (Start index), noCancellation) ]
                  |> Task.WhenAll

              for result in results do
                  result |> expectOk "concurrent submit" |> ignore

              Expect.equal
                  (scalar db.Db "SELECT count(*) FROM fsm_command WHERE status IN ('ready', 'leased') AND blocked = 0;")
                  (string (List.length entities))
                  "exactly one command per entity is claimable"

              let worker () =
                  task {
                      let mutable working = true

                      while working do
                          match! claim inbox 4 with
                          | [] -> working <- false
                          | batch ->
                              for held in batch do
                                  let! _ = reject db held
                                  ()
                  }

              let! _ = Task.WhenAll [ for _ in 1..4 -> worker () ]

              Expect.equal
                  (scalar db.Db "SELECT count(*) FROM fsm_command WHERE status IN ('ready', 'leased');")
                  "0"
                  "every command reached a terminal state"

              for name in entities do
                  Expect.equal
                      (column
                          db.Db
                          "SELECT seq FROM fsm_command WHERE entity_id = @entity ORDER BY command_id;"
                          [ "@entity", box name ])
                      [ for index in 1..perEntity -> string index ]
                      $"{name} is strictly ordered"
          } ]
