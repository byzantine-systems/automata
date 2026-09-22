module ByzantineSystems.Automata.Storage.Postgres.Tests.CommandInboxTests

open System
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open Expecto
open TestContext

let private entity name : Entity = entityId name

/// Annotated at every binding below. Leased<'kind, 'Work> is generic in the work it carries, so
/// `leased.Work.IdempotencyKey` gives inference nothing to go on and F# resolves the field
/// against whichever record declared it last.
type private Claimed = LeasedCommand<Entity, TestEvent>
let private lease = TimeSpan.FromSeconds 30.0

let private backoff =
    Backoff.create (TimeSpan.FromMilliseconds 50.0) (TimeSpan.FromSeconds 1.0)

let private submit (inbox: Inbox) entityName key event =
    task {
        let! outcome = inbox.Submit(submission (entity entityName) key event, noCancellation)
        return outcome |> expectOk $"submit {key}"
    }

let private claim (inbox: Inbox) batch : Task<Claimed list> =
    task {
        let! claimed = inbox.Claim(machine, batch, lease, noCancellation)
        return claimed |> expectOk "claim"
    }

let private claimOne (inbox: Inbox) : Task<Claimed> =
    task {
        let! (claimed: Claimed list) = claim inbox 1

        return
            match claimed with
            | [ single ] -> single
            | other -> failtestf "expected exactly one claimed command, got %i" (List.length other)
    }

let tests =
    testList
        "Postgres ICommandInbox"
        [

          testTask "a repeated idempotency key returns the original command" {
              do! reset ()
              let inbox = newInbox ()

              let! first = submit inbox "e1" "k1" (Start 1)
              let! again = submit inbox "e1" "k1" (Start 1)

              match first, again with
              | Accepted original, AlreadySubmitted existing ->
                  Expect.equal existing original "the repeat should carry the original command's id"
              | other -> failtestf "expected Accepted then AlreadySubmitted, got %A" other

              Expect.equal (scalar<int64> "SELECT count(*) FROM fsm.command") 1L "only one command should exist"
          }

          testTask "sequence numbers are gapless per entity and independent across entities" {
              do! reset ()
              let inbox = newInbox ()

              for index in 1..3 do
                  let! _ = submit inbox "e1" $"a{index}" (Start index)
                  ()

              for index in 1..2 do
                  let! _ = submit inbox "e2" $"b{index}" (Start index)
                  ()

              let sequenceOf name =
                  scalar<string>
                      $"SELECT string_agg(seq::text, ',' ORDER BY seq) FROM fsm.command WHERE entity_id = '{name}'"

              Expect.equal (sequenceOf "e1") "1,2,3" "the first entity should number its own commands"
              Expect.equal (sequenceOf "e2") "1,2" "the second entity should number independently"
          }

          testTask "only the first command of an entity is unblocked" {
              do! reset ()
              let inbox = newInbox ()

              let! _ = submit inbox "e1" "k1" (Start 1)
              let! _ = submit inbox "e1" "k2" (Start 2)
              let! _ = submit inbox "e1" "k3" Finish

              Expect.equal
                  (scalar<string> "SELECT string_agg(blocked::text, ',' ORDER BY seq) FROM fsm.command")
                  "false,true,true"
                  "the head is claimable and everything behind it waits"
          }

          // The regression that motivates the materialised CTE. Written as a plain IN (SELECT ...
          // LIMIT n) subquery, the planner rescans the candidate set per outer row, LIMIT bounds
          // each rescan rather than the statement, and this claim returns all ten.
          testTask "claiming one command claims exactly one" {
              do! reset ()
              let inbox = newInbox ()

              for index in 1..10 do
                  let! _ = submit inbox $"e{index}" $"k{index}" (Start index)
                  ()

              let! (claimed: Claimed list) = claim inbox 1

              Expect.hasLength claimed 1 "a batch of one must lease exactly one command"

              Expect.equal
                  (scalar<int64> "SELECT count(*) FROM fsm.command WHERE status = 'ready'")
                  9L
                  "the rest of the queue must be untouched"
          }

          testTask "a batch never contains two commands for one entity" {
              do! reset ()
              let inbox = newInbox ()

              for index in 1..4 do
                  let! _ = submit inbox "e1" $"k{index}" (Start index)
                  ()

              for index in 1..3 do
                  let! _ = submit inbox $"other{index}" $"o{index}" Finish
                  ()

              let! (claimed: Claimed list) = claim inbox 50
              let entities = claimed |> List.map (fun leased -> leased.Work.EntityId)

              Expect.equal
                  (List.distinct entities |> List.length)
                  (List.length entities)
                  "each entity appears at most once"

              Expect.hasLength claimed 4 "one head per entity, and there are four entities"
          }

          testTask "commands for one entity are delivered in submission order" {
              do! reset ()
              let inbox = newInbox ()

              let! _ = submit inbox "e1" "first" (Start 1)
              let! _ = submit inbox "e1" "second" Finish

              let! (head: Claimed) = claimOne inbox
              Expect.equal head.Work.IdempotencyKey "first" "the earliest command comes first"

              let! (blocked: Claimed list) = claim inbox 10
              Expect.isEmpty blocked "nothing else for the entity is claimable while its head is leased"

              let! outcome =
                  inbox.Acknowledge(head.Work.CommandId, head.Token, TerminalStatus.Succeeded, noCancellation)

              Expect.equal (outcome |> expectOk "ack") Updated "the fenced acknowledgement should succeed"

              let! (next: Claimed) = claimOne inbox
              Expect.equal next.Work.IdempotencyKey "second" "finishing the head releases the entity"
          }

          testTask "dead-lettering releases the entity rather than wedging it" {
              do! reset ()
              let inbox = newInbox ()

              let! _ = submit inbox "e1" "poison" (Start 1)
              let! _ = submit inbox "e1" "healthy" Finish

              let! (head: Claimed) = claimOne inbox

              let! outcome =
                  inbox.Acknowledge(head.Work.CommandId, head.Token, TerminalStatus.DeadLettered, noCancellation)

              Expect.equal (outcome |> expectOk "dead letter") Updated "dead-lettering is a terminal acknowledgement"

              let! (next: Claimed) = claimOne inbox
              Expect.equal next.Work.IdempotencyKey "healthy" "the entity keeps moving after a command is abandoned"
          }

          testTask "an expired lease is redelivered with a higher token" {
              do! reset ()
              let inbox = newInbox ()

              let! _ = submit inbox "e1" "k1" (Start 1)

              let! first = inbox.Claim(machine, 1, TimeSpan.FromMilliseconds 1.0, noCancellation)

              let first: Claimed =
                  match first |> expectOk "short claim" with
                  | [ single ] -> single
                  | other -> failtestf "expected one command, got %i" (List.length other)

              do! Task.Delay(TimeSpan.FromMilliseconds 50.0)

              let! (second: Claimed) = claimOne inbox

              Expect.equal second.Work.CommandId first.Work.CommandId "the same command comes back"
              Expect.equal second.DeliveryCount 2 "the delivery count counts redeliveries"

              Expect.isGreaterThan
                  (LeaseToken.value second.Token)
                  (LeaseToken.value first.Token)
                  "tokens are monotone, so a stale one is strictly lower"

              let! stale =
                  inbox.Acknowledge(first.Work.CommandId, first.Token, TerminalStatus.Succeeded, noCancellation)

              Expect.equal (stale |> expectOk "stale ack") LeaseLost "the reclaimed worker cannot finish the command"
          }

          testTask "a stale token cannot acknowledge, reschedule or extend" {
              do! reset ()
              let inbox = newInbox ()

              let! _ = submit inbox "e1" "k1" (Start 1)
              let! (head: Claimed) = claimOne inbox

              let stale: LeaseToken<CommandWork> =
                  LeaseToken.ofInt64 (LeaseToken.value head.Token - 1L)

              let! acked = inbox.Acknowledge(head.Work.CommandId, stale, TerminalStatus.Succeeded, noCancellation)
              Expect.equal (acked |> expectOk "ack") LeaseLost "acknowledging is fenced"

              let! rescheduled = inbox.Reschedule(head.Work.CommandId, stale, backoff, noCancellation)
              Expect.equal (rescheduled |> expectOk "reschedule") LeaseLost "rescheduling is fenced"

              let! extended = inbox.ExtendLease(head.Work.CommandId, stale, lease, noCancellation)
              Expect.equal (extended |> expectOk "extend") LeaseLost "extending is fenced"

              let! current = inbox.TryGet(head.Work.CommandId, noCancellation)

              match current |> expectOk "read back" with
              | Some record ->
                  Expect.equal record.Status CommandStatus.Leased "the command is untouched by the stale worker"
                  Expect.equal record.Attempts 0 "a rejected reschedule must not count an attempt"
              | None -> failtest "the command should still exist"
          }

          testTask "rescheduling defers the command and counts the attempt" {
              do! reset ()
              let inbox = newInbox ()

              let! _ = submit inbox "e1" "k1" (Start 1)
              let! (head: Claimed) = claimOne inbox

              let! outcome = inbox.Reschedule(head.Work.CommandId, head.Token, backoff, noCancellation)
              Expect.equal (outcome |> expectOk "reschedule") Updated "the holder may reschedule"

              let! (empty: Claimed list) = claim inbox 10
              Expect.isEmpty empty "the command is not claimable until its delay elapses"

              let! current = inbox.TryGet(head.Work.CommandId, noCancellation)

              match current |> expectOk "read back" with
              | Some record ->
                  Expect.equal record.Status CommandStatus.Ready "a rescheduled command is ready again"
                  Expect.equal record.Attempts 1 "the failed attempt is recorded"
                  Expect.isFalse record.Blocked "it keeps its place at the head of the entity"
              | None -> failtest "the command should still exist"

              // The envelope caps the delay, so the retry is claimable again shortly.
              do! Task.Delay(TimeSpan.FromMilliseconds 1200.0)
              let! (retried: Claimed list) = claim inbox 10
              Expect.hasLength retried 1 "the retry becomes claimable once the backoff elapses"
          }

          testTask "extending a held lease keeps the command claimed" {
              do! reset ()
              let inbox = newInbox ()

              let! _ = submit inbox "e1" "k1" (Start 1)
              let! (head: Claimed) = claimOne inbox

              let! outcome =
                  inbox.ExtendLease(head.Work.CommandId, head.Token, TimeSpan.FromMinutes 5.0, noCancellation)

              Expect.equal (outcome |> expectOk "extend") Updated "the holder may extend"

              Expect.equal
                  (scalar<int64> "SELECT count(*) FROM fsm.command WHERE visible_at > now() + interval '4 minutes'")
                  1L
                  "the deadline moved out"
          }

          testTask "a command can be found by the key it was submitted under" {
              do! reset ()
              let inbox = newInbox ()

              let! outcome = submit inbox "e1" "k1" (Start 7)
              let expectedId = commandIdOf outcome

              let! found = inbox.TryFind(machine, entity "e1", "k1", noCancellation)

              match found |> expectOk "find" with
              | Some record ->
                  Expect.equal record.CommandId expectedId "the same command"
                  Expect.equal record.Event (Start 7) "the event round-trips through jsonb"
                  Expect.equal record.ChartVersion version "the pinned chart version round-trips"
              | None -> failtest "the command should be found"

              let! missing = inbox.TryFind(machine, entity "e1", "absent", noCancellation)
              Expect.isNone (missing |> expectOk "find missing") "an unknown key finds nothing"
          }

          testTask "audit context round-trips, and absent fields stay absent" {
              do! reset ()
              let inbox = newInbox ()

              let audit =
                  { AuditContext.empty with
                      Tenant = Some "acme"
                      Principal = Some "operator@example.test" }

              let request =
                  { submission (entity "e1") "k1" (Start 1) with
                      Audit = audit }

              let! outcome = inbox.Submit(request, noCancellation)
              let commandId = commandIdOf (outcome |> expectOk "submit")
              let! found = inbox.TryGet(commandId, noCancellation)

              match found |> expectOk "read back" with
              | Some record ->
                  Expect.equal record.Audit.Tenant (Some "acme") "a supplied field comes back"
                  Expect.equal record.Audit.Source None "an unsupplied field reads as absent, not as an empty string"
              | None -> failtest "the command should exist"
          }

          // Concurrency is the whole point of this table, and a serial test cannot prove it.
          // Many submitters race on a handful of entities while many workers claim and finish.
          testTask "a concurrent workload keeps every entity ordered and leaves no drift" {
              do! reset ()
              let inbox = newInbox ()
              let entities = [ for index in 1..8 -> $"e{index}" ]
              let perEntity = 6

              let submissions =
                  [ for name in entities do
                        for index in 1..perEntity ->
                            inbox.Submit(submission (entity name) $"{name}-{index}" (Start index), noCancellation) ]

              let! results = Task.WhenAll submissions

              for result in results do
                  result |> expectOk "concurrent submit" |> ignore

              // Checked here rather than only at the end: once every command is terminal there
              // are no open commands left for the drift query to disagree about, so the
              // assertion would be vacuous. This is the moment the backlog is deepest.
              Expect.isEmpty (blockedDriftRows ()) "concurrent submission must not corrupt the derived blocked column"

              Expect.equal
                  (scalar<int64> "SELECT count(*) FROM fsm.command WHERE status IN ('ready', 'leased') AND NOT blocked")
                  (int64 (List.length entities))
                  "exactly one command per entity is claimable"

              let worker () =
                  task {
                      let mutable working = true

                      while working do
                          let! claimed = inbox.Claim(machine, 4, lease, noCancellation)

                          match claimed |> expectOk "worker claim" with
                          | [] -> working <- false
                          | batch ->
                              for leased in batch do
                                  let! outcome =
                                      inbox.Acknowledge(
                                          leased.Work.CommandId,
                                          leased.Token,
                                          TerminalStatus.Succeeded,
                                          noCancellation
                                      )

                                  outcome |> expectOk "worker ack" |> ignore
                  }

              let! _ = Task.WhenAll [ for _ in 1..4 -> worker () ]

              Expect.equal
                  (scalar<int64> "SELECT count(*) FROM fsm.command WHERE status <> 'succeeded'")
                  0L
                  "every command should have been processed exactly once"

              // Per entity, the order commands reached a terminal state must be their submission
              // order. command_id is assigned at submission and seq is derived from it, so a
              // violation shows up as a sequence that is not 1..n in order.
              for name in entities do
                  let observed =
                      scalar<string>
                          $"SELECT string_agg(seq::text, ',' ORDER BY command_id) FROM fsm.command WHERE entity_id = '{name}'"

                  let expected = [ 1..perEntity ] |> List.map string |> String.concat ","
                  Expect.equal observed expected $"{name} should be strictly ordered"
          } ]
