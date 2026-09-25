module ByzantineSystems.Automata.Storage.Postgres.Tests.FinalizeTests

open System
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open Expecto
open TestContext

let private entity name : Entity = entityId name
type private Claimed = LeasedCommand<Entity, TestEvent>
let private lease = TimeSpan.FromSeconds 30.0
let private effectiveAt = DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)

/// Submits one command and claims it, which is the state every test here starts from.
///
/// The claim is by batch and then filtered by key, because a claim is per machine rather than per
/// entity: any other entity with a claimable head would otherwise be returned instead, silently.
let private claimKey (inbox: Inbox) (key: string) : Task<Claimed> =
    task {
        let! claimed = inbox.Claim(machine, 50, lease, noCancellation)

        return
            claimed
            |> expectOk "claim"
            |> List.tryFind (fun (held: Claimed) -> held.Work.IdempotencyKey = key)
            |> Option.defaultWith (fun () -> failtestf "the command %s was not claimable" key)
    }

let private leased (inbox: Inbox) (name: string) (key: string) : Task<Claimed> =
    task {
        let! _ = inbox.Submit(submission (entity name) key (Start 1), noCancellation)
        return! claimKey inbox key
    }

let private commit (processor: Processor) (held: Claimed) (expected: Epoch) (toState: TestState) =
    task {
        let! outcome =
            processor.Commit(
                held.Work.CommandId,
                held.Token,
                expected,
                draft held.Work.EntityId Idle toState effectiveAt,
                noCancellation
            )

        return outcome |> expectOk "commit"
    }

let private counts () =
    scalar<string>
        "SELECT (SELECT count(*) FROM fsm.transition) || '/' ||
                (SELECT count(*) FROM fsm.instance_state) || '/' ||
                (SELECT count(*) FROM fsm.command_error)"

let tests =
    testList
        "Postgres finalize"
        [

          testTask "committing appends the transition, advances the belief and closes the command" {
              do! reset ()
              let inbox = newInbox ()
              let! held = leased inbox "e1" "first"
              let! _ = inbox.Submit(submission (entity "e1") "second" Finish, noCancellation)

              let! outcome = commit (newProcessor ()) held Epoch.initial (Active 7)

              Expect.equal outcome (Finalized(Epoch.next Epoch.initial)) "the epoch advances by one"
              Expect.equal (counts ()) "1/1/0" "one transition, one belief, no error"

              Expect.equal
                  (scalar<string> "SELECT status FROM fsm.command WHERE idempotency_key = 'first'")
                  "succeeded"
                  "the command is closed"

              Expect.equal
                  (scalar<bool> "SELECT NOT blocked FROM fsm.command WHERE idempotency_key = 'second'")
                  true
                  "and the entity's next command is released"
          }

          // The case the whole routine exists for. A worker whose connection dropped does not know
          // whether it committed; asking again must report what it already did, not a lost lease.
          testTask "a repeat from the same lease reports the work already done" {
              do! reset ()
              let inbox = newInbox ()
              let! held = leased inbox "e1" "k1"
              let processor = newProcessor ()

              let! first = commit processor held Epoch.initial (Active 7)
              let! again = commit processor held Epoch.initial (Active 7)

              Expect.equal first (Finalized(Epoch.next Epoch.initial)) "the first call commits"
              Expect.equal again (AlreadyFinalized(Epoch.next Epoch.initial)) "the retry reports the same epoch"
              Expect.equal (counts ()) "1/1/0" "and writes nothing a second time"
          }

          testTask "a reclaimed command cannot be finalized by the worker that lost it" {
              do! reset ()
              let inbox = newInbox ()
              let! _ = inbox.Submit(submission (entity "e1") "k1" (Start 1), noCancellation)

              let! first = inbox.Claim(machine, 1, TimeSpan.FromMilliseconds 1.0, noCancellation)
              let stale = (first |> expectOk "first claim") |> List.exactlyOne
              do! Task.Delay 60
              let! second = inbox.Claim(machine, 1, lease, noCancellation)
              let holder = (second |> expectOk "reclaim") |> List.exactlyOne

              Expect.isGreaterThan
                  (LeaseToken.value holder.Token)
                  (LeaseToken.value stale.Token)
                  "the reclaim issues a higher token"

              let! outcome = commit (newProcessor ()) stale Epoch.initial (Active 7)

              Expect.equal outcome FinalizeOutcome.LeaseLost "the worker that lost the lease writes nothing"
              Expect.equal (counts ()) "0/0/0" "and nothing was written"
          }

          testTask "a stale token against a still-leased command loses" {
              do! reset ()
              let inbox = newInbox ()
              let! held = leased inbox "e1" "k1"

              let forged =
                  { held with
                      Token = LeaseToken.ofInt64 (LeaseToken.value held.Token - 1L) }

              let! outcome = commit (newProcessor ()) forged Epoch.initial (Active 7)

              Expect.equal outcome FinalizeOutcome.LeaseLost "the token is the fence"
              Expect.equal (counts ()) "0/0/0" "and nothing was written"
          }

          testTask "an expected epoch that does not match reports both values" {
              do! reset ()
              let inbox = newInbox ()
              let! held = leased inbox "e1" "k1"
              let ahead = Epoch.ofUInt64 5UL

              let! outcome = commit (newProcessor ()) held ahead (Active 7)

              Expect.equal outcome (Conflict(ahead, Epoch.initial)) "the caller learns how far behind it was"
              Expect.equal (counts ()) "0/0/0" "a conflict writes nothing"
          }

          testTask "rejecting records the error and leaves the state alone" {
              do! reset ()
              let inbox = newInbox ()
              let! held = leased inbox "e1" "first"
              let! _ = inbox.Submit(submission (entity "e1") "second" Finish, noCancellation)

              let! outcome =
                  (newProcessor ())
                      .Reject(held.Work.CommandId, held.Token, CommandFailure.Domain(Refused "no"), noCancellation)

              Expect.equal (outcome |> expectOk "reject") (Finalized Epoch.initial) "the epoch does not move"
              Expect.equal (counts ()) "0/0/1" "an error, and no transition or belief"

              Expect.equal
                  (scalar<bool> "SELECT NOT blocked FROM fsm.command WHERE idempotency_key = 'second'")
                  true
                  "the entity is released"
          }

          testTask "dead-lettering records the error and releases the entity" {
              do! reset ()
              let inbox = newInbox ()
              let! held = leased inbox "e1" "first"
              let! _ = inbox.Submit(submission (entity "e1") "second" Finish, noCancellation)

              let! outcome =
                  (newProcessor ())
                      .DeadLetter(
                          held.Work.CommandId,
                          held.Token,
                          CommandFailure.Domain(Refused "poison"),
                          noCancellation
                      )

              Expect.equal (outcome |> expectOk "dead letter") (Finalized Epoch.initial) "terminal, no epoch change"
              Expect.equal (counts ()) "0/0/1" "an error, and no transition or belief"

              Expect.equal
                  (scalar<bool> "SELECT NOT blocked FROM fsm.command WHERE idempotency_key = 'second'")
                  true
                  "a command nobody can process must not wedge its entity"
          }

          // All of it or none of it. The transition is inserted before the belief moves, so a
          // belief write that raises must take the transition with it.
          testTask "a failure after the transition insert rolls the whole finalize back" {
              do! reset ()
              let inbox = newInbox ()
              let! first = leased inbox "e1" "k1"
              let processor = newProcessor ()
              let! _ = commit processor first Epoch.initial (Active 1)

              let! _ = inbox.Submit(submission (entity "e1") "k2" Finish, noCancellation)
              let! second = claimKey inbox "k2"

              // An effective instant before the live belief's lower bound, which close_and_open
              // refuses. By then the transition row has already been inserted.
              let! outcome =
                  processor.Commit(
                      second.Work.CommandId,
                      second.Token,
                      Epoch.next Epoch.initial,
                      draft second.Work.EntityId (Active 1) (Active 2) (effectiveAt.AddDays -1.0),
                      noCancellation
                  )

              match outcome with
              | Ok result -> failtestf "the belief write should have raised, got %A" result
              | Error _ -> ()

              Expect.equal (counts ()) "1/1/0" "only the first command's writes survive"

              Expect.equal
                  (scalar<string> "SELECT status FROM fsm.command WHERE idempotency_key = 'k2'")
                  "leased"
                  "the command is still open, so it will be redelivered"
          }

          testTask "epochs are gapless across an entity's commands" {
              do! reset ()
              let inbox = newInbox ()
              let processor = newProcessor ()

              for index in 1..4 do
                  let! held = leased inbox "e1" $"k{index}"
                  let! _ = commit processor held (Epoch.ofUInt64 (uint64 (index - 1))) (Active index)
                  ()

              Expect.equal
                  (scalar<string> "SELECT string_agg(epoch::text, ',' ORDER BY epoch) FROM fsm.transition")
                  "1,2,3,4"
                  "no gaps, no repeats"
          }

          testTask "one command can never produce two transitions" {
              do! reset ()
              let inbox = newInbox ()
              let! held = leased inbox "e1" "k1"
              let! _ = commit (newProcessor ()) held Epoch.initial (Active 7)

              // Independently of what the routine does: the constraint is what makes idempotence
              // a property of the schema rather than of the code above it.
              match
                  exec
                      "INSERT INTO fsm.transition (machine_id, entity_id, epoch, command_id, chart_version, event,
                                                   actions, from_state, to_state, handled_by, exited, entered,
                                                   status, effective_at)
                       SELECT machine_id, entity_id, epoch + 1, command_id, chart_version, event, actions,
                              from_state, to_state, handled_by, exited, entered, status, effective_at
                       FROM fsm.transition"
              with
              | Ok() -> failtest "a second transition for one command should be impossible"
              | Error error ->
                  Expect.stringContains error.Message "transition_unique_command" "the constraint refuses it"
          }

          // The application's clock decides when an event was effective, and only the database
          // decides the order history is written in.
          testTask "a skewed client clock cannot reorder committed history" {
              do! reset ()
              let inbox = newInbox ()
              let processor = newProcessor ()
              let! first = leased inbox "e1" "k1"

              let! _ =
                  processor.Commit(
                      first.Work.CommandId,
                      first.Token,
                      Epoch.initial,
                      draft first.Work.EntityId Idle (Active 1) effectiveAt,
                      noCancellation
                  )

              let! _ = inbox.Submit(submission (entity "e2") "k2" (Start 2), noCancellation)
              let! second = claimKey inbox "k2"

              // A wildly back-dated business instant on the second command.
              let! _ =
                  processor.Commit(
                      second.Work.CommandId,
                      second.Token,
                      Epoch.initial,
                      draft second.Work.EntityId Idle (Active 2) (effectiveAt.AddYears -5),
                      noCancellation
                  )

              Expect.equal
                  (scalar<string> "SELECT string_agg(entity_id, ',' ORDER BY committed_at, epoch) FROM fsm.transition")
                  "e1,e2"
                  "commit order follows the database clock, not the caller's"

              Expect.equal
                  (scalar<bool> "SELECT min(effective_at) < min(committed_at) FROM fsm.transition")
                  true
                  "business time and commit time are independent"
          }

          testTask "a command's result reads back as what happened to it" {
              do! reset ()
              let inbox = newInbox ()
              let processor = newProcessor ()

              let! pending = inbox.Submit(submission (entity "e1") "k1" (Start 1), noCancellation)
              let pendingId = pending |> expectOk "submit" |> commandIdOf

              let! before = processor.TryGetResult(pendingId, noCancellation)

              Expect.equal
                  (before |> expectOk "pending result")
                  (Some CommandResult.Pending)
                  "an unfinished command has no result yet"

              let! held = claimKey inbox "k1"
              let! _ = commit processor held Epoch.initial (Active 9)
              let! after = processor.TryGetResult(held.Work.CommandId, noCancellation)

              match after |> expectOk "committed result" with
              | Some(CommandResult.Committed committed) ->
                  Expect.equal committed.Epoch (Epoch.next Epoch.initial) "the stored epoch"
                  Expect.equal committed.Draft.ToState (Active 9) "the state it moved to"
                  Expect.equal committed.Draft.Actions [ Notify "done" ] "and the actions it emitted"
              | other -> failtestf "expected a committed result, got %A" other

              let! rejected = inbox.Submit(submission (entity "e2") "r1" Finish, noCancellation)
              let rejectedId = rejected |> expectOk "submit" |> commandIdOf
              let! heldTwo = claimKey inbox "r1"

              let! _ =
                  processor.Reject(
                      heldTwo.Work.CommandId,
                      heldTwo.Token,
                      CommandFailure.Domain(Refused "nope"),
                      noCancellation
                  )

              let! result = processor.TryGetResult(rejectedId, noCancellation)

              Expect.equal
                  (result |> expectOk "rejected result")
                  (Some(CommandResult.Rejected(CommandFailure.Domain(Refused "nope"))))
                  "a rejection reads back as the error that caused it"
          } ]
