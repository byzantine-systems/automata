module ByzantineSystems.Automata.Storage.Postgres.Tests.FinalizeTests

open System
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open Expecto
open TestContext

let private entity name : Entity = entityId name
type private Claimed = LeasedCommand<Entity, TestEvent>
let private lease = LeaseDuration.create (TimeSpan.FromSeconds 30.0)
let private effectiveAt = DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)

/// Submits one command and claims it, which is the state every test here starts from.
///
/// The claim is by batch and then filtered by key, because a claim is per machine rather than per
/// entity: any other entity with a claimable head would otherwise be returned instead, silently.
let private claimKey (inbox: Inbox) (key: string) : Task<Claimed> =
    task {
        let! claimed = inbox.Claim(machine, ClaimBatch.create 50, lease, noCancellation)

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
          } ]
