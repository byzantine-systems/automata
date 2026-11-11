module ByzantineSystems.Automata.Storage.Sqlite.Tests.FinalizeTests

open System
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Sqlite.Tests.TestContext
open Expecto
open Microsoft.Data.Sqlite

let private entity name : Entity = entityId name
let private effectiveAt = DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)

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

/// Transitions, snapshots, errors and queued actions, which together are everything a finalize
/// can write besides the command row.
let private counts (fixture: Fixture) =
    scalar
        fixture.Db
        "SELECT (SELECT count(*) FROM fsm_transition) || '/' ||
                (SELECT count(*) FROM fsm_entity_snapshot) || '/' ||
                (SELECT count(*) FROM fsm_command_error) || '/' ||
                (SELECT count(*) FROM fsm_action);"

let private statusOf (fixture: Fixture) (key: string) =
    column fixture.Db "SELECT status FROM fsm_command WHERE idempotency_key = @key;" [ "@key", box key ]
    |> List.exactlyOne

let private blockedOf (fixture: Fixture) (key: string) =
    column fixture.Db "SELECT blocked FROM fsm_command WHERE idempotency_key = @key;" [ "@key", box key ]
    |> List.exactlyOne

let tests =
    testList
        "finalize"
        [ testTask "committing appends the transition, moves the snapshot, queues the actions and closes the command" {
              let db = fixture "finalize"
              let inbox = inboxOf db
              let! held = leased inbox (entity "e1") "first"
              let! _ = submit inbox (entity "e1") "second" Finish

              let! outcome = commit (processorOf db) held Epoch.initial (Active 7)

              Expect.equal outcome (Finalized(Epoch.next Epoch.initial)) "the epoch advances by one"
              Expect.equal (counts db) "1/1/0/1" "a transition, a snapshot, no error, one queued action"
              Expect.equal (statusOf db "first") "succeeded" "the command is closed"
              Expect.equal (blockedOf db "second") "0" "and the entity's next command is released"
          }

          testTask "the commit's timestamps come from the store's clock" {
              let db = fixture "finalize"
              let inbox = inboxOf db
              let! held = leased inbox (entity "e1") "k1"
              db.Clock.Advance(TimeSpan.FromMinutes 3.0)

              let! _ = commit (processorOf db) held Epoch.initial (Active 7)

              let committedAt =
                  scalar db.Db "SELECT committed_at FROM fsm_transition;"
                  |> int64
                  |> ByzantineSystems.Automata.Storage.Sqlite.Instant.toDateTimeOffset

              Expect.equal committedAt (startTime.AddMinutes 3.0) "committed_at is the instant the commit ran"

              Expect.equal
                  (scalar
                      db.Db
                      "SELECT (SELECT committed_at FROM fsm_transition) = (SELECT committed_at FROM fsm_entity_snapshot);")
                  "1"
                  "and one instant stamps the whole transaction"
          }

          testTask "finalizing a command that does not exist is not found" {
              let db = fixture "finalize"

              let! outcome =
                  (processorOf db)
                      .Reject(
                          CommandId.ofInt64 404L,
                          LeaseToken.ofInt64 1L,
                          CommandFailure.Domain(Refused "no"),
                          noCancellation
                      )

              match outcome with
              | Error(StoreError.NotFound what) -> Expect.stringContains what "404" "the error names the command"
              | other -> failtestf "expected NotFound, got %A" other
          }

          // All of it or none of it. The actions are queued last, so failing that write proves the
          // transition and the snapshot before it roll back with it.
          testTask "a failure in the last write rolls the whole commit back" {
              let db = fixture "finalize"
              let inbox = inboxOf db
              let! held = leased inbox (entity "e1") "k1"

              exec
                  db.Db
                  "CREATE TRIGGER test_refuse_actions BEFORE INSERT ON fsm_action
                   BEGIN SELECT RAISE(ABORT, 'refused by the test'); END;"
                  []

              let! outcome =
                  (processorOf db)
                      .Commit(
                          held.Work.CommandId,
                          held.Token,
                          Epoch.initial,
                          draft held.Work.EntityId Idle (Active 7) effectiveAt,
                          noCancellation
                      )

              match outcome with
              | Error(StoreError.Unexpected(:? SqliteException)) -> ()
              | other -> failtestf "the refused write should surface as the defect it is, got %A" other

              Expect.equal (counts db) "0/0/0/0" "no transition, snapshot or action survives"
              Expect.equal (statusOf db "k1") "leased" "the command is still open, so it is redelivered"
          }

          testTask "one command can never produce two transitions" {
              let db = fixture "finalize"
              let inbox = inboxOf db
              let! held = leased inbox (entity "e1") "k1"
              let! _ = commit (processorOf db) held Epoch.initial (Active 7)

              let code =
                  extendedError (fun () ->
                      exec
                          db.Db
                          "INSERT INTO fsm_transition
                           SELECT machine_id, entity_id, epoch + 1, command_id, chart_version, event, actions,
                                  from_state, to_state, handled_by, exited, entered, status, effective_at, committed_at
                           FROM fsm_transition;"
                          [])

              Expect.equal code Code.constraintUnique "the schema, not the code, is what refuses it"
          } ]
