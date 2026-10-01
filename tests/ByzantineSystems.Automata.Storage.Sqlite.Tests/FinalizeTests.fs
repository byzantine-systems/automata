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

          // The case the whole design exists for. A worker that lost its connection does not know
          // whether it committed; asking again must report what it already did.
          testTask "a repeat from the same lease reports the work already done" {
              let db = fixture "finalize"
              let inbox = inboxOf db
              let! held = leased inbox (entity "e1") "k1"
              let processor = processorOf db

              let! first = commit processor held Epoch.initial (Active 7)
              let! again = commit processor held Epoch.initial (Active 7)

              Expect.equal first (Finalized(Epoch.next Epoch.initial)) "the first call commits"
              Expect.equal again (AlreadyFinalized(Epoch.next Epoch.initial)) "the retry reports the same epoch"
              Expect.equal (counts db) "1/1/0/1" "and writes nothing a second time"
          }

          testTask "a repeated rejection from the same lease is already finalized" {
              let db = fixture "finalize"
              let inbox = inboxOf db
              let! held = leased inbox (entity "e1") "k1"
              let processor = processorOf db
              let failure = CommandFailure.Domain(Refused "no")

              let! first = processor.Reject(held.Work.CommandId, held.Token, failure, noCancellation)
              let! again = processor.Reject(held.Work.CommandId, held.Token, failure, noCancellation)

              Expect.equal (first |> expectOk "reject") (Finalized Epoch.initial) "the first call rejects"
              Expect.equal (again |> expectOk "again") (AlreadyFinalized Epoch.initial) "the retry is recognised"
              Expect.equal (counts db) "0/0/1/0" "one error, recorded once"
          }

          testTask "a command finished under another lease is lost to this one" {
              let db = fixture "finalize"
              let inbox = inboxOf db
              let! _ = submit inbox (entity "e1") "k1" (Start 1)
              let! stale = claimKey inbox "k1"

              db.Clock.Advance(TimeSpan.FromSeconds 31.0)
              let! holder = claimKey inbox "k1"
              let processor = processorOf db
              let! _ = commit processor holder Epoch.initial (Active 1)

              let! outcome = commit processor stale Epoch.initial (Active 7)

              Expect.equal outcome FinalizeOutcome.LeaseLost "the worker that lost the lease writes nothing"
              Expect.equal (counts db) "1/1/0/1" "only the holder's commit exists"
          }

          testTask "a reclaimed command cannot be finalized by the worker that lost it" {
              let db = fixture "finalize"
              let inbox = inboxOf db
              let! _ = submit inbox (entity "e1") "k1" (Start 1)
              let! stale = claimKey inbox "k1"

              db.Clock.Advance(TimeSpan.FromSeconds 31.0)
              let! holder = claimKey inbox "k1"

              Expect.isGreaterThan
                  (LeaseToken.value holder.Token)
                  (LeaseToken.value stale.Token)
                  "the reclaim issues a higher token"

              let! outcome = commit (processorOf db) stale Epoch.initial (Active 7)

              Expect.equal outcome FinalizeOutcome.LeaseLost "the worker that lost the lease writes nothing"
              Expect.equal (counts db) "0/0/0/0" "and nothing was written"
          }

          testTask "an expired lease nobody reclaimed may still commit" {
              let db = fixture "finalize"
              let inbox = inboxOf db
              let! held = leased inbox (entity "e1") "k1"
              db.Clock.Advance(TimeSpan.FromMinutes 5.0)

              let! outcome = commit (processorOf db) held Epoch.initial (Active 7)

              Expect.equal outcome (Finalized(Epoch.next Epoch.initial)) "the token, not the clock, is the fence"
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

          testTask "an expected epoch that does not match reports both values" {
              let db = fixture "finalize"
              let inbox = inboxOf db
              let! held = leased inbox (entity "e1") "k1"
              let ahead = Epoch.ofUInt64 5UL

              let! outcome = commit (processorOf db) held ahead (Active 7)

              Expect.equal outcome (Conflict(ahead, Epoch.initial)) "the caller learns how far off it was"
              Expect.equal (counts db) "0/0/0/0" "a conflict writes nothing"
              Expect.equal (statusOf db "k1") "leased" "and leaves the command open"
          }

          testTask "rejecting records the error, leaves the state alone and releases the entity" {
              let db = fixture "finalize"
              let inbox = inboxOf db
              let! held = leased inbox (entity "e1") "first"
              let! _ = submit inbox (entity "e1") "second" Finish

              let! outcome =
                  (processorOf db)
                      .Reject(held.Work.CommandId, held.Token, CommandFailure.Domain(Refused "no"), noCancellation)

              Expect.equal (outcome |> expectOk "reject") (Finalized Epoch.initial) "the epoch does not move"
              Expect.equal (counts db) "0/0/1/0" "an error, and nothing else"
              Expect.equal (statusOf db "first") "rejected" "the command is closed"
              Expect.equal (blockedOf db "second") "0" "the entity is released"
          }

          testTask "dead-lettering records the error and releases the entity" {
              let db = fixture "finalize"
              let inbox = inboxOf db
              let! held = leased inbox (entity "e1") "first"
              let! _ = submit inbox (entity "e1") "second" Finish

              let! outcome =
                  (processorOf db)
                      .DeadLetter(held.Work.CommandId, held.Token, CommandFailure.Machine "poison", noCancellation)

              Expect.equal (outcome |> expectOk "dead letter") (Finalized Epoch.initial) "terminal, no epoch change"
              Expect.equal (counts db) "0/0/1/0" "an error, and nothing else"
              Expect.equal (statusOf db "first") "dead_letter" "the command is closed"
              Expect.equal (blockedOf db "second") "0" "a command nobody can process does not wedge its entity"
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

          testTask "epochs are gapless across an entity's commands" {
              let db = fixture "finalize"
              let inbox = inboxOf db
              let processor = processorOf db

              for index in 1..4 do
                  let! held = leased inbox (entity "e1") $"k{index}"
                  let! _ = commit processor held (Epoch.ofUInt64 (uint64 (index - 1))) (Active index)
                  ()

              Expect.equal
                  (column db.Db "SELECT epoch FROM fsm_transition ORDER BY epoch;" [])
                  [ "1"; "2"; "3"; "4" ]
                  "no gaps, no repeats"

              Expect.equal (scalar db.Db "SELECT epoch FROM fsm_entity_snapshot;") "4" "the snapshot is the latest"
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
          }

          testTask "a command's result reads back as what happened to it" {
              let db = fixture "finalize"
              let inbox = inboxOf db
              let processor = processorOf db

              let! pendingId = submit inbox (entity "e1") "k1" (Start 1)
              let! before = processor.TryGetResult(pendingId, noCancellation)
              Expect.equal (before |> expectOk "pending") (Some CommandResult.Pending) "an open command has no result"

              let! held = claimKey inbox "k1"
              let! _ = commit processor held Epoch.initial (Active 9)
              let! after = processor.TryGetResult(held.Work.CommandId, noCancellation)

              match after |> expectOk "committed result" with
              | Some(CommandResult.Committed committed) ->
                  Expect.equal committed.Epoch (Epoch.next Epoch.initial) "the stored epoch"
                  Expect.equal committed.Draft.ToState (Active 9) "the state it moved to"
                  Expect.equal committed.Draft.Actions [ Notify "done" ] "the actions it emitted"
                  Expect.equal committed.Draft.Exited [ stateId "idle" ] "the path it left"
                  Expect.equal committed.Draft.Entered [ stateId "active" ] "the path it entered"
                  Expect.equal committed.Draft.EffectiveAt effectiveAt "business time, as supplied"
              | other -> failtestf "expected a committed result, got %A" other

              let! rejectedId = submit inbox (entity "e2") "r1" Finish
              let! heldTwo = claimKey inbox "r1"

              let! _ =
                  processor.Reject(
                      heldTwo.Work.CommandId,
                      heldTwo.Token,
                      CommandFailure.Domain(Refused "nope"),
                      noCancellation
                  )

              let! rejected = processor.TryGetResult(rejectedId, noCancellation)

              Expect.equal
                  (rejected |> expectOk "rejected result")
                  (Some(CommandResult.Rejected(CommandFailure.Domain(Refused "nope"))))
                  "a rejection reads back as the error that caused it"

              let! missing = processor.TryGetResult(CommandId.ofInt64 404L, noCancellation)
              Expect.equal (missing |> expectOk "missing") None "an unknown command has no result"
          }

          testTask "every failure envelope round-trips" {
              let db = fixture "finalize"
              let inbox = inboxOf db
              let processor = processorOf db

              let failures =
                  [ CommandFailure.Domain(Refused "domain")
                    CommandFailure.Machine "machine"
                    CommandFailure.Replay(ReplayError.Diverged(Epoch.ofUInt64 3UL, "diverged"))
                    CommandFailure.Replay ReplayError.HistoryPurged ]

              for index, failure in List.indexed failures do
                  let! commandId = submit inbox (entity $"e{index}") $"k{index}" (Start 1)
                  let! held = claimKey inbox $"k{index}"
                  let! _ = processor.DeadLetter(held.Work.CommandId, held.Token, failure, noCancellation)
                  let! result = processor.TryGetResult(commandId, noCancellation)

                  Expect.equal
                      (result |> expectOk "result")
                      (Some(CommandResult.DeadLettered failure))
                      $"{failure} reads back as itself"
          } ]
