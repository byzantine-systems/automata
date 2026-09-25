module ByzantineSystems.Automata.Storage.Sqlite.Tests.ActionQueueTests

open System
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Sqlite
open ByzantineSystems.Automata.Storage.Sqlite.Tests.TestContext
open Expecto

let private entity name : Entity = entityId name
let private actionLease = TimeSpan.FromSeconds 30.0
let private effectiveAt = DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)

/// Commits one command, which is what puts actions in the queue.
let private committed (fixture: Fixture) (key: string) =
    task {
        let! held = leased (inboxOf fixture) (entity "e1") key

        let! outcome =
            (processorOf fixture)
                .Commit(
                    held.Work.CommandId,
                    held.Token,
                    Epoch.initial,
                    draft held.Work.EntityId Idle (Active 1) effectiveAt,
                    noCancellation
                )

        outcome |> expectOk "commit" |> ignore
        return held
    }

let private claimActions (queue: Actions) : Threading.Tasks.Task<ClaimedAction list> =
    task {
        let! claimed = queue.Claim(testMachine, 50, actionLease, noCancellation)
        return claimed |> expectOk "claim actions"
    }

let private claimOnly (queue: Actions) =
    task {
        let! claimed = claimActions queue
        return List.exactlyOne claimed
    }

let private depth (fixture: Fixture) =
    scalar fixture.Db "SELECT count(*) FROM fsm_action;"

let private deadLetters (fixture: Fixture) =
    scalar fixture.Db "SELECT count(*) FROM fsm_action_dead_letter;"

/// A whole store whose actions are plain strings, which the default codec writes as bare JSON
/// strings. That is the shape json_each's value column would have unquoted.
let private stringStore (fixture: Fixture) =
    MachineStoreOptions.forEntityId<TestEntity, TestState, TestEvent, string, TestError> fixture.Context
    |> SqliteMachineStore

let tests =
    testList
        "IActionQueue"
        [ testTask "a claimed action carries the command and ordinal that produced it" {
              let db = fixture "actions"
              let! held = committed db "k1"

              let! (action: ClaimedAction) = claimOnly (actionsOf db)

              Expect.equal action.Work.CommandId held.Work.CommandId "the command that emitted it"
              Expect.equal action.Work.Ordinal 0 "the first action in the list"
              Expect.equal action.Work.Epoch (Epoch.ofUInt64 1UL) "the epoch the commit wrote"
              Expect.equal action.Work.EntityId (entity "e1") "the entity"
              Expect.equal action.Work.MachineId testMachine "the machine"
              Expect.equal action.Work.Action (Notify "done") "the action itself, decoded"
              Expect.equal action.DeliveryCount 1 "the first delivery"
              Expect.equal action.ExpiresAt (startTime + actionLease) "the lease runs from the store's clock"
          }

          testTask "a finalize that writes nothing queues nothing" {
              let db = fixture "actions"
              let! held = leased (inboxOf db) (entity "e1") "k1"

              let! outcome =
                  (processorOf db)
                      .Commit(
                          held.Work.CommandId,
                          LeaseToken.ofInt64 999999L,
                          Epoch.initial,
                          draft held.Work.EntityId Idle (Active 1) effectiveAt,
                          noCancellation
                      )

              Expect.equal (outcome |> expectOk "commit") FinalizeOutcome.LeaseLost "fenced out"
              Expect.equal (depth db) "0" "no effects for a transition that never happened"
          }

          testTask "a rejection queues nothing" {
              let db = fixture "actions"
              let! held = leased (inboxOf db) (entity "e1") "k1"

              let! _ =
                  (processorOf db)
                      .Reject(held.Work.CommandId, held.Token, CommandFailure.Domain(Refused "no"), noCancellation)

              Expect.equal (depth db) "0" "a refused command asked for nothing"
          }

          testTask "one machine's claim never takes another machine's actions" {
              let db = fixture "actions"
              let! _ = committed db "k1"

              let! other = (actionsOf db).Claim(machineId "other-machine", 50, actionLease, noCancellation)
              Expect.isEmpty (other |> expectOk "claim") "the queue is shared, the claim is not"
          }

          testTask "a leased action is not claimed again until its lease lapses" {
              let db = fixture "actions"
              let! _ = committed db "k1"
              let queue = actionsOf db
              let! (first: ClaimedAction) = claimOnly queue

              let! during = claimActions queue
              Expect.isEmpty during "a live lease is not redelivered"

              db.Clock.Advance(actionLease)
              let! (second: ClaimedAction) = claimOnly queue

              Expect.equal second.Work.CommandId first.Work.CommandId "the same command"
              Expect.equal second.Work.Ordinal first.Work.Ordinal "the same position"
              Expect.equal second.DeliveryCount 2 "counted as a redelivery"
              Expect.isGreaterThan (LeaseToken.value second.Token) (LeaseToken.value first.Token) "under a new token"

              let! stale = queue.Complete(first, noCancellation)
              Expect.equal (stale |> expectOk "stale complete") LeaseLost "the lapsed holder cannot complete it"
              Expect.equal (depth db) "1" "and the action is still queued"
          }

          testTask "completing removes the action" {
              let db = fixture "actions"
              let! _ = committed db "k1"
              let queue = actionsOf db
              let! (action: ClaimedAction) = claimOnly queue

              let! outcome = queue.Complete(action, noCancellation)
              Expect.equal (outcome |> expectOk "complete") Updated "delivered"
              Expect.equal (depth db) "0" "the action left the queue"

              let! again = queue.Complete(action, noCancellation)
              Expect.equal (again |> expectOk "complete again") LeaseLost "a second completion finds nothing"
          }

          testTask "a forged token cannot complete, reschedule or abandon" {
              let db = fixture "actions"
              let! _ = committed db "k1"
              let queue = actionsOf db
              let! (action: ClaimedAction) = claimOnly queue

              let forged =
                  { action with
                      Token = LeaseToken.ofInt64 (LeaseToken.value action.Token + 1L) }

              let! completed = queue.Complete(forged, noCancellation)
              let! rescheduled = queue.Reschedule(forged, Backoff.defaults, noCancellation)
              let! abandoned = queue.Abandon(forged, "gave up", noCancellation)

              Expect.equal (completed |> expectOk "complete") LeaseLost "completion is fenced"
              Expect.equal (rescheduled |> expectOk "reschedule") LeaseLost "rescheduling is fenced"
              Expect.equal (abandoned |> expectOk "abandon") LeaseLost "abandoning is fenced"
              Expect.equal (depth db) "1" "the action is untouched"
              Expect.equal (deadLetters db) "0" "and nothing was recorded"
          }

          // Stricter than the PostgreSQL store: rescheduling clears the token, so a worker that
          // rescheduled and then completes by mistake finds its lease gone.
          testTask "a stale completion after a reschedule loses" {
              let db = fixture "actions"
              let! _ = committed db "k1"
              let queue = actionsOf db
              let! (action: ClaimedAction) = claimOnly queue

              let! rescheduled = queue.Reschedule(action, Backoff.defaults, noCancellation)
              Expect.equal (rescheduled |> expectOk "reschedule") Updated "the holder may reschedule"

              let! completed = queue.Complete(action, noCancellation)
              Expect.equal (completed |> expectOk "complete") LeaseLost "the rescheduled lease is spent"
              Expect.equal (depth db) "1" "and the delivery stays queued"
          }

          testTask "a rescheduled action returns within the ceiling and keeps its identity" {
              let db = fixture "actions"
              let! _ = committed db "k1"
              let queue = actionsOf db
              let! (first: ClaimedAction) = claimOnly queue
              let backoff = Backoff.create (TimeSpan.FromSeconds 1.0) (TimeSpan.FromSeconds 10.0)

              let! _ = queue.Reschedule(first, backoff, noCancellation)
              db.Clock.Advance(Backoff.ceiling backoff)
              let! (second: ClaimedAction) = claimOnly queue

              Expect.equal second.Work.CommandId first.Work.CommandId "the same command"
              Expect.equal second.Work.Ordinal first.Work.Ordinal "the same position"
              Expect.equal second.DeliveryCount 2 "a destination deduplicates on the pair, not the count"
          }

          testTask "abandoning records the effect that never happened" {
              let db = fixture "actions"
              let! held = committed db "k1"
              let queue = actionsOf db
              let! (action: ClaimedAction) = claimOnly queue

              let! outcome = queue.Abandon(action, "the destination refused it", noCancellation)
              Expect.equal (outcome |> expectOk "abandon") Updated "given up on"
              Expect.equal (depth db) "0" "the action left the queue"

              Expect.equal
                  (column
                      db.Db
                      "SELECT command_id || '|' || ordinal || '|' || reason || '|' || deliveries
                       FROM fsm_action_dead_letter;"
                      [])
                  [ $"{CommandId.value held.Work.CommandId}|0|the destination refused it|1" ]
                  "the record carries the command, the position, the reason and the delivery count"

              Expect.equal
                  (scalar
                      db.Db
                      "SELECT (SELECT action FROM fsm_action_dead_letter) = (SELECT actions -> 0 FROM fsm_transition);")
                  "1"
                  "and the action exactly as the commit queued it"
          }

          test "an abandonment must say why" {
              let db = fixture "actions"
              let queue = actionsOf db

              let action: ClaimedAction =
                  { Work =
                      { MachineId = testMachine
                        EntityId = entity "e1"
                        CommandId = CommandId.ofInt64 1L
                        Epoch = Epoch.ofUInt64 1UL
                        Ordinal = 0
                        Action = Notify "x" }
                    Token = LeaseToken.ofInt64 1L
                    DeliveryCount = 1
                    ExpiresAt = startTime }

              Expect.throwsT<ArgumentException>
                  (fun () -> queue.Abandon(action, "  ", noCancellation) |> ignore)
                  "a blank reason is refused before anything is sent"
          }

          // json_each's value column unquotes a JSON string, so an action codec that emits one
          // would queue text that is no longer JSON, and the action_is_json CHECK would refuse
          // the whole commit. The fan-out takes each element with ->, which keeps it JSON.
          testTask "actions a codec writes as bare JSON strings survive the fan-out" {
              let db = fixture "actions"
              let store = stringStore db
              let inbox = store :> ICommandInbox<Entity, TestEvent>
              let queue = store :> IActionQueue<Entity, string>
              let! held = leased inbox (entity "e1") "k1"

              let strings =
                  { MachineId = testMachine
                    EntityId = held.Work.EntityId
                    Event = Finish
                    Actions = [ "plain"; "with \"quotes\""; "" ]
                    FromState = Idle
                    ToState = Active 1
                    HandledBy = stateId "root"
                    Exited = []
                    Entered = []
                    Status = Running
                    EffectiveAt = effectiveAt }

              let! outcome =
                  (store :> ICommandProcessorStore<Entity, TestState, TestEvent, string, TestError>)
                      .Commit(held.Work.CommandId, held.Token, Epoch.initial, strings, noCancellation)

              Expect.equal (outcome |> expectOk "commit") (Finalized(Epoch.ofUInt64 1UL)) "the commit lands"

              let! claimed = queue.Claim(testMachine, 50, actionLease, noCancellation)

              Expect.equal
                  (claimed
                   |> expectOk "claim"
                   |> List.sortBy _.Work.Ordinal
                   |> List.map _.Work.Action)
                  [ "plain"; "with \"quotes\""; "" ]
                  "each string comes back as it went in, in order"
          } ]
