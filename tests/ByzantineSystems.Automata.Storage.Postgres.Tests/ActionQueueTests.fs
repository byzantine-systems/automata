module ByzantineSystems.Automata.Storage.Postgres.Tests.ActionQueueTests

open System
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Postgres
open Expecto
open TestContext

let private entity name : Entity = entityId name
type private Claimed = LeasedCommand<Entity, TestEvent>
let private lease = LeaseDuration.create (TimeSpan.FromSeconds 30.0)
let private actionLease = LeaseDuration.create (TimeSpan.FromSeconds 30.0)
let private effectiveAt = DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)

let private leased (inbox: Inbox) (name: string) (key: string) : Task<Claimed> =
    task {
        let! _ = inbox.Submit(submission (entity name) key (Start 1), noCancellation)
        let! claimed = inbox.Claim(machine, ClaimBatch.create 50, lease, noCancellation)

        return
            claimed
            |> expectOk "claim"
            |> List.tryFind (fun (held: Claimed) -> held.Work.IdempotencyKey = key)
            |> Option.defaultWith (fun () -> failtestf "the command %s was not claimable" key)
    }

/// Commits one command, which is what puts actions in the queue.
let private commit (processor: Processor) (held: Claimed) =
    task {
        let! outcome =
            processor.Commit(
                held.Work.CommandId,
                held.Token,
                Epoch.initial,
                draft held.Work.EntityId Idle (Active 1) effectiveAt,
                noCancellation
            )

        return outcome |> expectOk "commit"
    }

let private claimActions (queue: Actions) =
    task {
        let! claimed = queue.Claim(machine, ClaimBatch.create 50, actionLease, noCancellation)
        return claimed |> expectOk "claim actions"
    }

let private queueDepth () =
    scalar<int64> $"SELECT count(*) FROM pgmq.q_{actionQueue}"

let private deadLetterCount () =
    scalar<int64> "SELECT count(*) FROM fsm.action_dead_letter"

let tests =
    testList
        "Postgres action queue"
        [ ContractProperties.poisonBatchProperty
              "poison actions do not wedge a batch"
              12
              (CommandInboxTests.poisonFixture true)

          testTask "a commit routes actions to the boot registration" {
              do! reset ()

              exec
                  "SELECT fsm.ensure_action_queue('registered_action_target'); SELECT pgmq.purge_queue('registered_action_target'); INSERT INTO fsm.machine_maintenance(machine_id, action_queue) VALUES ('pg-tests', 'registered_action_target') ON CONFLICT(machine_id) DO UPDATE SET action_queue = EXCLUDED.action_queue"
              |> expectOk "register A"

              let! held = leased (newInbox ()) "routing" "key"
              // This store's delivery configuration is the fixture's normal queue B.
              let processor = newMachineStore RetentionPolicy.keepEverything :> Processor
              let! _ = commit processor held

              Expect.equal
                  (scalar<int64> "SELECT count(*) FROM pgmq.q_registered_action_target")
                  1L
                  "registration A is authoritative"

              Expect.equal (queueDepth ()) 0L "client configuration B cannot redirect the commit"
          }

          testTask "a commit with actions requires a delivery registration" {
              do! reset ()
              exec "DELETE FROM fsm.machine_maintenance" |> expectOk "unregister"
              let! held = leased (newInbox ()) "unregistered" "key"

              let! outcome =
                  (newProcessor ())
                      .Commit(
                          held.Work.CommandId,
                          held.Token,
                          Epoch.initial,
                          draft held.Work.EntityId Idle (Active 1) effectiveAt,
                          noCancellation
                      )

              match outcome with
              | Error(StoreError.Unexpected(:? Npgsql.PostgresException as error)) ->
                  Expect.equal error.SqlState "22023" "a configuration refusal"
                  Expect.stringContains error.MessageText "action delivery is not registered" "a named reason"
              | other -> failtestf "expected an unregistered delivery refusal, got %A" other

              Expect.equal
                  (scalar<int64> "SELECT count(*) FROM fsm.transition")
                  0L
                  "the transition rolls back with routing"

              Expect.equal (queueDepth ()) 0L "nothing was delivered"
          }

          testTask "explicit Discard records actions without queueing or maintaining a delivery queue" {
              do! reset ()

              let store =
                  MachineStoreOptions.forEntityId<TestEntity, TestState, TestEvent, TestAction, TestError>
                      (context ())
                      ActionDelivery.Discard
                  |> PostgresMachineStore

              let! booted = (store :> IStoreBoot).Boot(machine, noCancellation)
              Expect.equal (booted |> expectOk "boot") BootReport.Ready "discard is a valid boot policy"
              let! held = leased (store :> Inbox) "discard" "key"
              let! _ = commit (store :> Processor) held
              let! result = (store :> Processor).TryGetResult(held.Work.CommandId, noCancellation)

              match result |> expectOk "result" with
              | Some(CommandResult.Committed transition) ->
                  Expect.equal transition.Draft.Actions [ Notify "done" ] "actions remain in the transition"
              | other -> failtestf "expected a committed transition, got %A" other

              Expect.equal (queueDepth ()) 0L "there is no delivery"
              let! actions = (store :> Actions).Claim(machine, ClaimBatch.create 10, actionLease, noCancellation)
              Expect.equal (actions |> expectOk "claim") [] "the discard consumer needs no physical queue"

              exec "SELECT * FROM fsm.run_maintenance(100); SELECT * FROM fsm.notify_pending();"
              |> expectOk "discard maintenance"
          }

          testTask "a commit enqueues its actions in the same transaction" {
              do! reset ()
              let inbox = newInbox ()
              let! held = leased inbox "e1" "k1"

              Expect.equal 0L (queueDepth ()) "nothing is queued before the commit"

              let! outcome = commit (newProcessor ()) held
              Expect.equal (Finalized(Epoch.ofUInt64 1UL)) outcome "the commit landed"

              // The draft carries one action, so the queue carries one message. The point is not
              // the count: it is that it appeared with the commit and not by a later write.
              Expect.equal 1L (queueDepth ()) "the action is queued"
          }

          testTask "a finalize that writes nothing queues nothing" {
              do! reset ()
              let inbox = newInbox ()
              let! held = leased inbox "e1" "k1"

              // A stale token means this worker lost the command, so nothing at all is written,
              // the actions included. A queued effect for a transition that did not happen is
              // exactly what enqueueing inside the transaction prevents.
              let! outcome =
                  (newProcessor ())
                      .Commit(
                          held.Work.CommandId,
                          LeaseToken.ofInt64 999999L,
                          Epoch.initial,
                          draft held.Work.EntityId Idle (Active 1) effectiveAt,
                          noCancellation
                      )

              Expect.equal FinalizeOutcome.LeaseLost (outcome |> expectOk "commit") "fenced out"
              Expect.equal 0L (queueDepth ()) "no effects for a transition that never happened"
          }

          testTask "a rejection queues nothing" {
              do! reset ()
              let inbox = newInbox ()
              let! held = leased inbox "e1" "k1"

              let! _ =
                  (newProcessor ())
                      .Reject(held.Work.CommandId, held.Token, CommandFailure.Domain(Refused "no"), noCancellation)

              Expect.equal 0L (queueDepth ()) "a refused command asked for nothing"
          }

          testTask "a claimed action carries the command and ordinal that produced it" {
              do! reset ()
              let inbox = newInbox ()
              let! held = leased inbox "e1" "k1"
              let! _ = commit (newProcessor ()) held

              let! claimed = claimActions (newActionQueue ())

              match claimed with
              | [ (action: LeasedAction<Entity, TestAction>) ] ->
                  Expect.equal held.Work.CommandId action.Work.CommandId "the command that emitted it"
                  Expect.equal 0 action.Work.Ordinal "the first action in the list"
                  Expect.equal (Epoch.ofUInt64 1UL) action.Work.Epoch "the epoch the commit wrote"
                  Expect.equal (entity "e1") action.Work.EntityId "the entity"
                  Expect.equal (Notify "done") action.Work.Action "the action itself, decoded"
              | other -> failtestf "expected exactly one claimed action, got %d" (List.length other)
          }

          testTask "completing archives the message" {
              do! reset ()
              let inbox = newInbox ()
              let! held = leased inbox "e1" "k1"
              let! _ = commit (newProcessor ()) held

              let queue = newActionQueue ()
              let! claimed = claimActions queue
              let action: LeasedAction<Entity, TestAction> = List.exactlyOne claimed

              let! outcome = queue.Complete(action, noCancellation)
              Expect.equal Updated (outcome |> expectOk "complete") "delivered"
              Expect.equal 0L (queueDepth ()) "the message left the queue"
          }

          testTask "a stale delivery count cannot complete" {
              // pgmq's own archive ignores read_ct, so this is the property the fsm wrappers
              // exist to add. Without it, a worker that stalled past its visibility timeout could
              // acknowledge work another worker had already reclaimed.
              do! reset ()
              let inbox = newInbox ()
              let! held = leased inbox "e1" "k1"
              let! _ = commit (newProcessor ()) held

              let queue = newActionQueue ()
              let! claimed = claimActions queue
              let action: LeasedAction<Entity, TestAction> = List.exactlyOne claimed

              let stale =
                  { action with
                      DeliveryCount = action.DeliveryCount - 1 }

              let! outcome = queue.Complete(stale, noCancellation)
              Expect.equal LeaseUpdateOutcome.LeaseLost (outcome |> expectOk "complete") "fenced out"
              Expect.equal 1L (queueDepth ()) "the message is untouched"
          }

          testTask "a stale delivery count cannot reschedule" {
              do! reset ()
              let inbox = newInbox ()
              let! held = leased inbox "e1" "k1"
              let! _ = commit (newProcessor ()) held

              let queue = newActionQueue ()
              let! claimed = claimActions queue
              let action: LeasedAction<Entity, TestAction> = List.exactlyOne claimed

              let stale =
                  { action with
                      DeliveryCount = action.DeliveryCount + 7 }

              let! outcome = queue.Reschedule(stale, Backoff.defaults, noCancellation)
              Expect.equal LeaseUpdateOutcome.LeaseLost (outcome |> expectOk "reschedule") "fenced out"
          }

          testTask "a stale delivery count cannot abandon" {
              do! reset ()
              let inbox = newInbox ()
              let! held = leased inbox "e1" "k1"
              let! _ = commit (newProcessor ()) held

              let queue = newActionQueue ()
              let! claimed = claimActions queue
              let action: LeasedAction<Entity, TestAction> = List.exactlyOne claimed

              let stale =
                  { action with
                      DeliveryCount = action.DeliveryCount - 1 }

              let! outcome = queue.Abandon(stale, Reason.create "gave up", noCancellation)
              Expect.equal LeaseUpdateOutcome.LeaseLost (outcome |> expectOk "abandon") "fenced out"
              Expect.equal 0L (deadLetterCount ()) "and nothing was recorded"
          }

          testTask "abandoning records the effect that never happened" {
              do! reset ()
              let inbox = newInbox ()
              let! held = leased inbox "e1" "k1"
              let! _ = commit (newProcessor ()) held

              let queue = newActionQueue ()
              let! claimed = claimActions queue
              let action: LeasedAction<Entity, TestAction> = List.exactlyOne claimed

              let! outcome = queue.Abandon(action, Reason.create "the destination refused it", noCancellation)
              Expect.equal Updated (outcome |> expectOk "abandon") "given up on"
              Expect.equal 0L (queueDepth ()) "the message left the queue"
              Expect.equal 1L (deadLetterCount ()) "and the fact was recorded"

              let reason = scalar<string> "SELECT reason FROM fsm.action_dead_letter"

              Expect.equal "the destination refused it" reason "the reason survives"
          }

          testTask "a redelivery keeps the same command and ordinal" {
              // The queue's own message id changes between deliveries, which is why a
              // destination deduplicates on (command_id, ordinal) instead.
              do! reset ()
              let inbox = newInbox ()
              let! held = leased inbox "e1" "k1"
              let! _ = commit (newProcessor ()) held

              let queue = newActionQueue ()
              let! first = claimActions queue
              let firstAction: LeasedAction<Entity, TestAction> = List.exactlyOne first

              // Return it to the queue immediately, then claim again.
              let! _ =
                  queue.Reschedule(
                      firstAction,
                      Backoff.create (TimeSpan.FromMilliseconds 1.) (TimeSpan.FromMilliseconds 1.),
                      noCancellation
                  )

              do! Task.Delay 1200

              let! second = claimActions queue
              let secondAction: LeasedAction<Entity, TestAction> = List.exactlyOne second

              Expect.equal firstAction.Work.CommandId secondAction.Work.CommandId "the same command"
              Expect.equal firstAction.Work.Ordinal secondAction.Work.Ordinal "the same position"

              Expect.isGreaterThan
                  secondAction.DeliveryCount
                  firstAction.DeliveryCount
                  "the delivery count climbed, which is what fences the earlier claim"
          }

          testTask "creating the queue twice is harmless" {
              do! reset ()
              let created = scalar<bool> $"SELECT fsm.ensure_action_queue('{actionQueue}')"
              Expect.isFalse created "the fixture already created it, so this call did nothing"
          }

          testTask "a queue name that would need quoting is refused by the database too" {
              // The F# builder checks the same pattern. This is the layer that does the
              // interpolating, and the layer that interpolates cannot afford to assume.
              do! reset ()

              match exec "SELECT fsm.ensure_action_queue('bad name; DROP TABLE x')" with
              | Error error -> Expect.stringContains (error.ToString()) "queue name" "refused by the allowlist"
              | Ok() -> failtest "an unacceptable queue name was accepted"
          } ]
