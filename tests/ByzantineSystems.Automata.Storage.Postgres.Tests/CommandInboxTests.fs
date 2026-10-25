module ByzantineSystems.Automata.Storage.Postgres.Tests.CommandInboxTests

open System
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Postgres
open ByzantineSystems.Automata.Storage.ContractProperties
open Expecto
open TestContext

let private entity name : Entity = entityId name

/// Annotated at every binding below. Leased<'kind, 'Work> is generic in the work it carries, so
/// `leased.Work.IdempotencyKey` gives inference nothing to go on and F# resolves the field
/// against whichever record declared it last.
type private Claimed = LeasedCommand<Entity, TestEvent>
let private lease = LeaseDuration.create (TimeSpan.FromSeconds 30.0)

let private submit (inbox: Inbox) entityName key event =
    task {
        let! outcome = inbox.Submit(submission (entity entityName) key event, noCancellation)
        return outcome |> expectOk $"submit {key}"
    }

let private claim (inbox: Inbox) batch : Task<Claimed list> =
    task {
        let! claimed = inbox.Claim(machine, ClaimBatch.create batch, lease, noCancellation)
        return claimed |> expectOk "claim"
    }

// The shared inbox state machine, over this provider: one truncate-and-reseed per generated
// case, and the database's own clock, so time moves by sleeping. Lease deadlines are stamped
// by the database, which is why the model is given a deadband around them.
let private contractEnv () : ContractInboxEnv =
    let encode, decode = EntityKey.forEntityId<ContractEntity>

    let stores () =
        { Inbox =
            PostgresCommandInbox<ContractEntityId, ContractEvent>(
                { Context = context ()
                  EventCodec = Serialization.systemTextJson<ContractEvent> ()
                  EntityIdEncode = encode
                  EntityIdDecode = decode }
            )
            :> ContractInbox
          Processor =
            PostgresCommandProcessorStore<ContractEntityId, ContractState, ContractEvent, ContractAction, ContractError>(
                { Context = context ()
                  ActionQueue = actionQueue
                  StateCodec = Serialization.systemTextJson<ContractState> ()
                  EventCodec = Serialization.systemTextJson<ContractEvent> ()
                  ActionCodec = Serialization.systemTextJson<ContractAction list> ()
                  ErrorCodec = Serialization.systemTextJson<ContractError> ()
                  EntityIdEncode = encode
                  EntityIdDecode = decode }
            )
            :> ContractProcessor
          Now = fun () -> DateTimeOffset.UtcNow
          Advance = fun span -> Task.Delay(span + TimeSpan.FromMilliseconds 20.0) }

    { Machine = machine
      Version = version
      ClaimLease = LeaseDuration.create (TimeSpan.FromMilliseconds 50.0)
      Backoff = Backoff.create (TimeSpan.FromMilliseconds 5.0) (TimeSpan.FromMilliseconds 40.0)
      Margin = TimeSpan.FromMilliseconds 25.0
      Ticks = fun ms -> TimeSpan.FromMilliseconds(float ms)
      Fresh =
        fun () ->
            task {
                do! reset ()
                return stores ()
            } }

let private behaviorClasses =
    [ "resubmit"
      "empty-claim"
      "batch-limited"
      "blocked-sibling"
      "delay-not-yet"
      "delay-elapsed"
      "expiry-reclaimed"
      "reject-releases"
      "deadletter-releases"
      "retry-deferred"
      "renew-held"
      "stale-reject"
      "stale-deadletter"
      "stale-retry"
      "stale-renew"
      "lookup-hit"
      "lookup-miss" ]

let tests =
    testList
        "Postgres ICommandInbox"
        [ ContractProperties.inboxProperty "the inbox follows the shared state machine" 12 25 (contractEnv ())

          ContractProperties.inboxReachability
              "the generated inbox traces reach every behavior class"
              (contractEnv ())
              behaviorClasses

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

          // A serial test cannot prove ordering holds under contention. Many submitters race on
          // a handful of entities while many workers claim and finish.
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
                          let! claimed = inbox.Claim(machine, ClaimBatch.create 4, lease, noCancellation)

                          match claimed |> expectOk "worker claim" with
                          | [] -> working <- false
                          | batch ->
                              for leased in batch do
                                  let! outcome =
                                      (newProcessor ())
                                          .Reject(
                                              leased.Work.CommandId,
                                              leased.Token,
                                              CommandFailure.Domain(Refused "done"),
                                              noCancellation
                                          )

                                  outcome |> expectOk "worker finalize" |> ignore
                  }

              let! _ = Task.WhenAll [ for _ in 1..4 -> worker () ]

              Expect.equal
                  (scalar<int64> "SELECT count(*) FROM fsm.command WHERE status IN ('ready', 'leased')")
                  0L
                  "every command should have reached a terminal state exactly once"

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
