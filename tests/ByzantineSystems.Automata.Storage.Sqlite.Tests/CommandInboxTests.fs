module ByzantineSystems.Automata.Storage.Sqlite.Tests.CommandInboxTests

open System
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Sqlite
open ByzantineSystems.Automata.Storage.ContractProperties
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

// The shared inbox state machine, over this provider's fixture: a fresh migrated file per
// generated case, and the fixture's frozen clock, so leases expire and visibility opens the
// instant the trace says, without sleeping.
let private contractEnv () : ContractInboxEnv =
    let encode, decode = EntityKey.forEntityId<ContractEntity>

    let stores () =
        let db = fixture "contract-inbox"

        { Inbox =
            SqliteCommandInbox<ContractEntityId, ContractEvent>(
                { Context = db.Context
                  EventCodec = Serialization.systemTextJson<ContractEvent> ()
                  EntityIdEncode = encode
                  EntityIdDecode = decode }
            )
            :> ContractInbox
          Processor =
            SqliteCommandProcessorStore<ContractEntityId, ContractState, ContractEvent, ContractAction, ContractError>(
                { Context = db.Context
                  StateCodec = Serialization.systemTextJson<ContractState> ()
                  EventCodec = Serialization.systemTextJson<ContractEvent> ()
                  ActionCodec = Serialization.systemTextJson<ContractAction list> ()
                  ErrorCodec = Serialization.systemTextJson<ContractError> ()
                  EntityIdEncode = encode
                  EntityIdDecode = decode }
            )
            :> ContractProcessor
          Now = fun () -> db.Clock.GetUtcNow()
          Advance =
            fun span ->
                db.Clock.Advance span
                Task.CompletedTask }

    { Machine = testMachine
      Version = version
      ClaimLease = LeaseDuration.create (TimeSpan.FromMilliseconds 50.0)
      Backoff = Backoff.create (TimeSpan.FromMilliseconds 10.0) (TimeSpan.FromMilliseconds 100.0)
      Margin = TimeSpan.Zero
      Ticks = fun ms -> TimeSpan.FromMilliseconds(float ms)
      Fresh = fun () -> Task.FromResult(stores ()) }

let private behaviorClasses =
    [ "resubmit"
      "key-reused"
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
      "commit-finalized"
      "commit-conflict"
      "commit-already"
      "commit-stale-fenced"
      "terminal-stale-commit"
      "ready-zero-token-fenced"
      "expired-unreclaimed"
      "reject-already"
      "deadletter-already"
      "stale-reject"
      "stale-deadletter"
      "stale-retry"
      "stale-renew"
      "lookup-hit"
      "lookup-miss"
      "result-pending"
      "result-committed"
      "result-rejected"
      "result-deadlettered"
      "result-missing" ]

let tests =
    testList
        "ICommandInbox"
        [ ContractProperties.inboxProperty "the inbox follows the shared state machine" 25 30 (contractEnv ())

          ContractProperties.finalizeProperty "finalization obeys the shared equations" 8 (contractEnv ())

          ContractProperties.requestIdentityProperty "idempotency keys name immutable requests" 25 (contractEnv ())

          ContractProperties.arrivalOrderProperty "arrival time follows sequence order" 25 (contractEnv ())

          ContractProperties.readyTokenProperty
              "a ready command's sentinel token never authorizes finalization"
              12
              (contractEnv ())

          ContractProperties.inboxReachability
              "the generated inbox traces reach every behavior class"
              (contractEnv ())
              behaviorClasses

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
