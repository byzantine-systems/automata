module ByzantineSystems.Automata.Storage.Sqlite.Tests.ConformanceTests

open System
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Sqlite
open ByzantineSystems.Automata.Storage.ContractProperties
open ByzantineSystems.Automata.Storage.Sqlite.Tests.TestContext
open Expecto

let private entity name : Entity = entityId name

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
                  ActionDelivery = ActionDelivery.Deliver(ActionQueueName.create "sqlite_actions")
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

// The shared history equations, over this provider's fixture: a fresh migrated file per
// generated case, and the fixture's frozen clock, so commit instants are exact rather than
// bounded.
let private contractHistoryEnv () : ContractHistoryEnv =
    let encode, decode = EntityKey.forEntityId<ContractEntity>

    let stores () =
        let db = fixture "contract-history"

        let processor =
            SqliteCommandProcessorStore<ContractEntityId, ContractState, ContractEvent, ContractAction, ContractError>(
                { Context = db.Context
                  ActionDelivery = ActionDelivery.Deliver(ActionQueueName.create "sqlite_actions")
                  StateCodec = Serialization.systemTextJson<ContractState> ()
                  EventCodec = Serialization.systemTextJson<ContractEvent> ()
                  ActionCodec = Serialization.systemTextJson<ContractAction list> ()
                  ErrorCodec = Serialization.systemTextJson<ContractError> ()
                  EntityIdEncode = encode
                  EntityIdDecode = decode }
            )

        { Inbox =
            SqliteCommandInbox<ContractEntityId, ContractEvent>(
                { Context = db.Context
                  EventCodec = Serialization.systemTextJson<ContractEvent> ()
                  EntityIdEncode = encode
                  EntityIdDecode = decode }
            )
            :> ContractInbox
          Processor = processor :> ContractProcessor
          Reader = processor :> ContractReader
          Now = fun () -> db.Clock.GetUtcNow() }

    { Machine = testMachine
      Version = version
      CommittedAtTolerance = TimeSpan.Zero
      Fresh = fun () -> Task.FromResult(stores ()) }

// The shared action-queue equations, over this provider's fixture: a fresh migrated file per
// generated case, and the fixture's frozen clock, so a reschedule's backoff elapses the instant
// the trace says, without sleeping.
let private contractActionEnv () : ContractActionEnv =
    let encode, decode = EntityKey.forEntityId<ContractEntity>

    let stores () =
        let db = fixture "contract-action"

        let processor =
            SqliteCommandProcessorStore<ContractEntityId, ContractState, ContractEvent, ContractAction, ContractError>(
                { Context = db.Context
                  ActionDelivery = ActionDelivery.Deliver(ActionQueueName.create "sqlite_actions")
                  StateCodec = Serialization.systemTextJson<ContractState> ()
                  EventCodec = Serialization.systemTextJson<ContractEvent> ()
                  ActionCodec = Serialization.systemTextJson<ContractAction list> ()
                  ErrorCodec = Serialization.systemTextJson<ContractError> ()
                  EntityIdEncode = encode
                  EntityIdDecode = decode }
            )

        { Inbox =
            SqliteCommandInbox<ContractEntityId, ContractEvent>(
                { Context = db.Context
                  EventCodec = Serialization.systemTextJson<ContractEvent> ()
                  EntityIdEncode = encode
                  EntityIdDecode = decode }
            )
            :> ContractInbox
          Processor = processor :> ContractProcessor
          Reader = processor :> ContractReader
          Queue =
            SqliteActionQueue<ContractEntityId, ContractAction>(
                { Context = db.Context
                  ActionCodec = Serialization.systemTextJson<ContractAction> ()
                  EntityIdEncode = encode
                  EntityIdDecode = decode }
            )
            :> ContractActionQueue
          Now = fun () -> db.Clock.GetUtcNow()
          Advance =
            fun span ->
                db.Clock.Advance span
                Task.CompletedTask }

    { Machine = testMachine
      Version = version
      ClaimLease = LeaseDuration.create (TimeSpan.FromSeconds 30.0)
      ActionLease = LeaseDuration.create (TimeSpan.FromSeconds 30.0)
      Backoff = Backoff.create (TimeSpan.FromMilliseconds 10.0) (TimeSpan.FromMilliseconds 100.0)
      Fresh = fun () -> Task.FromResult(stores ()) }

let private poisonFixture actions count payload =
    task {
        let db = fixture "poison-batch"
        let inbox = inboxOf db
        let processor = processorOf db
        let ids = ResizeArray<CommandId>()

        for index in 1..count do
            let! submitted =
                inbox.SubmitAsync(submission (entity $"poison-{index}") $"k{index}" (Start payload), noCancellation)

            ids.Add(submitted |> expectOk "submit" |> commandIdOf)

        if actions then
            let! held = claim inbox count

            for row in held do
                let! committed =
                    processor.CommitAsync(
                        row.Work.CommandId,
                        row.Token,
                        Epoch.initial,
                        draft row.Work.EntityId Idle (Active payload) startTime,
                        noCancellation
                    )

                committed |> expectOk "commit" |> ignore

        return
            { Ids = List.ofSeq ids
              Corrupt =
                fun id ->
                    task {
                        if actions then
                            exec
                                db.Db
                                "UPDATE fsm_action SET action = '{}' WHERE command_id = @id"
                                [ "@id", box (CommandId.value id) ]
                        else
                            corruptCommand
                                db.Db
                                "UPDATE fsm_command SET event = '{}' WHERE command_id = @id"
                                [ "@id", box (CommandId.value id) ]
                    }
              Claim =
                fun () ->
                    task {
                        if actions then
                            let! rows =
                                (actionsOf db)
                                    .ClaimAsync(
                                        testMachine,
                                        ClaimBatch.create count,
                                        LeaseDuration.create (TimeSpan.FromMinutes 1.),
                                        noCancellation
                                    )

                            return rows |> Result.map (List.map (fun row -> row.Work.CommandId))
                        else
                            let! rows =
                                inbox.ClaimAsync(
                                    testMachine,
                                    ClaimBatch.create count,
                                    LeaseDuration.create (TimeSpan.FromMinutes 1.),
                                    noCancellation
                                )

                            return rows |> Result.map (List.map (fun row -> row.Work.CommandId))
                    }
              IsQuarantined =
                fun id ->
                    task {
                        if actions then
                            return
                                column
                                    db.Db
                                    "SELECT reason FROM fsm_action_dead_letter WHERE command_id = @id"
                                    [ "@id", box (CommandId.value id) ]
                                |> List.exists (fun reason -> reason.StartsWith "undecodable:")
                        else
                            let! result = processor.TryGetResultAsync(id, noCancellation)

                            return
                                match result with
                                | Ok(Some(CommandResult.DeadLettered(CommandFailure.Machine reason))) ->
                                    reason.StartsWith "undecodable:"
                                | _ -> false
                    } }
    }

let private registryFresh () : Task<IChartRegistry> =
    fixture "registry-property" |> registryOf |> Task.FromResult

let private settings: Conformance.Settings =
    { InboxCases = 25
      InboxEndSize = 30
      FinalizeCases = 8
      RequestIdentityCases = 25
      ArrivalOrderCases = 25
      PoisonCases = 25
      ReadyTokenCases = 12
      ClaimExclusivityCases = 25
      ActionCases = 25
      RegistryCases = 40
      HistoryCases = 30 }

let tests =
    Conformance.tests
        { Name = "SQLite storage conformance"
          Inbox = contractEnv ()
          History = contractHistoryEnv ()
          Action = contractActionEnv ()
          Registry = registryFresh
          PoisonCommand = poisonFixture false
          PoisonAction = poisonFixture true
          Corrections = None
          Settings = settings }
