module ByzantineSystems.Automata.Storage.Postgres.Tests.ConformanceTests

open System
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Postgres
open ByzantineSystems.Automata.Storage.ContractProperties
open Expecto
open Npgsql
open TestContext

let private entity name : Entity = entityId name
let private lease = LeaseDuration.create (TimeSpan.FromSeconds 30.0)

// The shared inbox state machine, over this provider: one truncate-and-reseed per generated
// case, and the database's own clock, so time moves by sleeping. Lease deadlines are stamped
// by the database, which is why the model is given a deadband around them.
let private contractEnv (context: unit -> PostgresContext) reset beliefs : ContractInboxEnv =
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
                  StateCodec = Serialization.systemTextJson<ContractState> ()
                  EventCodec = Serialization.systemTextJson<ContractEvent> ()
                  ActionCodec = Serialization.systemTextJson<ContractAction list> ()
                  ErrorCodec = Serialization.systemTextJson<ContractError> ()
                  EntityIdEncode = encode
                  EntityIdDecode = decode },
                beliefs = beliefs
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

// The shared history equations, over this provider: one truncate-and-reseed per generated case,
// and the database's own clock, so commit instants are bounded rather than predicted.
let private contractHistoryEnv (context: unit -> PostgresContext) reset beliefs : ContractHistoryEnv =
    let encode, decode = EntityKey.forEntityId<ContractEntity>

    let stores () =
        let processor =
            PostgresCommandProcessorStore<ContractEntityId, ContractState, ContractEvent, ContractAction, ContractError>(
                { Context = context ()
                  StateCodec = Serialization.systemTextJson<ContractState> ()
                  EventCodec = Serialization.systemTextJson<ContractEvent> ()
                  ActionCodec = Serialization.systemTextJson<ContractAction list> ()
                  ErrorCodec = Serialization.systemTextJson<ContractError> ()
                  EntityIdEncode = encode
                  EntityIdDecode = decode },
                beliefs = beliefs
            )

        { Inbox =
            PostgresCommandInbox<ContractEntityId, ContractEvent>(
                { Context = context ()
                  EventCodec = Serialization.systemTextJson<ContractEvent> ()
                  EntityIdEncode = encode
                  EntityIdDecode = decode }
            )
            :> ContractInbox
          Processor = processor :> ContractProcessor
          Reader = processor :> ContractReader
          Now = fun () -> DateTimeOffset.UtcNow }

    { Machine = machine
      Version = version
      CommittedAtTolerance = TimeSpan.FromSeconds 5.0
      Fresh =
        fun () ->
            task {
                do! reset ()
                return stores ()
            } }

// The shared action-queue equations, over this provider: one truncate-and-reseed per generated
// case, and the database's own clock, so a reschedule's backoff elapses by sleeping.
let private contractActionEnv (context: unit -> PostgresContext) reset actionQueue beliefs : ContractActionEnv =
    let encode, decode = EntityKey.forEntityId<ContractEntity>

    let stores () =
        let processor =
            PostgresCommandProcessorStore<ContractEntityId, ContractState, ContractEvent, ContractAction, ContractError>(
                { Context = context ()
                  StateCodec = Serialization.systemTextJson<ContractState> ()
                  EventCodec = Serialization.systemTextJson<ContractEvent> ()
                  ActionCodec = Serialization.systemTextJson<ContractAction list> ()
                  ErrorCodec = Serialization.systemTextJson<ContractError> ()
                  EntityIdEncode = encode
                  EntityIdDecode = decode },
                beliefs = beliefs
            )

        { Inbox =
            PostgresCommandInbox<ContractEntityId, ContractEvent>(
                { Context = context ()
                  EventCodec = Serialization.systemTextJson<ContractEvent> ()
                  EntityIdEncode = encode
                  EntityIdDecode = decode }
            )
            :> ContractInbox
          Processor = processor :> ContractProcessor
          Reader = processor :> ContractReader
          Queue =
            PostgresActionQueue<ContractEntityId, ContractAction>(
                { Context = context ()
                  Queue = actionQueue
                  ActionCodec = Serialization.systemTextJson<ContractAction> ()
                  EntityIdEncode = encode
                  EntityIdDecode = decode }
            )
            :> ContractActionQueue
          Now = fun () -> DateTimeOffset.UtcNow
          Advance = fun span -> Task.Delay(span + TimeSpan.FromMilliseconds 20.0) }

    { Machine = machine
      Version = version
      ClaimLease = LeaseDuration.create (TimeSpan.FromSeconds 30.0)
      ActionLease = LeaseDuration.create (TimeSpan.FromSeconds 30.0)
      Backoff = Backoff.create (TimeSpan.FromMilliseconds 5.0) (TimeSpan.FromMilliseconds 40.0)
      Fresh =
        fun () ->
            task {
                do! reset ()
                return stores ()
            } }

let private poisonFixture (context: unit -> PostgresContext) reset actionQueue beliefs actions count payload =
    task {
        do! reset ()

        let options =
            MachineStoreOptions.forEntityId<TestEntity, TestState, TestEvent, TestAction, TestError>
                (context ())
                (ActionDelivery.Deliver(ActionQueueName.create actionQueue))

        let store: IMachineStore<Entity, TestState, TestEvent, TestAction, TestError> =
            match beliefs with
            | BeliefModel.Snapshot -> PostgresMachineStore(options)
            | BeliefModel.Bitemporal -> PostgresBitemporalMachineStore(MachineStoreOptions.bitemporal options)

        let inbox = store :> Inbox
        let processor = store :> Processor
        let name = SchemaName.value (context ()).Schema

        let exec statement =
            use connection = (context ()).DataSource.OpenConnection()
            use command = new NpgsqlCommand(statement, connection)
            command.ExecuteNonQuery() |> ignore

        let scalar statement =
            use connection = (context ()).DataSource.OpenConnection()
            use command = new NpgsqlCommand(statement, connection)
            command.ExecuteScalar() :?> bool

        let ids = ResizeArray<CommandId>()

        for index in 1..count do
            let! submitted =
                inbox.SubmitAsync(submission (entity $"poison-{index}") $"k{index}" (Start payload), noCancellation)

            ids.Add(submitted |> expectOk "submit" |> commandIdOf)

        if actions then
            let! held = inbox.ClaimAsync(machine, ClaimBatch.create count, lease, noCancellation)

            for row in held |> expectOk "claim" do
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
                        let id = CommandId.value id

                        let sql =
                            if actions then
                                $"UPDATE pgmq.q_{actionQueue} SET message = jsonb_set(message, '{{action}}', '{{}}'::jsonb) WHERE (message->>'command_id')::bigint = {id}"
                            else
                                $"UPDATE {name}.command SET event = '{{}}'::jsonb WHERE command_id = {id}"

                        exec sql
                    }
              Claim =
                fun () ->
                    task {
                        if actions then
                            let! rows =
                                (store :> Actions)
                                    .ClaimAsync(
                                        machine,
                                        ClaimBatch.create count,
                                        LeaseDuration.create (TimeSpan.FromMinutes 1.),
                                        noCancellation
                                    )

                            return rows |> Result.map (List.map (fun row -> row.Work.CommandId))
                        else
                            let! rows =
                                inbox.ClaimAsync(
                                    machine,
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
                                scalar
                                    $"SELECT EXISTS (SELECT 1 FROM {name}.action_dead_letter WHERE command_id = {CommandId.value id} AND reason LIKE 'undecodable:%%')"
                        else
                            let! result = processor.TryGetResultAsync(id, noCancellation)

                            return
                                match result with
                                | Ok(Some(CommandResult.DeadLettered(CommandFailure.Machine reason))) ->
                                    reason.StartsWith "undecodable:"
                                | _ -> false
                    } }
    }

let private registryFresh (context: unit -> PostgresContext) reset () : Task<IChartRegistry> =
    task {
        do! reset ()
        return PostgresChartRegistry({ Context = context () }) :> IChartRegistry
    }

let private settings: Conformance.Settings =
    { InboxCases = 12
      InboxEndSize = 25
      FinalizeCases = 3
      RequestIdentityCases = 12
      ArrivalOrderCases = 12
      PoisonCases = 12
      ReadyTokenCases = 9
      ClaimExclusivityCases = 12
      ActionCases = 12
      RegistryCases = 20
      HistoryCases = 12 }

let forInstallation name context (reset: unit -> Task) actionQueue beliefs corrections =
    let reset () = task { do! reset () }

    Conformance.tests
        { Name = name
          Inbox = contractEnv context reset beliefs
          History = contractHistoryEnv context reset beliefs
          Action = contractActionEnv context reset actionQueue beliefs
          Registry = registryFresh context reset
          PoisonCommand = poisonFixture context reset actionQueue beliefs false
          PoisonAction = poisonFixture context reset actionQueue beliefs true
          Corrections =
            corrections
            |> Option.map (fun factory ->
                { CorrectionContract.Environment.Machine = machine
                  Version = version
                  Fresh =
                    fun () ->
                        task {
                            do! reset ()
                            return factory ()
                        } })
          Settings = settings }

let tests =
    forInstallation
        "PostgreSQL bitemporal conformance"
        context
        reset
        actionQueue
        BeliefModel.Bitemporal
        (Some(fun () ->
            MachineStoreOptions.forEntityId<ContractEntity, ContractState, ContractEvent, ContractAction, ContractError>
                (context ())
                (ActionDelivery.Deliver(ActionQueueName.create actionQueue))
            |> MachineStoreOptions.bitemporal
            |> PostgresBitemporalMachineStore
            :> CorrectionContract.Store))
