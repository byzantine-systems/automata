module ByzantineSystems.Automata.Storage.Postgres.Tests.ProfileTests

open System
open System.Security.Cryptography
open System.Text
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Runtime
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.ContractProperties
open ByzantineSystems.Automata.Storage.Postgres
open Expecto
open Npgsql
open TestContext

let private suffix =
    SHA256.HashData(Encoding.UTF8.GetBytes schemaValue)
    |> Convert.ToHexString
    |> fun text -> text[..15].ToLowerInvariant()

let private name = "automata_snapshot_" + suffix
let private chosen = SchemaName.tryCreate name |> expectOk "snapshot schema"
let private queue = "snapshot_test_" + suffix

let private getContext () =
    context () |> PostgresContext.withSchema chosen

let private execute statement =
    use connection = (dataSource ()).OpenConnection()
    use command = new NpgsqlCommand(statement, connection)
    command.ExecuteNonQuery() |> ignore

let private scalar<'T> statement =
    use connection = (dataSource ()).OpenConnection()
    use command = new NpgsqlCommand(statement, connection)
    command.ExecuteScalar() :?> 'T

let private migrate beliefs =
    Migrator.migrate
        Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance
        { ConnectionString = connectionString
          Schema = chosen
          Beliefs = beliefs }

let private seed () =
    execute
        $"SELECT {name}.ensure_action_queue('{queue}');
        INSERT INTO {name}.machine_chart_version(machine_id, version, fingerprint) VALUES('pg-tests',1,repeat('a',64));
        INSERT INTO {name}.machine_maintenance(machine_id, action_queue) VALUES('pg-tests','{queue}')"

let private initialize beliefs =
    execute $"DROP SCHEMA IF EXISTS {name}_temporal CASCADE; DROP SCHEMA IF EXISTS {name} CASCADE"
    migrate beliefs |> expectOk "install profile" |> ignore
    // A clean reset changes domain OIDs. Discard connections prepared against the old types.
    (dataSource ()).Clear()
    seed ()
    execute $"SELECT pgmq.purge_queue('{queue}'); DELETE FROM pgmq.a_{queue}"

let private installed = lazy (initialize BeliefModel.Snapshot)

let private resetSnapshot () : Task =
    task {
        installed.Force()

        execute
            $"TRUNCATE {name}.entity_state, {name}.command, {name}.command_error, {name}.transition,
            {name}.machine_chart_version, {name}.supervision_event, {name}.action_dead_letter,
            {name}.machine_maintenance, {name}.command_correction, {name}.entity_purge RESTART IDENTITY;
            SELECT pgmq.purge_queue('{queue}'); DELETE FROM pgmq.a_{queue}"

        seed ()
    }

let private options () =
    MachineStoreOptions.forEntityId<TestEntity, TestState, TestEvent, TestAction, TestError>
        (getContext ())
        (ActionDelivery.Deliver(ActionQueueName.create queue))

let private snapshotStore () = PostgresMachineStore(options ())

let private bitemporalStore () =
    PostgresBitemporalMachineStore(MachineStoreOptions.bitemporal (options ()))

let private additiveChart =
    statechart<TestState, TestEvent, TestAction, TestError> {
        root "root"
        classify (fun _ -> stateId "total")

        state "total" {
            on
                (fun _ event ->
                    match event with
                    | Start _ -> true
                    | Finish -> false)
                (fun state event ->
                    match state, event with
                    | Idle, Start n -> [ Notify "started" ], Active n
                    | Active total, Start n -> [ Notify "added" ], Active(total + n)
                    | Idle, Finish -> [], Idle
                    | Active total, Finish -> [], Active total)
        }
    }
    |> expectOk "additive chart"

let private commitOne (store: IMachineStore<Entity, TestState, TestEvent, TestAction, TestError>) key expected =
    task {
        let! submitted = store.SubmitAsync(submission (entityId "e1") key (Start 1), noCancellation)
        let command = submitted |> expectOk "submit" |> commandIdOf

        let! held =
            (store :> Inbox)
                .ClaimAsync(
                    machine,
                    ClaimBatch.create 1,
                    LeaseDuration.create (TimeSpan.FromMinutes 1.),
                    noCancellation
                )

        let held = held |> expectOk "claim" |> List.exactlyOne

        let! committed =
            store.CommitAsync(
                command,
                held.Token,
                expected,
                draft (entityId "e1") Idle (Active 1) (startTime.AddHours(float (Epoch.value expected))),
                noCancellation
            )

        Expect.equal committed (Ok(Finalized(Epoch.next expected))) "the snapshot advances exactly once"
        return command
    }

let conformance =
    ConformanceTests.forInstallation
        "PostgreSQL snapshot conformance"
        getContext
        resetSnapshot
        queue
        BeliefModel.Snapshot
        (Some(fun () ->
            MachineStoreOptions.forEntityId<ContractEntity, ContractState, ContractEvent, ContractAction, ContractError>
                (getContext ())
                (ActionDelivery.Deliver(ActionQueueName.create queue))
            |> SnapshotCorrectionStore
            :> CorrectionContract.Store))

let tests =
    testList
        "PostgreSQL profiles"
        [ testTask "snapshot stores run without temporal tables and advertise no temporal capabilities" {
              initialize BeliefModel.Snapshot
              let store = snapshotStore ()
              Expect.isNone (Store.tryTemporal store) "no time-travel reader"
              Expect.isNone (Store.tryCorrections store) "no bundled correction writer"

              Expect.isFalse
                  (scalar<bool> $"SELECT to_regnamespace('{name}_temporal') IS NOT NULL")
                  "the module is absent"

              Expect.isTrue
                  (scalar<bool> $"SELECT to_regclass('{name}.snapshot_schemaversions') IS NOT NULL")
                  "snapshot migrations have an independent journal"

              let! boot = (store :> IStoreBoot).BootAsync(machine, noCancellation)
              Expect.equal boot (Ok BootReport.Ready) "snapshot boot needs no temporal module"
              let! _ = commitOne store "one" Epoch.initial
              let! snapshot = (store :> Reader).TryGetSnapshotAsync(machine, entityId "e1", noCancellation)

              Expect.equal
                  (snapshot
                   |> expectOk "snapshot"
                   |> Option.map (fun (row: Snapshot<TestState>) -> row.Epoch))
                  (Some(Epoch.next Epoch.initial))
                  "core snapshot is authoritative"
          }

          testTask "both profile mismatches are refused before boot writes" {
              initialize BeliefModel.Snapshot
              let! temporalBoot = (bitemporalStore () :> IStoreBoot).BootAsync(machine, noCancellation)

              match temporalBoot with
              | Ok(BootReport.Refused defects) ->
                  Expect.isTrue
                      (defects
                       |> List.exists (function
                           | BootDefect.Misconfigured _ -> true
                           | _ -> false))
                      "configured bitemporal against snapshot"
              | other -> failtestf "expected refusal, got %A" other

              migrate BeliefModel.Bitemporal |> expectOk "enable" |> ignore
              let! snapshotBoot = (snapshotStore () :> IStoreBoot).BootAsync(machine, noCancellation)

              match snapshotBoot with
              | Ok(BootReport.Refused [ BootDefect.Misconfigured _ ]) -> ()
              | other -> failtestf "expected snapshot mismatch, got %A" other

              Expect.equal
                  (migrate BeliefModel.Snapshot)
                  (Error(MigrationError.ProfileMismatch(BeliefModel.Snapshot, BeliefModel.Bitemporal)))
                  "migrations never silently remove history"
          }

          testTask "opting in seeds a live snapshot and subsequent commits extend its timeline" {
              initialize BeliefModel.Snapshot
              let! _ = commitOne (snapshotStore ()) "before" Epoch.initial
              migrate BeliefModel.Bitemporal |> expectOk "opt in" |> ignore
              let store = bitemporalStore ()

              let! seeded =
                  (store :> ITemporalReader<Entity, TestState>)
                      .ValidAtAsync(machine, entityId "e1", startTime, noCancellation)

              Expect.equal
                  (seeded |> expectOk "seed" |> Option.map _.Snapshot.State)
                  (Some(Active 1))
                  "the snapshot is seeded"

              let! _ = commitOne store "after" (Epoch.next Epoch.initial)

              Expect.equal
                  (scalar<int64> $"SELECT count(*) FROM {name}_temporal.instance_state")
                  2L
                  "the second commit splits the seeded belief"
          }

          testTask "a snapshot client cannot bypass history through its commit entry point" {
              initialize BeliefModel.Bitemporal

              let! submitted =
                  (snapshotStore () :> Inbox).SubmitAsync(submission (entityId "e1") "bypass" (Start 1), noCancellation)

              let id = submitted |> expectOk "submit" |> commandIdOf

              let! held =
                  (snapshotStore () :> Inbox)
                      .ClaimAsync(
                          machine,
                          ClaimBatch.create 1,
                          LeaseDuration.create (TimeSpan.FromMinutes 1.),
                          noCancellation
                      )

              let held = held |> expectOk "claim" |> List.exactlyOne

              let! result =
                  (snapshotStore () :> Processor)
                      .CommitAsync(
                          id,
                          held.Token,
                          Epoch.initial,
                          draft (entityId "e1") Idle (Active 1) startTime,
                          noCancellation
                      )

              match result with
              | Error(StoreError.Unexpected(:? PostgresException as error)) ->
                  Expect.equal error.SqlState "22023" "routine-level backstop"
              | other -> failtestf "expected profile refusal, got %A" other

              Expect.equal (scalar<int64> $"SELECT count(*) FROM {name}.entity_state") 0L "nothing partially committed"
          }

          testTask "operator corrections conflict on the core epoch and never change it" {
              initialize BeliefModel.Bitemporal
              let store = bitemporalStore ()
              let! _ = commitOne store "one" Epoch.initial

              let! conflict =
                  (store :> ICorrectionStore<Entity, TestState>)
                      .CorrectAsync(machine, entityId "e1", Epoch.initial, startTime, [], noCancellation)

              Expect.equal
                  conflict
                  (Ok(CorrectionOutcome.Conflict(Epoch.next Epoch.initial)))
                  "stale operator state is fenced"

              let! erased =
                  (store :> ICorrectionStore<Entity, TestState>)
                      .CorrectAsync(machine, entityId "e1", Epoch.next Epoch.initial, startTime, [], noCancellation)

              Expect.equal erased (Ok(Corrected 1)) "an empty belief list can erase the timeline"
              let! _ = commitOne store "next" (Epoch.next Epoch.initial)

              Expect.equal
                  (scalar<int64> $"SELECT epoch FROM {name}.entity_state")
                  2L
                  "erasing beliefs cannot regress the epoch"
          }

          testTask "row-write counts distinguish snapshot upserts from temporal splits" {
              for model in [ BeliefModel.Snapshot; BeliefModel.Bitemporal ] do
                  initialize model
                  let inbox = snapshotStore () :> Inbox
                  let mutable writes = []

                  for step in 0..1 do
                      let! _ =
                          inbox.SubmitAsync(submission (entityId "counted") $"k{step}" (Start step), noCancellation)

                      let! batch =
                          inbox.ClaimAsync(
                              machine,
                              ClaimBatch.create 1,
                              LeaseDuration.create (TimeSpan.FromMinutes 1.),
                              noCancellation
                          )

                      let held = batch |> expectOk "claim counted" |> List.exactlyOne
                      use connection = (dataSource ()).OpenConnection()
                      use transaction = connection.BeginTransaction()

                      let count () =
                          use command =
                              new NpgsqlCommand(
                                  $"SELECT coalesce(sum(pg_stat_get_xact_tuples_inserted(c.oid) + pg_stat_get_xact_tuples_updated(c.oid)),0)::bigint
                                FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                                WHERE n.nspname IN ('{name}','{name}_temporal') AND c.relkind='r'",
                                  connection,
                                  transaction
                              )

                          command.ExecuteScalar() :?> int64

                      let before = count ()

                      let owner =
                          match model with
                          | BeliefModel.Snapshot -> name
                          | BeliefModel.Bitemporal -> name + "_temporal"

                      use commit =
                          new NpgsqlCommand(
                              $"SELECT * FROM {owner}.commit_command(@id,@token,@expected,'succeeded',NULL,'{{}}'::jsonb,'running',@at,
                              '{{}}'::jsonb,'[]'::jsonb,'{{}}'::jsonb,'{{}}'::jsonb,'root','{{}}'::text[],'{{}}'::text[])",
                              connection,
                              transaction
                          )

                      commit.Parameters.AddWithValue("id", CommandId.value held.Work.CommandId)
                      |> ignore

                      commit.Parameters.AddWithValue("token", LeaseToken.value held.Token) |> ignore
                      commit.Parameters.AddWithValue("expected", int64 step) |> ignore
                      commit.Parameters.AddWithValue("at", startTime.AddHours(float step)) |> ignore
                      let! _ = commit.ExecuteNonQueryAsync noCancellation
                      writes <- writes @ [ count () - before ]
                      do! transaction.CommitAsync noCancellation

                  match model with
                  | BeliefModel.Snapshot -> Expect.equal writes [ 3L; 3L ] "transition + snapshot + command"
                  | BeliefModel.Bitemporal ->
                      Expect.equal writes [ 4L; 6L ] "first belief, then split + history alongside core writes"

                  printfn
                      "PostgreSQL %s row writes: first=%d subsequent=%d (no actions, no queued sibling)"
                      (BeliefModel.value model)
                      writes[0]
                      writes[1]
          }

          testTask "runtime replay works through a custom snapshot correction capability without beliefs" {
              initialize BeliefModel.Snapshot
              execute $"DELETE FROM {name}.machine_chart_version"
              let custom = SnapshotCorrectionStore(options ())

              let built =
                  MachineCE.machine<Entity, TestState, TestEvent, TestAction, TestError> machine {
                      chart additiveChart
                      chartVersion 1
                      initialState Idle
                      store (custom :> IMachineStore<Entity, TestState, TestEvent, TestAction, TestError>)
                  }
                  |> expectOk "build custom machine"

              let! started =
                  Machine.startAsync built (PostgresChartRegistry({ Context = getContext () })) noCancellation

              started |> expectOk "start custom machine" |> ignore

              for key, amount, hour in [ "one", 1, 10; "two", 2, 12 ] do
                  let envelope =
                      EventEnvelope.create key (Start amount)
                      |> EventEnvelope.withReceivedAt (startTime.AddHours(float hour))

                  let! submitted = Machine.enqueue built (entityId "custom") envelope noCancellation
                  submitted |> expectOk "enqueue history" |> ignore
                  let! polled = (Machine.processor built).PollAsync noCancellation
                  polled |> expectOk "process history" |> ignore

              let! preview =
                  Machine.previewCorrection
                      built
                      (entityId "custom")
                      (Start 10)
                      (startTime.AddHours 11.)
                      CorrectionPolicy.defaults
                      noCancellation

              let plan = preview |> expectOk "preview without beliefs"
              Expect.equal plan.Commit.Transition.ToState (Active 13) "prior state plus replayed suffix"

              let! submitted =
                  Machine.correct
                      built
                      (entityId "custom")
                      (EventEnvelope.create "missed" (Start 10))
                      (startTime.AddHours 11.)
                      CorrectionPolicy.defaults
                      noCancellation

              submitted |> expectOk "enqueue correction" |> ignore
              let! polled = (Machine.processor built).PollAsync noCancellation
              polled |> expectOk "replay correction" |> ignore
              let! current = Machine.state built (entityId "custom") noCancellation

              Expect.equal
                  (current
                   |> expectOk "corrected snapshot"
                   |> Option.map (fun (s: Snapshot<TestState>) -> s.State, s.Epoch))
                  (Some(Active 13, Epoch.ofUInt64 3UL))
                  "custom transaction advanced the snapshot"

              Expect.equal
                  (scalar<int64> $"SELECT count(*) FROM pgmq.q_{queue}")
                  2L
                  "historical actions were not re-emitted"

              Expect.isFalse
                  (scalar<bool> $"SELECT to_regnamespace('{name}_temporal') IS NOT NULL")
                  "no temporal schema was involved"

              do! Machine.stopAsync built noCancellation
          } ]
