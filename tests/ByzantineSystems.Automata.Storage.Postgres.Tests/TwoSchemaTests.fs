module ByzantineSystems.Automata.Storage.Postgres.Tests.TwoSchemaTests

open System
open System.Security.Cryptography
open System.Text
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Resilience
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Postgres
open Expecto
open Npgsql
open TestContext

/// A second Automata installation in the SAME database as the fixture's, because everything
/// global to the database — channels, advisory keys, cron job names, pgmq queues — is the
/// surface two installations can collide on. The schemas are deliberately not symmetric: the
/// fixture's is the primary under test and this one is the interloper, so a test that passes
/// only when both are fresh is a test of neither.
let private otherSchema =
    match SchemaName.tryCreate (schemaValue + "_iso") with
    | Ok value -> value
    | Error error -> failwithf "the derived sibling schema must validate: %A" error

let private otherName = SchemaName.value otherSchema

/// Its own queue, derived the way the fixture derives its own, so the two never share a name.
let private otherQueue =
    let digest =
        SHA256.HashData(Encoding.UTF8.GetBytes otherName) |> Convert.ToHexString

    $"automata_test_%s{digest[..15].ToLowerInvariant()}"

let private otherContext () =
    PostgresContext.create (dataSource ()) TransientPolicy.defaults ignore
    |> PostgresContext.withSchema otherSchema

let private otherStore () =
    MachineStoreOptions.forEntityId<TestEntity, TestState, TestEvent, TestAction, TestError>
        (otherContext ())
        (ActionDelivery.Deliver(ActionQueueName.create otherQueue))
    |> PostgresMachineStore

let private otherInbox () : ICommandInbox<Entity, TestEvent> =
    let encode, decode = EntityKey.forEntityId<TestEntity>

    PostgresCommandInbox<Entity, TestEvent>(
        { Context = otherContext ()
          EventCodec = Serialization.systemTextJson<TestEvent> ()
          EntityIdEncode = encode
          EntityIdDecode = decode }
    )

/// Installs the sibling schema from nothing, so a previous run's leftovers cannot answer for
/// this one, and seeds the same registration the fixture seeds for the primary.
let private freshOther () : Task =
    task {
        exec $"DROP SCHEMA IF EXISTS {otherName} CASCADE"
        |> expectOk "drop the stale sibling"

        exec $"DROP SCHEMA IF EXISTS {otherName}_temporal CASCADE"
        |> expectOk "drop its temporal twin"

        match
            Migrator.migrate
                Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance
                { ConnectionString = connectionString
                  Schema = otherSchema
                  Beliefs = BeliefModel.Snapshot }
        with
        | Ok _ -> ()
        | Error(MigrationError.Failed(script, error)) ->
            failtestf "the sibling schema should migrate, failed at %s: %s" script error.Message
        | Error(MigrationError.ProfileMismatch _ as error) -> failtestf "unexpected profile: %A" error

        exec
            $"SELECT {otherName}.ensure_action_queue('{otherQueue}');
              INSERT INTO {otherName}.machine_chart_version (machine_id, version, fingerprint)
              VALUES ('{MachineId.value machine}', 1, '{fingerprint}');
              INSERT INTO {otherName}.machine_maintenance (machine_id, action_queue)
              VALUES ('{MachineId.value machine}', '{otherQueue}');"
        |> expectOk "seed the sibling"
    }

/// The db-reset recipe from the Makefile, for the primary schema only, exactly as an operator
/// would run it between the two assertions below.
let private resetPrimary () =
    exec
        $"""DO $$
        DECLARE queue_name text;
        BEGIN
            IF TO_REGPROCEDURE(format('%%I.unschedule_maintenance()', '{schemaValue}')) IS NOT NULL THEN
                EXECUTE format('SELECT %%I.unschedule_maintenance()', '{schemaValue}');
            END IF;
            IF TO_REGCLASS(format('%%I.machine_maintenance', '{schemaValue}')) IS NOT NULL THEN
                FOR queue_name IN EXECUTE format(
                        'SELECT DISTINCT action_queue FROM %%I.machine_maintenance WHERE action_queue <> ''''',
                        '{schemaValue}') LOOP
                    PERFORM pgmq.drop_queue(queue_name);
                END LOOP;
            END IF;
            FOR queue_name IN
                SELECT substring(c.relname FROM 3)
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'pgmq'
                  AND left(c.relname, 2) = 'q_'
                  AND pg_catalog.obj_description(c.oid, 'pg_class') = format('automata:%%s', '{schemaValue}')
            LOOP
                PERFORM pgmq.drop_queue(queue_name);
            END LOOP;
            EXECUTE format('DROP SCHEMA IF EXISTS %%I CASCADE', '{schemaValue}');
            EXECUTE format('DROP SCHEMA IF EXISTS %%I CASCADE', '{schemaValue}_temporal');
        END
        $$;"""
    |> expectOk "reset the primary"

/// What a second process listening on one schema's channels hears, the way MaintenanceTests
/// listens on the primary's.
let private listenOn (channel: string) =
    task {
        let! conn = (dataSource ()).OpenConnectionAsync(noCancellation).AsTask()
        let heard = ResizeArray<string * string>()
        conn.Notification.Add(fun n -> lock heard (fun () -> heard.Add(n.Channel, n.Payload)))
        use cmd = new NpgsqlCommand($"LISTEN {channel}", conn)
        let! _ = cmd.ExecuteNonQueryAsync noCancellation
        return conn, heard
    }

let tests =
    testList
        "two schemas in one database"
        [ testTask "the same machine id boots in both schemas" {
              do! reset ()
              do! freshOther ()

              let! primaryBoot =
                  (newMachineStore RetentionPolicy.keepEverything :> IStoreBoot).BootAsync(machine, noCancellation)

              let! siblingBoot = (otherStore () :> IStoreBoot).BootAsync(machine, noCancellation)

              Expect.equal primaryBoot (Ok BootReport.Ready) "the primary boots"
              Expect.equal siblingBoot (Ok BootReport.Ready) "and so does the sibling, same machine id"
          }

          // Channels are database-global. A notification a listener did not subscribe to is a
          // wasted wake-up at best and another installation's traffic at worst.
          testTask "a commit in one schema does not wake the other's listener" {
              do! reset ()
              do! freshOther ()

              let primaryChannel, _ = Listener.channels schema
              let siblingChannel, _ = Listener.channels otherSchema
              let! primaryListener, primaryHeard = listenOn primaryChannel
              use (primaryListener: NpgsqlConnection) = primaryListener
              let! siblingListener, siblingHeard = listenOn siblingChannel
              use (siblingListener: NpgsqlConnection) = siblingListener

              let! _ = (newInbox ()).SubmitAsync(submission (entityId "e1") "k1" (Start 1), noCancellation)

              exec "SELECT fsm.notify_pending()" |> expectOk "notify the primary"

              do! primaryListener.WaitAsync(TimeSpan.FromMilliseconds 300.)
              do! siblingListener.WaitAsync(TimeSpan.FromMilliseconds 300.)

              Expect.isNonEmpty primaryHeard "the primary's own listener was woken"
              Expect.isEmpty siblingHeard "the sibling's listener heard nothing"
          }

          // The submit advisory key is prefixed with the schema name; a lock the sibling holds
          // must not queue this schema's submissions behind it.
          testTask "a submit in one schema does not wait on the other's advisory lock" {
              do! reset ()
              do! freshOther ()

              use holder = (dataSource ()).OpenConnection()
              use transaction = holder.BeginTransaction()
              let encodeEntity, _ = EntityKey.forEntityId<TestEntity>

              use acquire =
                  new NpgsqlCommand(
                      $"SELECT pg_advisory_xact_lock(hashtextextended(format('{otherName}.command/%%s/%%s', @machine::text, @entity::text), 0))",
                      holder,
                      transaction
                  )

              acquire.Parameters.AddWithValue("machine", MachineId.value machine) |> ignore

              acquire.Parameters.AddWithValue("entity", encodeEntity (entityId "e1"))
              |> ignore

              let! _ = acquire.ExecuteNonQueryAsync noCancellation

              use probe =
                  new NpgsqlCommand(
                      "SELECT count(*) FROM pg_locks WHERE pid = @pid AND locktype = 'advisory' AND granted",
                      holder,
                      transaction
                  )

              probe.Parameters.AddWithValue("pid", holder.ProcessID) |> ignore

              Expect.equal
                  (probe.ExecuteScalar() :?> int64)
                  1L
                  "the sibling's lock is genuinely held, or this test exercises nothing"

              let started = DateTimeOffset.UtcNow

              let! submitted = (newInbox ()).SubmitAsync(submission (entityId "e1") "k1" (Start 1), noCancellation)

              Expect.isLessThan
                  (DateTimeOffset.UtcNow - started)
                  (TimeSpan.FromSeconds 5.)
                  "the primary's submit took its own key, not the sibling's"

              let _ = submitted |> expectOk "submit"
              do! transaction.RollbackAsync noCancellation
          }

          testTask "a second schema claiming this schema's queue name is refused at boot" {
              do! reset ()
              do! freshOther ()

              // The sibling's store configured to deliver into the PRIMARY's queue, which the
              // primary already owns. Boot is where the collision becomes visible.
              let store =
                  MachineStoreOptions.forEntityId<TestEntity, TestState, TestEvent, TestAction, TestError>
                      (otherContext ())
                      (ActionDelivery.Deliver(ActionQueueName.create actionQueue))
                  |> PostgresMachineStore

              match! (store :> IStoreBoot).BootAsync(machine, noCancellation) with
              | Ok(BootReport.Refused [ BootDefect.Misconfigured reason ]) ->
                  Expect.stringContains reason "belongs to" "the refusal names the owner"
              | other -> failtestf "expected a misconfiguration refusal, got %A" other
          }

          // E11: an extension installed from a session whose search_path points at an Automata
          // schema must land in public anyway, and must survive that schema being dropped.
          testTask "extensions ignore the migrator's search_path and survive a reset" {
              do! reset ()

              let temporary =
                  match SchemaName.tryCreate (schemaValue + "_tmp") with
                  | Ok value -> value
                  | Error error -> failtestf "the throwaway schema must validate: %A" error

              let temporaryName = SchemaName.value temporary
              let database = scalar<string> "SELECT current_database()"

              let original = scalar<string> "SELECT current_setting('search_path')"

              try
                  exec $"ALTER DATABASE \"{database}\" SET search_path TO {otherName}"
                  |> expectOk "point every new connection at the sibling schema"

                  match
                      Migrator.migrate
                          Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance
                          { ConnectionString = connectionString
                            Schema = temporary
                            Beliefs = BeliefModel.Bitemporal }
                  with
                  | Ok _ -> ()
                  | Error(MigrationError.Failed(script, error)) ->
                      failtestf "migration under a foreign search_path failed at %s: %s" script error.Message
                  | Error(MigrationError.ProfileMismatch _ as error) -> failtestf "unexpected profile: %A" error

                  Expect.equal
                      (scalar<string>
                          "SELECT extnamespace::regnamespace::text FROM pg_extension WHERE extname = 'btree_gist'")
                      "public"
                      "btree_gist installs into public, whatever the session path says"

                  Expect.equal
                      (scalar<string>
                          "SELECT n.nspname FROM pg_namespace n JOIN pg_extension e ON e.extnamespace = n.oid WHERE e.extname = 'pgmq'")
                      "pgmq"
                      "and pgmq keeps its own schema"

                  exec
                      $"DROP SCHEMA IF EXISTS {temporaryName} CASCADE;
                        DROP SCHEMA IF EXISTS {temporaryName}_temporal CASCADE;"
                  |> expectOk "reset the throwaway"

                  Expect.equal
                      (scalar<int64> "SELECT count(*) FROM pg_extension WHERE extname IN ('btree_gist', 'pgmq')")
                      2L
                      "dropping the schema the path pointed at removes neither extension"
              finally
                  exec $"ALTER DATABASE \"{database}\" RESET search_path" |> ignore

              Expect.equal
                  (scalar<string> "SELECT current_setting('search_path')")
                  original
                  "the database-level default is back to what the operator set"
          }

          // Runs last on purpose: it drops the primary schema, and the fixture re-migrates it
          // before the suite moves on.
          testTask "resetting one schema leaves the other installed and serving" {
              do! reset ()
              do! freshOther ()

              let! before = (otherInbox ()).SubmitAsync(submission (entityId "sib") "k1" (Start 1), noCancellation)

              let _ = before |> expectOk "the sibling serves before the reset"

              resetPrimary ()

              Expect.equal
                  (scalar<bool> $"SELECT to_regnamespace('{otherName}') IS NOT NULL")
                  true
                  "the sibling's schema is untouched"

              Expect.equal
                  (scalar<bool> $"SELECT to_regclass('pgmq.q_{otherQueue}') IS NOT NULL")
                  true
                  "and so is its queue, which the primary's reset had every chance to drop"

              let! after = (otherInbox ()).SubmitAsync(submission (entityId "sib") "k2" (Start 2), noCancellation)

              let _ = after |> expectOk "the sibling still serves after the reset"

              Expect.equal
                  (scalar<bool> $"SELECT to_regnamespace('{schemaValue}') IS NULL")
                  true
                  "the primary is genuinely gone, or the sibling proved nothing"

              // Leave the database as the rest of the suite expects it: primary reinstalled,
              // sibling and its queue removed.
              exec
                  $"SELECT pgmq.drop_queue('{otherQueue}');
                    DROP SCHEMA IF EXISTS {otherName} CASCADE;
                    DROP SCHEMA IF EXISTS {otherName}_temporal CASCADE;"
              |> expectOk "clean up the sibling"

              migrate ()
          } ]
