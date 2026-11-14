module ByzantineSystems.Automata.Storage.Postgres.Tests.MaintenanceTests

open System
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Postgres
open Expecto
open Npgsql
open TestContext

let private entity name : Entity = entityId name
let private lease = TimeSpan.FromSeconds 30.0
let private hour = TimeSpan.FromHours 1.0
let private effectiveAt = DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)
let private commandChannel, actionChannel = Listener.channels schema

let private keepCommandsFor span =
    { RetentionPolicy.keepEverything with
        Commands = Retain.For span }

// ---------------------------------------------------------------------------
// Arranging commands through the real store, so a purge is tested against rows the library
// actually writes rather than rows a test imagined.
// ---------------------------------------------------------------------------

let private submit (name: string) (key: string) =
    task {
        let! outcome = (newInbox ()).SubmitAsync(submission (entity name) key (Start 1), noCancellation)
        return outcome |> expectOk "submit" |> commandIdOf
    }

let private submitCorrection (name: string) (key: string) =
    task {
        let correction =
            { submission (entity name) key (Start 1) with
                Kind = CommandKind.Correction(effectiveAt, CorrectionPolicy.defaults) }

        let! outcome = (newInbox ()).SubmitAsync(correction, noCancellation)
        return outcome |> expectOk "submit correction" |> commandIdOf
    }

let private claimed (key: string) =
    task {
        let! batch = (newInbox ()).ClaimAsync(machine, ClaimBatch.create 50, LeaseDuration.create lease, noCancellation)

        return
            batch
            |> expectOk "claim"
            |> List.tryFind (fun (c: LeasedCommand<Entity, TestEvent>) -> c.Work.IdempotencyKey = key)
            |> Option.defaultWith (fun () -> failtestf "the command %s was not claimable" key)
    }

let private commit (leased: LeasedCommand<Entity, TestEvent>) (expected: Epoch) =
    task {
        let! outcome =
            (newProcessor ())
                .CommitAsync(
                    leased.Work.CommandId,
                    leased.Token,
                    expected,
                    draft leased.Work.EntityId Idle (Active 1) effectiveAt,
                    noCancellation
                )

        return outcome |> expectOk "commit"
    }

let private reject (leased: LeasedCommand<Entity, TestEvent>) =
    task {
        let! outcome =
            (newProcessor ())
                .RejectAsync(leased.Work.CommandId, leased.Token, CommandFailure.Machine "refused", noCancellation)

        outcome |> expectOk "reject" |> ignore
    }

let private run (batch: int) =
    task {
        let! reports = (newMaintenance ()).RunAsync(ClaimBatch.create batch, noCancellation)
        return reports |> expectOk "run"
    }

let private reportFor (reports: MaintenanceReport list) =
    reports
    |> List.tryFind (fun report -> report.MachineId = machine)
    |> Option.defaultWith (fun () -> failtestf "no report for the test machine in %A" reports)

let private count (sql: string) = scalar<int64> sql

let private ageCommands () =
    arrange "UPDATE fsm.command SET received_at = now() - interval '2 hours'"

// ---------------------------------------------------------------------------
// Listening, the way a second process would.
// ---------------------------------------------------------------------------

let private listenOn (channel: string) =
    task {
        let! conn = (dataSource ()).OpenConnectionAsync(noCancellation).AsTask()
        let heard = ResizeArray<string * string>()
        conn.Notification.Add(fun n -> lock heard (fun () -> heard.Add(n.Channel, n.Payload)))
        use cmd = new NpgsqlCommand($"LISTEN {channel}", conn)
        let! _ = cmd.ExecuteNonQueryAsync()
        return conn, heard
    }

/// Waits for delivery, or for the given quiet period to pass without any.
let private settle (conn: NpgsqlConnection) (quiet: TimeSpan) =
    task {
        let! _ = conn.WaitAsync(quiet)
        return ()
    }

let private notify () =
    task {
        let! tick = (newMaintenance ()).NotifyPendingAsync noCancellation
        return tick |> expectOk "notify"
    }

// ---------------------------------------------------------------------------
// The plan probe, from notify_pending's own body, for the same reason explainClaim reads the
// claim's: a copy in the test could drift from the routine and still pass.
// ---------------------------------------------------------------------------

let private explainNotify () : string =
    use conn = (dataSource ()).OpenConnection()

    let body =
        use read =
            new NpgsqlCommand(sql "SELECT prosrc FROM pg_proc WHERE oid = 'fsm.notify_pending()'::regprocedure", conn)

        read.ExecuteScalar() :?> string

    use prepare = new NpgsqlCommand($"PREPARE notify_probe AS {body}", conn)
    prepare.ExecuteNonQuery() |> ignore
    // EXPLAIN without ANALYZE plans the statement without running it, so nobody is notified.
    use explain = new NpgsqlCommand("EXPLAIN (FORMAT JSON) EXECUTE notify_probe", conn)
    explain.ExecuteScalar() |> string

let private cronConnectionString =
    Environment.GetEnvironmentVariable "AUTOMATA_TEST_CRON_DB"

let private cronInstalled () =
    count "SELECT count(*) FROM pg_extension WHERE extname = 'pg_cron'" > 0L

let pureTests =
    testList
        "Boot decisions"
        [ test "a database that applied exactly what this build ships has no defects" {
              Expect.isEmpty (Boot.compare [ "a"; "b" ] [ "a"; "b" ]) "equal is ready"
          }

          test "a database behind the code names what it has not applied" {
              Expect.equal (Boot.compare [ "a"; "b" ] [ "a" ]) [ BootDefect.SchemaBehind [ "b" ] ] "behind"
          }

          test "a database ahead of the code names what this build does not know" {
              Expect.equal (Boot.compare [ "a" ] [ "a"; "z" ]) [ BootDefect.SchemaAhead [ "z" ] ] "ahead"
          }

          test "a database can be behind and ahead at once" {
              // Two branches' migrations meeting in one database, which is exactly when a boot
              // check earns its keep.
              Expect.equal
                  (Boot.compare [ "a"; "b" ] [ "a"; "z" ])
                  [ BootDefect.SchemaBehind [ "b" ]; BootDefect.SchemaAhead [ "z" ] ]
                  "both"
          }

          test "an empty database reports the schema and both extensions missing" {
              let nothing =
                  { HasSchema = false
                    HasJournal = false
                    HasRoutines = false
                    HasBtreeGist = false
                    HasPgmq = false }

              Expect.equal
                  (Boot.defects SchemaName.defaults nothing None [ "a" ])
                  [ BootDefect.MissingPrerequisite "btree_gist"
                    BootDefect.MissingPrerequisite "pgmq"
                    BootDefect.SchemaMissing ]
                  "one defect per thing missing, so an operator fixes them all in one go"
          }

          test "a schema whose routines never ran is refused" {
              let noRoutines =
                  { HasSchema = true
                    HasJournal = true
                    HasRoutines = false
                    HasBtreeGist = true
                    HasPgmq = true }

              Expect.equal
                  (Boot.defects SchemaName.defaults noRoutines (Some [ "a" ]) [ "a" ])
                  [ BootDefect.MissingPrerequisite "fsm routines (the repeatable migrations)" ]
                  "numbered migrations alone are not a working schema"
          }

          test "a missing journal names the configured schema, not the legacy public one" {
              let noJournal =
                  { HasSchema = true
                    HasJournal = false
                    HasRoutines = true
                    HasBtreeGist = true
                    HasPgmq = true }

              let schema =
                  match SchemaName.tryCreate "automata_ci" with
                  | Ok schema -> schema
                  | Error error -> failtestf "automata_ci must validate: %A" error

              Expect.equal
                  (Boot.defects schema noJournal None [ "a" ])
                  [ BootDefect.MissingPrerequisite "migration journal automata_ci.schemaversions" ]
                  "the operator is told which schema's journal to look for"
          }

          test "keeping forever is written as an infinite interval" {
              Expect.equal (Boot.interval Retain.Forever) "infinity" "PostgreSQL's own spelling"
          }

          test "a retention period is written exactly, in microseconds" {
              Expect.equal (Boot.interval (Retain.For(TimeSpan.FromSeconds 1.5))) "1500000 microseconds" "no rounding"
          } ]

let migratorTests =
    testList
        "Migrator"
        [ test "a database that cannot be reached is a value, not an exception" {
              // Nothing listens on this host, so the run fails before any script. The failure
              // comes back as data the host can log and exit on, never as a throw.
              match
                  Migrator.migrate
                      Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance
                      { ConnectionString = "Host=unreachable.invalid;Timeout=1"
                        Schema = SchemaName.defaults }
              with
              | Error(MigrationError.Failed(script, error)) ->
                  Expect.equal script "" "no script had started"
                  Expect.isNotNull error "and the cause travels with it"
              | Ok report -> failtestf "an unreachable database should not migrate, got %A" report
          }

          // The journal was public.schemaversions before the schema was configurable, and every
          // existing installation still has it there. Without adoption the schema-local journal
          // replays every numbered script against objects that already exist and fails.
          testTask "a legacy public journal is adopted, not replayed against" {
              if not TestContext.configured then
                  skiptest "set AUTOMATA_TEST_DB to a migrated PostgreSQL instance"

              if schema <> SchemaName.defaults then
                  skiptest "the legacy journal only ever journaled the default schema"

              do! reset ()

              // Rebuild the pre-schema-configuration shape: the journal in public, nothing in
              // the schema, and one row of retained data the replay would have to preserve.
              exec
                  """CREATE TABLE public.schemaversions (
                         schemaversionsid serial PRIMARY KEY,
                         scriptname varchar(255) NOT NULL,
                         applied timestamp NOT NULL
                     );
                     INSERT INTO public.schemaversions (scriptname, applied)
                     SELECT scriptname, applied FROM fsm.schemaversions;
                     DROP TABLE fsm.schemaversions;
                     INSERT INTO fsm.machine_chart_version (machine_id, version, fingerprint)
                     VALUES ('legacy', 42, repeat('a', 64));"""
              |> expectOk "arrange the legacy shape"

              match
                  Migrator.migrate
                      Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance
                      { ConnectionString = connectionString
                        Schema = schema }
              with
              | Ok report ->
                  Expect.isFalse
                      (report.Applied
                       |> List.exists (fun script -> script.Contains ".migrations.main."))
                      "a numbered script must never replay against an installed schema"

                  Expect.equal
                      (scalar<int64> "SELECT count(*) FROM fsm.machine_chart_version WHERE machine_id = 'legacy'")
                      1L
                      "retained data survives the upgrade"

                  Expect.equal
                      (scalar<int64> "SELECT count(*) FROM fsm.schemaversions")
                      10L
                      "the adopted journal carries every applied script"

                  Expect.isTrue
                      (scalar<bool> "SELECT to_regclass('public.schemaversions') IS NULL")
                      "the legacy journal is gone, not left to confuse the next reader"
              | Error error -> failtestf "an adopted migration should succeed, got %A" error
          } ]

let bootTests =
    testList
        "Postgres boot"
        [ testTask "a migrated database boots ready" {
              do! reset ()
              do! boot RetentionPolicy.keepEverything
          }

          testTask "a store built from the defaults boots and round-trips a command" {
              do! reset ()

              let store =
                  MachineStoreOptions.forEntityId<TestEntity, TestState, TestEvent, TestAction, TestError>
                      (context ())
                      (ActionDelivery.Deliver(ActionQueueName.create actionQueue))
                  |> PostgresMachineStore

              let! booted = (store :> IStoreBoot).BootAsync(machine, noCancellation)
              Expect.equal (booted |> expectOk "boot") BootReport.Ready "the defaults are a working store"

              let inbox = store :> ICommandInbox<Entity, TestEvent>
              let! submitted = inbox.SubmitAsync(submission (entity "e1") "k1" (Start 1), noCancellation)
              let commandId = submitted |> expectOk "submit" |> commandIdOf
              let! found = inbox.TryGetAsync(commandId, noCancellation)

              Expect.equal
                  (found |> expectOk "read" |> Option.map (fun record -> record.Event))
                  (Some(Start 1))
                  "the default codecs read back what they wrote"
          }

          testTask "boot creates the action queue when it is missing" {
              // Before this existed, a missing queue surfaced as a failed commit rather than a
              // failed boot, one command at a time.
              do! reset ()
              arrange $"SELECT pgmq.drop_queue('{actionQueue}')"

              do! boot RetentionPolicy.keepEverything

              Expect.equal
                  (count $"SELECT count(*) FROM pgmq.meta WHERE queue_name = '{actionQueue}'")
                  1L
                  "the queue exists once boot has run"
          }

          testTask "booting again replaces the registration rather than adding one" {
              do! reset ()
              do! boot RetentionPolicy.keepEverything
              do! boot (keepCommandsFor hour)

              Expect.equal (count "SELECT count(*) FROM fsm.machine_maintenance") 1L "one row per machine"

              Expect.equal
                  (scalar<TimeSpan> "SELECT keep_commands FROM fsm.machine_maintenance")
                  hour
                  "the latest boot's policy is the one in force"
          } ]

let retentionTests =
    testList
        "Postgres retention"
        [ testTask "old terminal commands are purged with everything that references them" {
              do! reset ()
              do! boot (keepCommandsFor hour)
              let! _ = submit "e1" "k1"
              let! _ = submit "e1" "k2"
              let! _ = submit "e1" "k3"
              let! first = claimed "k1"
              let! _ = commit first Epoch.initial
              let! second = claimed "k2"
              do! reject second
              ageCommands ()

              let! reports = run 100

              Expect.equal (reportFor reports).PurgedCommands 2L "the committed and the rejected command"
              Expect.equal (count "SELECT count(*) FROM fsm.transition") 0L "the transition went with its command"
              Expect.equal (count "SELECT count(*) FROM fsm.command_error") 0L "and so did the error"

              Expect.equal
                  (count "SELECT count(*) FROM fsm.command WHERE idempotency_key = 'k3'")
                  1L
                  "an open command is work, not history, and is never purged"
          }

          testTask "a terminal correction is purged with its correction metadata" {
              do! reset ()
              do! boot (keepCommandsFor hour)
              let! _ = submitCorrection "corrected" "c1"
              let! _ = submit "corrected" "later"
              let! correction = claimed "c1"
              do! reject correction
              ageCommands ()

              let! reports = run 100

              Expect.equal (reportFor reports).PurgedCommands 1L "the correction command"
              Expect.equal (count "SELECT count(*) FROM fsm.command_correction") 0L "its metadata went with it"

              Expect.equal
                  (count "SELECT count(*) FROM fsm.command WHERE idempotency_key = 'later'")
                  1L
                  "the entity's newest command remains"
          }

          testTask "an entity's newest command is kept, so its sequence never restarts" {
              do! reset ()
              do! boot (keepCommandsFor hour)
              let! _ = submit "e2" "only"
              let! only = claimed "only"
              let! _ = commit only Epoch.initial
              ageCommands ()

              let! reports = run 100
              Expect.equal (reportFor reports).PurgedCommands 0L "the newest command is kept"

              let! _ = submit "e2" "next"

              Expect.equal
                  (count "SELECT seq FROM fsm.command WHERE idempotency_key = 'next'")
                  2L
                  "the next command follows the kept one rather than starting again at 1"
          }

          testTask "keeping forever purges nothing, however old" {
              do! reset ()
              do! boot RetentionPolicy.keepEverything
              let! _ = submit "e1" "k1"
              let! _ = submit "e1" "k2"
              let! first = claimed "k1"
              let! _ = commit first Epoch.initial
              arrange "UPDATE fsm.command SET received_at = now() - interval '100 years'"

              let! reports = run 100
              Expect.equal (reportFor reports).PurgedCommands 0L "deleting audit data is never a default"
          }

          testTask "one pass purges at most one batch" {
              // Bounded so a pass is a short transaction; the next tick carries on.
              do! reset ()
              do! boot (keepCommandsFor hour)

              for key in [ "k1"; "k2"; "k3" ] do
                  let! _ = submit "e1" key
                  let! leased = claimed key
                  let! snapshot = (newReader ()).TryGetSnapshotAsync(machine, entity "e1", noCancellation)

                  let expected =
                      snapshot
                      |> expectOk "snapshot"
                      |> Option.map (fun (s: Snapshot<TestState>) -> s.Epoch)
                      |> Option.defaultValue Epoch.initial

                  let! _ = commit leased expected
                  ()

              let! _ = submit "e1" "k4"
              ageCommands ()

              let! reports = run 2
              Expect.equal (reportFor reports).PurgedCommands 2L "the batch bounds the purge"
          }

          testTask "superseded beliefs are purged by when they were superseded" {
              do! reset ()

              do!
                  boot
                      { RetentionPolicy.keepEverything with
                          BeliefHistory = Retain.For hour }

              believe "e1" (effectiveAt.AddHours 10.) (Active 1) 1L
              believe "e1" (effectiveAt.AddHours 14.) (Active 2) 2L

              arrange
                  "UPDATE fsm.instance_state_history SET system_time = tstzrange(now() - interval '3 hours', now() - interval '2 hours')"

              let liveBefore = count "SELECT count(*) FROM fsm.instance_state"
              let! reports = run 100

              Expect.isGreaterThan (reportFor reports).PurgedBeliefs 0L "the old opinion went"
              Expect.equal (count "SELECT count(*) FROM fsm.instance_state_history") 0L "all of it"

              Expect.equal
                  (count "SELECT count(*) FROM fsm.instance_state")
                  liveBefore
                  "what is believed now is never touched"
          }

          testTask "delivered actions are purged from the archive" {
              do! reset ()

              do!
                  boot
                      { RetentionPolicy.keepEverything with
                          ActionArchive = Retain.For hour }

              let! _ = submit "e1" "k1"
              let! first = claimed "k1"
              let! _ = commit first Epoch.initial

              let! actions =
                  (newActionQueue ())
                      .ClaimAsync(machine, ClaimBatch.create 10, LeaseDuration.create lease, noCancellation)

              for action in actions |> expectOk "claim actions" do
                  let! _ = (newActionQueue ()).CompleteAsync(action, noCancellation)
                  ()

              arrange $"UPDATE pgmq.a_{actionQueue} SET archived_at = now() - interval '2 hours'"

              let! reports = run 100
              Expect.equal (reportFor reports).PurgedActions 1L "the one delivered action"
          } ]

let reaperTests =
    testList
        "Postgres lease reaping"
        [ testTask "a lease long expired goes back to ready, and its holder is fenced" {
              do! reset ()
              do! boot RetentionPolicy.keepEverything
              let! _ = submit "e1" "k1"
              let! held = claimed "k1"
              arrange "UPDATE fsm.command SET visible_at = now() - interval '10 minutes'"

              let! reports = run 100
              Expect.equal (reportFor reports).Reaped 1L "past the grace period"

              Expect.equal
                  (scalar<string> "SELECT status FROM fsm.command")
                  "ready"
                  "the status says what is true: nobody holds it"

              let! late = commit held Epoch.initial

              Expect.equal
                  late
                  FinalizeOutcome.LeaseLost
                  "a worker waking up afterwards is fenced exactly as after a reclaim"
          }

          testTask "a lease inside the grace period is left for its holder to extend" {
              // extend_lease accepts an expired lease nobody reclaimed. Reaping at expiry would
              // take that back, which is what the grace period is for.
              do! reset ()
              do! boot RetentionPolicy.keepEverything
              let! _ = submit "e1" "k1"
              let! held = claimed "k1"
              arrange "UPDATE fsm.command SET visible_at = now() - interval '1 minute'"

              let! reports = run 100
              Expect.equal (reportFor reports).Reaped 0L "inside the grace period"

              let! extended =
                  (newInbox ())
                      .ExtendLeaseAsync(held.Work.CommandId, held.Token, LeaseDuration.create lease, noCancellation)

              Expect.equal (extended |> expectOk "extend") Updated "its holder can still keep it"
          } ]

let driftTests =
    testList
        "Postgres drift"
        [ testTask "drift is counted by a pass and repaired only on request" {
              do! reset ()
              do! boot RetentionPolicy.keepEverything
              let! _ = submit "e9" "d1"
              let! _ = submit "e9" "d2"
              // A head that went terminal without unblocking its successor: the bug drift
              // detection exists to surface.
              arrange "UPDATE fsm.command SET status = 'succeeded', lease_token = 1 WHERE idempotency_key = 'd1'"

              let! reports = run 100
              Expect.equal (reportFor reports).Drifted 1L "the pass counts it"

              Expect.isTrue
                  (scalar<bool> "SELECT blocked FROM fsm.command WHERE idempotency_key = 'd2'")
                  "and does not repair it"

              let maintenance = newMaintenance ()
              let! drift = maintenance.DetectDriftAsync noCancellation
              Expect.equal (drift |> expectOk "detect" |> List.length) 1 "detection names the command"

              let! repaired = maintenance.RepairDriftAsync noCancellation
              Expect.equal (repaired |> expectOk "repair" |> List.length) 1 "repair changes exactly it"

              let! after = maintenance.DetectDriftAsync noCancellation
              Expect.isEmpty (after |> expectOk "detect again") "healthy afterwards"
          } ]

/// Two versions of one entity's belief, the first superseded days ago, so a refresh moves it to
/// the cold layer. Returns the instant, in belief time, at which the first was still held.
let private oldBelief () =
    believe "e1" (effectiveAt.AddHours 10.) (Active 1) 1L
    believe "e1" (effectiveAt.AddHours 14.) (Active 2) 2L

    arrange
        "UPDATE fsm.instance_state_history SET system_time = tstzrange(now() - interval '5 days', now() - interval '4 days')"

    DateTimeOffset.UtcNow.AddDays -4.5

let private heldAt (knownAt: DateTimeOffset) =
    task {
        let! belief =
            (newTemporalReader ()).AsOfAsync(machine, entity "e1", effectiveAt.AddHours 16., knownAt, noCancellation)

        return belief |> expectOk "as of" |> Option.map (fun b -> b.Snapshot.State)
    }

let private coldRows () =
    count "SELECT count(*) FROM fsm.belief_cold"

let coldTests =
    testList
        "Postgres cold beliefs"
        [ testTask "an as-of answer is the same before and after the cold layer is refreshed" {
              // The whole correctness claim of the split: moving a belief from the live history
              // into the materialized layer changes where it is read from and nothing else.
              do! reset ()
              let knownAt = oldBelief ()
              let! before = heldAt knownAt

              do! boot RetentionPolicy.keepEverything
              let! _ = run 100
              Expect.equal (coldRows ()) 1L "the old belief moved to the cold layer"

              let! after = heldAt knownAt
              Expect.equal after before "and reads exactly as it did"
              Expect.equal after (Some(Active 1)) "which is the belief held at the time"
          }

          testTask "a pass refreshes the cold layer once a day, not every tick" {
              do! reset ()
              oldBelief () |> ignore
              do! boot RetentionPolicy.keepEverything
              let! _ = run 100
              Expect.equal (coldRows ()) 1L "the first pass refreshes"

              // A second old belief arrives after the refresh. Until the day turns the pass leaves
              // it in the live history, where the view still finds it.
              believe "e2" (effectiveAt.AddHours 10.) (Active 1) 1L
              believe "e2" (effectiveAt.AddHours 14.) (Active 2) 2L

              arrange
                  "UPDATE fsm.instance_state_history SET system_time = tstzrange(now() - interval '5 days', now() - interval '4 days') WHERE entity_id = 'e2'"

              let! _ = run 100
              Expect.equal (coldRows ()) 1L "not refreshed again the same day"

              Expect.equal
                  (count "SELECT count(*) FROM fsm.belief WHERE entity_id = 'e2'")
                  2L
                  "both of its beliefs are still served: the live one, and the old one from the live history"
          }

          testTask "retention hides a cold belief without waiting for a refresh" {
              do! reset ()
              let knownAt = oldBelief ()
              do! boot RetentionPolicy.keepEverything
              let! _ = run 100
              Expect.equal (coldRows ()) 1L "cold, and visible"

              do!
                  boot
                      { RetentionPolicy.keepEverything with
                          BeliefHistory = Retain.For(TimeSpan.FromDays 2.) }

              let! held = heldAt knownAt
              Expect.isNone held "past its retention the belief is gone for every reader, cold layer or not"
          } ]

let singleFlightTests =
    testList
        "Postgres single flight"
        [ testTask "a pass while another holds the lock does nothing and says so" {
              do! reset ()
              do! boot RetentionPolicy.keepEverything
              // Another host mid-pass, holding the lock in an open transaction.
              use holder = (dataSource ()).OpenConnection()
              use transaction = holder.BeginTransaction()

              use take =
                  new NpgsqlCommand(
                      sql "SELECT pg_advisory_xact_lock(hashtextextended('fsm.run_maintenance', 0))",
                      holder
                  )

              take.ExecuteScalar() |> ignore

              let! whileHeld = run 100
              Expect.isEmpty whileHeld "no rows means another pass is running"

              transaction.Rollback()
              let! afterwards = run 100
              Expect.isNonEmpty afterwards "and the next pass runs"
          } ]

let notifyTests =
    testList
        "Postgres notification"
        [ testTask "a machine with claimable work is announced by name" {
              do! reset ()
              do! boot RetentionPolicy.keepEverything
              let! _ = submit "e1" "k1"
              let! conn, heard = listenOn commandChannel
              use _ = conn

              let! tick = notify ()
              do! settle conn (TimeSpan.FromSeconds 2.)

              Expect.isGreaterThanOrEqual tick.Sent 1 "at least this machine"

              Expect.contains
                  (List.ofSeq heard)
                  (commandChannel, MachineId.value machine)
                  "the payload is the machine id, never data"
          }

          testTask "a machine whose only command is not visible yet is not announced" {
              do! reset ()
              do! boot RetentionPolicy.keepEverything

              let later =
                  { submission (entity "e1") "k1" (Start 1) with
                      VisibleAt = Some(DateTimeOffset.UtcNow.AddHours 1.) }

              let! _ = (newInbox ()).SubmitAsync(later, noCancellation)
              let! conn, heard = listenOn commandChannel
              use _ = conn

              let! _ = notify ()
              do! settle conn (TimeSpan.FromMilliseconds 500.)

              Expect.isEmpty heard "waking a worker for a command it cannot claim is the cost to avoid"
          }

          testTask "a queue with a visible action is announced by name" {
              do! reset ()
              do! boot RetentionPolicy.keepEverything
              let! _ = submit "e1" "k1"
              let! first = claimed "k1"
              let! _ = commit first Epoch.initial
              let! conn, heard = listenOn actionChannel
              use _ = conn

              let! _ = notify ()
              do! settle conn (TimeSpan.FromSeconds 2.)

              Expect.contains (List.ofSeq heard) (actionChannel, actionQueue) "the payload is the queue name"
          }

          testTask "a tick reports how full the notification queue is" {
              do! reset ()
              do! boot RetentionPolicy.keepEverything
              let! tick = notify ()
              Expect.isTrue (tick.QueueUsage >= 0.0 && tick.QueueUsage <= 1.0) "a fraction"
          }

          testTask "the listener says what it may have missed, then hears its machine" {
              do! reset ()
              do! boot RetentionPolicy.keepEverything
              let commands = ref 0
              let actions = ref 0
              let stop = new CancellationTokenSource()
              let store = newMachineStore RetentionPolicy.keepEverything :> IWorkNotifications

              let listening =
                  store.ListenAsync(
                      machine,
                      (fun () -> Interlocked.Increment commands |> ignore),
                      (fun () -> Interlocked.Increment actions |> ignore),
                      stop.Token
                  )

              let until (condition: unit -> bool) =
                  task {
                      let deadline = DateTime.UtcNow.AddSeconds 5.

                      while not (condition ()) && DateTime.UtcNow < deadline do
                          do! Task.Delay 20
                  }

              // Anything announced before LISTEN ran was announced to nobody, so starting to
              // listen is itself a reason to look.
              do! until (fun () -> commands.Value >= 1 && actions.Value >= 1)
              Expect.equal (commands.Value, actions.Value) (1, 1) "one hint for each, on subscribing"

              let! _ = submit "e1" "k1"
              let! _ = notify ()
              do! until (fun () -> commands.Value >= 2)
              Expect.equal commands.Value 2 "and then its own machine's announcement"

              stop.Cancel()
              let! ended = listening
              stop.Dispose()
              Expect.equal ended (Ok()) "cancellation is how a listener is meant to end"
          } ]

let cronTests =
    testList
        "Postgres cron"
        [ testTask "without the right to use pg_cron, scheduling says so and nothing is scheduled" {
              do! reset ()
              let maintenance = newMaintenance ()

              let! scheduling =
                  maintenance.ScheduleAsync(
                      TimeSpan.FromSeconds 1.,
                      TimeSpan.FromMinutes 1.,
                      ClaimBatch.create 100,
                      noCancellation
                  )

              let expected =
                  if cronInstalled () then
                      Scheduling.NotPermitted
                  else
                      Scheduling.Unavailable

              Expect.equal (scheduling |> expectOk "schedule") expected "the database cannot run it"

              let! removed = maintenance.UnscheduleAsync noCancellation
              Expect.equal (removed |> expectOk "unschedule") 0 "nothing to remove"
          }

          testTask "scheduling twice leaves three jobs, and unscheduling leaves none" {
              if String.IsNullOrWhiteSpace cronConnectionString then
                  skiptest "set AUTOMATA_TEST_CRON_DB to a superuser connection on a pg_cron database"

              do! reset ()
              use source = DataSource.create cronConnectionString

              // Migrations run as the application's role and never install pg_cron, which is
              // superuser work; this connection is a superuser, so it does what a DBA would.
              do!
                  task {
                      use conn = source.OpenConnection()
                      use install = new NpgsqlCommand("CREATE EXTENSION IF NOT EXISTS pg_cron", conn)
                      let! _ = install.ExecuteNonQueryAsync()
                      return ()
                  }

              let maintenance =
                  PostgresMaintenance(
                      PostgresContext.create source ByzantineSystems.Automata.Resilience.TransientPolicy.defaults ignore
                      |> PostgresContext.withSchema schema
                  )
                  :> IDatabaseMaintenance

              let jobs () =
                  use conn = source.OpenConnection()

                  use cmd =
                      new NpgsqlCommand(
                          $"SELECT count(*) FROM cron.job WHERE jobname LIKE 'automata-{schemaValue}-%%'",
                          conn
                      )

                  cmd.ExecuteScalar() :?> int64

              for _ in 1..2 do
                  let! scheduled =
                      maintenance.ScheduleAsync(
                          TimeSpan.FromSeconds 1.,
                          TimeSpan.FromMinutes 1.,
                          ClaimBatch.create 100,
                          noCancellation
                      )

                  Expect.equal (scheduled |> expectOk "schedule") Scheduling.Scheduled "scheduled"

              Expect.equal (jobs ()) 3L "asserting again updates rather than duplicating"

              let! removed = maintenance.UnscheduleAsync noCancellation
              Expect.equal (removed |> expectOk "unschedule") 3 "all three, by name"
              Expect.equal (jobs ()) 0L "nothing left calling a schema that may be dropped next"
          } ]

let planTests =
    testList
        "Postgres notification plan"
        [ testTask "the notification probe is served by the claim index" {
              // The probe states the claim predicate verbatim. If it drifts, it stops using the
              // index and starts scanning the inbox once per registered machine per tick.
              do! reset ()
              do! boot RetentionPolicy.keepEverything

              arrange
                  """INSERT INTO fsm.command (machine_id, entity_id, seq, idempotency_key, chart_version, event, request_digest, blocked)
                     SELECT 'pg-tests', 'e' || g, 1, 'k' || g, 1, '{}'::jsonb, sha256(''::bytea), false FROM generate_series(1, 20000) g"""

              arrange "ANALYZE fsm.command"

              let found = scans (explainNotify ())

              Expect.isTrue
                  (found |> List.exists (fun (_, relation) -> relation = "command"))
                  "the plan reads the inbox"

              Expect.isFalse
                  (found |> List.contains ("Seq Scan", "command"))
                  "a sequential scan of the inbox means the predicate drifted from the index"
          } ]

let integration =
    testList
        "Postgres maintenance"
        [ bootTests
          retentionTests
          coldTests
          reaperTests
          driftTests
          singleFlightTests
          notifyTests
          cronTests
          planTests ]
