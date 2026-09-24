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

let private keepCommandsFor span =
    { RetentionPolicy.keepEverything with
        Commands = Retain.For span }

// ---------------------------------------------------------------------------
// Arranging commands through the real store, so a purge is tested against rows the library
// actually writes rather than rows a test imagined.
// ---------------------------------------------------------------------------

let private submit (name: string) (key: string) =
    task {
        let! outcome = (newInbox ()).Submit(submission (entity name) key (Start 1), noCancellation)
        return outcome |> expectOk "submit" |> commandIdOf
    }

let private claimed (key: string) =
    task {
        let! batch = (newInbox ()).Claim(machine, 50, lease, noCancellation)

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
                .Commit(
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
                .Reject(leased.Work.CommandId, leased.Token, CommandFailure.Machine "refused", noCancellation)

        outcome |> expectOk "reject" |> ignore
    }

let private run (batch: int) =
    task {
        let! reports = (newMaintenance ()).Run(batch, noCancellation)
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
        let! tick = (newMaintenance ()).NotifyPending noCancellation
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
            new NpgsqlCommand("SELECT prosrc FROM pg_proc WHERE oid = 'fsm.notify_pending()'::regprocedure", conn)

        read.ExecuteScalar() :?> string

    use prepare = new NpgsqlCommand($"PREPARE notify_probe AS {body}", conn)
    prepare.ExecuteNonQuery() |> ignore
    // EXPLAIN without ANALYZE plans the statement without running it, so nobody is notified.
    use explain = new NpgsqlCommand("EXPLAIN (FORMAT JSON) EXECUTE notify_probe", conn)
    explain.ExecuteScalar() |> string

/// Every (node type, relation) pair in a JSON plan, walked rather than string-matched, so a
/// sequential scan of a small registry table is not mistaken for one of the inbox.
let private scans (plan: string) : (string * string) list =
    let rec walk (node: JsonElement) =
        [ let kind = node.GetProperty("Node Type").GetString()

          match node.TryGetProperty "Relation Name" with
          | true, relation -> kind, relation.GetString()
          | _ -> ()

          match node.TryGetProperty "Plans" with
          | true, children ->
              for child in children.EnumerateArray() do
                  yield! walk child
          | _ -> () ]

    use document = JsonDocument.Parse plan
    walk (document.RootElement[0].GetProperty "Plan")

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
                  (Boot.defects nothing None [ "a" ])
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
                  (Boot.defects noRoutines (Some [ "a" ]) [ "a" ])
                  [ BootDefect.MissingPrerequisite "fsm routines (the repeatable migrations)" ]
                  "numbered migrations alone are not a working schema"
          }

          test "keeping forever is written as an infinite interval" {
              Expect.equal (Boot.interval Retain.Forever) "infinity" "PostgreSQL's own spelling"
          }

          test "a retention period is written exactly, in microseconds" {
              Expect.equal (Boot.interval (Retain.For(TimeSpan.FromSeconds 1.5))) "1500000 microseconds" "no rounding"
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
                      actionQueue
                  |> PostgresMachineStore

              let! booted = (store :> IStoreBoot).Boot(machine, noCancellation)
              Expect.equal (booted |> expectOk "boot") BootReport.Ready "the defaults are a working store"

              let inbox = store :> ICommandInbox<Entity, TestEvent>
              let! submitted = inbox.Submit(submission (entity "e1") "k1" (Start 1), noCancellation)
              let commandId = submitted |> expectOk "submit" |> commandIdOf
              let! found = inbox.TryGet(commandId, noCancellation)

              Expect.equal
                  (found |> expectOk "read" |> Option.map (fun record -> record.Event))
                  (Some(Start 1))
                  "the default codecs read back what they wrote"
          }

          testTask "a queue name the store could not use is refused before the database is asked" {
              // Nothing is listening on this host. Asking the database would answer Unavailable;
              // answering Misconfigured proves nobody asked.
              use unreachable = DataSource.create "Host=unreachable.invalid;Timeout=1"

              let store =
                  MachineStoreOptions.forEntityId<TestEntity, TestState, TestEvent, TestAction, TestError>
                      (PostgresContext.create
                          unreachable
                          ByzantineSystems.Automata.Resilience.TransientPolicy.defaults
                          ignore)
                      "Bad Queue; DROP TABLE"
                  |> PostgresMachineStore

              match! (store :> IStoreBoot).Boot(machine, noCancellation) with
              | Ok(BootReport.Refused [ BootDefect.Misconfigured reason ]) ->
                  Expect.stringContains reason "Bad Queue" "the reason names the queue"
              | other -> failtestf "expected a configuration refusal, got %A" other
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
                  let! snapshot = (newReader ()).TryGetSnapshot(machine, entity "e1", noCancellation)

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
              let! actions = (newActionQueue ()).Claim(machine, 10, lease, noCancellation)

              for action in actions |> expectOk "claim actions" do
                  let! _ = (newActionQueue ()).Complete(action, noCancellation)
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

              let! extended = (newInbox ()).ExtendLease(held.Work.CommandId, held.Token, lease, noCancellation)
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
              let! drift = maintenance.DetectDrift noCancellation
              Expect.equal (drift |> expectOk "detect" |> List.length) 1 "detection names the command"

              let! repaired = maintenance.RepairDrift noCancellation
              Expect.equal (repaired |> expectOk "repair" |> List.length) 1 "repair changes exactly it"

              let! after = maintenance.DetectDrift noCancellation
              Expect.isEmpty (after |> expectOk "detect again") "healthy afterwards"
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
                  new NpgsqlCommand("SELECT pg_advisory_xact_lock(hashtextextended('fsm.run_maintenance', 0))", holder)

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
              let! conn, heard = listenOn "fsm_command"
              use _ = conn

              let! tick = notify ()
              do! settle conn (TimeSpan.FromSeconds 2.)

              Expect.isGreaterThanOrEqual tick.Sent 1 "at least this machine"

              Expect.contains
                  (List.ofSeq heard)
                  ("fsm_command", MachineId.value machine)
                  "the payload is the machine id, never data"
          }

          testTask "a machine whose only command is not visible yet is not announced" {
              do! reset ()
              do! boot RetentionPolicy.keepEverything

              let later =
                  { submission (entity "e1") "k1" (Start 1) with
                      VisibleAt = Some(DateTimeOffset.UtcNow.AddHours 1.) }

              let! _ = (newInbox ()).Submit(later, noCancellation)
              let! conn, heard = listenOn "fsm_command"
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
              let! conn, heard = listenOn "fsm_action"
              use _ = conn

              let! _ = notify ()
              do! settle conn (TimeSpan.FromSeconds 2.)

              Expect.contains (List.ofSeq heard) ("fsm_action", actionQueue) "the payload is the queue name"
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
                  store.Listen(
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
                  maintenance.Schedule(TimeSpan.FromSeconds 1., TimeSpan.FromMinutes 1., 100, noCancellation)

              let expected =
                  if cronInstalled () then
                      Scheduling.NotPermitted
                  else
                      Scheduling.Unavailable

              Expect.equal (scheduling |> expectOk "schedule") expected "the database cannot run it"

              let! removed = maintenance.Unschedule noCancellation
              Expect.equal (removed |> expectOk "unschedule") 0 "nothing to remove"
          }

          testTask "scheduling twice leaves two jobs, and unscheduling leaves none" {
              if String.IsNullOrWhiteSpace cronConnectionString then
                  skiptest "set AUTOMATA_TEST_CRON_DB to a superuser connection on a pg_cron database"

              do! reset ()
              use source = DataSource.create cronConnectionString

              let maintenance =
                  PostgresMaintenance(
                      PostgresContext.create source ByzantineSystems.Automata.Resilience.TransientPolicy.defaults ignore
                  )
                  :> IDatabaseMaintenance

              let jobs () =
                  use conn = source.OpenConnection()

                  use cmd =
                      new NpgsqlCommand("SELECT count(*) FROM cron.job WHERE jobname LIKE 'automata-%'", conn)

                  cmd.ExecuteScalar() :?> int64

              for _ in 1..2 do
                  let! scheduled =
                      maintenance.Schedule(TimeSpan.FromSeconds 1., TimeSpan.FromMinutes 1., 100, noCancellation)

                  Expect.equal (scheduled |> expectOk "schedule") Scheduling.Scheduled "scheduled"

              Expect.equal (jobs ()) 2L "asserting again updates rather than duplicating"

              let! removed = maintenance.Unschedule noCancellation
              Expect.equal (removed |> expectOk "unschedule") 2 "both, by name"
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
                  """INSERT INTO fsm.command (machine_id, entity_id, seq, idempotency_key, chart_version, event, blocked)
                     SELECT 'pg-tests', 'e' || g, 1, 'k' || g, 1, '{}'::jsonb, false FROM generate_series(1, 20000) g"""

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
          reaperTests
          driftTests
          singleFlightTests
          notifyTests
          cronTests
          planTests ]
