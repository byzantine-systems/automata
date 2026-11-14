module ByzantineSystems.Automata.Storage.Postgres.Tests.CommandInboxTests

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Postgres
open Expecto
open Npgsql
open TestContext

let private entity name : Entity = entityId name

/// Annotated at every binding below. Leased<'kind, 'Work> is generic in the work it carries, so
/// `leased.Work.IdempotencyKey` gives inference nothing to go on and F# resolves the field
/// against whichever record declared it last.
type private Claimed = LeasedCommand<Entity, TestEvent>
let private lease = LeaseDuration.create (TimeSpan.FromSeconds 30.0)

let private submit (inbox: Inbox) entityName key event =
    task {
        let! outcome = inbox.SubmitAsync(submission (entity entityName) key event, noCancellation)
        return outcome |> expectOk $"submit {key}"
    }

let private claim (inbox: Inbox) batch : Task<Claimed list> =
    task {
        let! claimed = inbox.ClaimAsync(machine, ClaimBatch.create batch, lease, noCancellation)
        return claimed |> expectOk "claim"
    }

let tests =
    testList
        "Postgres ICommandInbox"
        [ testCaseTask "arrival is sampled after a blocked submit takes the entity lock" (fun () ->
              task {
                  do! reset ()
                  use deadline = new CancellationTokenSource(TimeSpan.FromSeconds 10.)
                  let ct = deadline.Token
                  use! holder = (dataSource ()).OpenConnectionAsync ct
                  use! blocked = (dataSource ()).OpenConnectionAsync ct
                  use! transaction = holder.BeginTransactionAsync ct

                  use acquire =
                      new NpgsqlCommand(
                          sql
                              "SELECT pg_advisory_xact_lock(hashtextextended(format('fsm.command/%s/%s', @machine::text, 'arrival-race'), 0))",
                          holder,
                          transaction
                      )

                  acquire.Parameters.AddWithValue("machine", MachineId.value machine) |> ignore
                  let! _ = acquire.ExecuteNonQueryAsync ct

                  let submitSql =
                      sql "SELECT * FROM fsm.submit_command(@machine, 'arrival-race', @key, 1, '{}'::jsonb)"

                  use waiting = new NpgsqlCommand(submitSql, blocked)
                  waiting.Parameters.AddWithValue("machine", MachineId.value machine) |> ignore
                  waiting.Parameters.AddWithValue("key", "second") |> ignore
                  let waitingResult = waiting.ExecuteNonQueryAsync ct

                  use probe =
                      new NpgsqlCommand(
                          "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = @pid AND locktype = 'advisory' AND NOT granted)",
                          holder,
                          transaction
                      )

                  probe.Parameters.AddWithValue("pid", blocked.ProcessID) |> ignore
                  let mutable isWaiting = false

                  while not isWaiting do
                      let! observed = probe.ExecuteScalarAsync ct
                      isWaiting <- unbox<bool> observed

                      if not isWaiting then
                          do! Task.Delay(5, ct)

                  // B's statement clock is now older than this first insertion's arrival.
                  use first = new NpgsqlCommand(submitSql, holder, transaction)
                  first.Parameters.AddWithValue("machine", MachineId.value machine) |> ignore
                  first.Parameters.AddWithValue("key", "first") |> ignore
                  let! _ = first.ExecuteNonQueryAsync ct
                  do! transaction.CommitAsync ct
                  let! _ = waitingResult

                  use result =
                      new NpgsqlCommand(
                          sql
                              "SELECT received_at FROM fsm.command WHERE machine_id = @machine AND entity_id = 'arrival-race' ORDER BY seq",
                          holder
                      )

                  result.Parameters.AddWithValue("machine", MachineId.value machine) |> ignore
                  use! reader = result.ExecuteReaderAsync ct
                  let! _ = reader.ReadAsync ct
                  let firstArrival = reader.GetDateTime 0
                  let! _ = reader.ReadAsync ct
                  let secondArrival = reader.GetDateTime 0

                  Expect.isGreaterThanOrEqual
                      secondArrival
                      firstArrival
                      "waiting on the lock cannot reverse arrival order"
              })

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

              let! outcome = inbox.SubmitAsync(request, noCancellation)
              let commandId = commandIdOf (outcome |> expectOk "submit")
              let! found = inbox.TryGetAsync(commandId, noCancellation)

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
                            inbox.SubmitAsync(submission (entity name) $"{name}-{index}" (Start index), noCancellation) ]

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
                          let! claimed = inbox.ClaimAsync(machine, ClaimBatch.create 4, lease, noCancellation)

                          match claimed |> expectOk "worker claim" with
                          | [] -> working <- false
                          | batch ->
                              for leased in batch do
                                  let! outcome =
                                      (newProcessor ())
                                          .RejectAsync(
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
