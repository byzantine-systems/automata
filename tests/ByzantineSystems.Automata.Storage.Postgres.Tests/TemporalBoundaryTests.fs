module ByzantineSystems.Automata.Storage.Postgres.Tests.TemporalBoundaryTests

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open Expecto
open FsCheck
open Npgsql
open TestContext

let tests =
    testList
        "temporal concurrency boundaries"
        [ testTask "multiple writes in one top-level statement never archive a zero-width opinion" {
              do! reset ()

              exec
                  """DO $$ BEGIN
                  INSERT INTO fsm.instance_state(machine_id,entity_id,state,status,epoch,command_id,chart_version,valid_during)
                  VALUES('pg-tests','one-statement','{}','running',1,1,1,tstzrange('2026-01-01','infinity','[)'));
                  UPDATE fsm.instance_state SET state='{"next":1}' WHERE entity_id='one-statement';
                  END $$"""
              |> expectOk "insert and update"

              Expect.equal
                  (scalar<int64> "SELECT count(*) FROM fsm.instance_state_history")
                  0L
                  "the intermediate opinion was never observable"

              Expect.isTrue
                  (scalar<bool> "SELECT NOT isempty(system_time) FROM fsm.instance_state")
                  "the surviving opinion remains open"
          }

          testCaseTask "an update blocked behind a newer belief cannot move system time backwards" (fun () ->
              task {
                  do! reset ()
                  believe "blocked" startTime (Active 1) 1L
                  use deadline = new CancellationTokenSource(TimeSpan.FromSeconds 10.)
                  let ct = deadline.Token
                  use! holder = (dataSource ()).OpenConnectionAsync ct
                  use! waiter = (dataSource ()).OpenConnectionAsync ct
                  use! transaction = holder.BeginTransactionAsync ct

                  use take =
                      new NpgsqlCommand(
                          sql "SELECT 1 FROM fsm.instance_state WHERE entity_id='blocked' FOR UPDATE",
                          holder,
                          transaction
                      )

                  let! _ = take.ExecuteScalarAsync ct

                  use pending =
                      new NpgsqlCommand(
                          sql "UPDATE fsm.instance_state SET state='{\"v\":3}' WHERE entity_id='blocked'",
                          waiter
                      )

                  let waiting = pending.ExecuteNonQueryAsync ct

                  use probe =
                      new NpgsqlCommand(
                          "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=@pid AND wait_event_type='Lock')",
                          holder,
                          transaction
                      )

                  probe.Parameters.AddWithValue("pid", waiter.ProcessID) |> ignore
                  let mutable blocked = false

                  while not blocked do
                      let! observed = probe.ExecuteScalarAsync ct
                      blocked <- unbox<bool> observed

                      if not blocked then
                          do! Task.Delay(5, ct)

                  use advance =
                      new NpgsqlCommand(
                          sql "UPDATE fsm.instance_state SET state='{\"v\":2}' WHERE entity_id='blocked'",
                          holder,
                          transaction
                      )

                  let! _ = advance.ExecuteNonQueryAsync ct
                  do! transaction.CommitAsync ct
                  let! _ = waiting

                  Expect.equal
                      (scalar<int64> "SELECT count(*) FROM fsm.instance_state_history")
                      1L
                      "the blocked statement clamps to the newer lower bound rather than archiving an empty interval"

                  Expect.isTrue
                      (scalar<bool>
                          "SELECT lower(s.system_time)=max(upper(h.system_time)) FROM fsm.instance_state s CROSS JOIN fsm.instance_state_history h GROUP BY s.system_time")
                      "knowledge intervals meet and never reverse"
              })

          testCase "UTC cold cutoffs are invariant under session timezone" (fun () ->
              Check.One(
                  Config.QuickThrowOnFailure.WithMaxTest(12),
                  fun (choice: byte) ->
                      use connection = (dataSource ()).OpenConnection()
                      use transaction = connection.BeginTransaction()

                      let zone =
                          [| "UTC"; "Pacific/Auckland"; "America/Sao_Paulo"; "Asia/Kathmandu" |][int choice % 4]

                      use set =
                          new NpgsqlCommand($"SET LOCAL TIME ZONE '{zone}'", connection, transaction)

                      set.ExecuteNonQuery() |> ignore

                      use read =
                          new NpgsqlCommand(
                              sql
                                  "SELECT fsm.belief_cold_cutoff() = (date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC') - interval '1 day'",
                              connection,
                              transaction
                          )

                      unbox<bool> (read.ExecuteScalar())
              ))

          testTask "history purge honors the batch while skipping a locked oldest row" {
              do! reset ()

              exec
                  """INSERT INTO fsm.instance_state_history(machine_id,entity_id,state,status,epoch,command_id,chart_version,valid_during,system_time)
                  SELECT 'pg-tests', 'history-'||g, '{}','running',g,g,1,tstzrange('2026-01-01','infinity','[)'),
                    tstzrange(now()-interval '10 days',now()-interval '1 day'*g,'[)') FROM generate_series(1,5) g"""
              |> expectOk "seed old beliefs"

              use holder = (dataSource ()).OpenConnection()
              use transaction = holder.BeginTransaction()

              use lock =
                  new NpgsqlCommand(
                      sql "SELECT 1 FROM fsm.instance_state_history WHERE entity_id='history-5' FOR UPDATE",
                      holder,
                      transaction
                  )

              lock.ExecuteScalar() |> ignore

              Expect.equal
                  (scalar<int64> "SELECT fsm.purge_belief_history('pg-tests',now(),2)")
                  2L
                  "the batch is a hard bound"

              Expect.equal
                  (scalar<int64> "SELECT count(*) FROM fsm.instance_state_history WHERE entity_id='history-5'")
                  1L
                  "the locked oldest row is skipped"

              Expect.equal
                  (scalar<int64> "SELECT count(*) FROM fsm.instance_state_history")
                  3L
                  "the rest remains for later passes"
          } ]
