module ByzantineSystems.Automata.Storage.Postgres.Tests.IntegrityConcurrencyProperties

open System
open System.Diagnostics
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Postgres.Tests.TestContext
open Expecto
open FsCheck
open Npgsql

/// Observe the actual lock barrier, not a guessed delay. If the contender finishes
/// before reaching it, the guard failed to serialize the two operations.
let private awaitLock (pending: Task<_>) pid ct =
    task {
        use observer = (dataSource ()).OpenConnection()

        use cmd =
            new NpgsqlCommand(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = @pid AND wait_event_type = 'Lock')",
                observer
            )

        cmd.Parameters.AddWithValue("pid", pid) |> ignore
        let timer = Stopwatch.StartNew()
        let mutable blocked = false

        while not blocked
              && not pending.IsCompleted
              && timer.Elapsed < TimeSpan.FromSeconds 5. do
            let! result = cmd.ExecuteScalarAsync ct
            blocked <- unbox<bool> result

            if not blocked then
                do! Task.Delay(5, ct)

        Expect.isTrue blocked "the contender must reach the database lock barrier"
    }

let private race seed scenario =
    task {
        do! reset ()
        let entity: Entity = entityId "integrity-race"
        let! submitted = (newInbox ()).SubmitAsync(submission entity "race" (Start seed), noCancellation)
        let id = submitted |> expectOk "race submission" |> commandIdOf |> CommandId.value

        let header =
            sql
                $"INSERT INTO fsm.command_error(command_id, kind) VALUES ({id}, 'machine'); INSERT INTO fsm.command_error_machine(command_id, reason) VALUES ({id}, 'race-{seed}')"

        let succeed =
            sql $"UPDATE fsm.command SET status = 'succeeded', lease_token = 7 WHERE command_id = {id}"

        let fail =
            sql $"UPDATE fsm.command SET status = 'rejected', lease_token = 7 WHERE command_id = {id}"

        let purge = sql $"DELETE FROM fsm.command WHERE command_id = {id}"

        if scenario = 2 || scenario = 3 then
            arrange header

        let firstSql, secondSql =
            match scenario with
            | 0 -> succeed, header
            | 1 -> header, succeed
            | 2 -> purge, fail
            | 3 -> fail, purge
            | 4 -> purge, header
            | _ -> header, purge

        use timeout = new CancellationTokenSource(TimeSpan.FromSeconds 15.)
        use first = (dataSource ()).OpenConnection()
        use second = (dataSource ()).OpenConnection()
        use transaction = first.BeginTransaction()
        use firstCmd = new NpgsqlCommand(firstSql, first, transaction)
        let! _ = firstCmd.ExecuteNonQueryAsync timeout.Token

        let pending =
            task {
                use cmd = new NpgsqlCommand(secondSql, second)

                try
                    let! changed = cmd.ExecuteNonQueryAsync timeout.Token
                    return Ok changed
                with :? PostgresException as error ->
                    return Error(error.SqlState, error.ConstraintName)
            }
        // Always release the holder even when the barrier assertion fails, and
        // await the contender before disposing its connection.
        let! blocked =
            task {
                try
                    do! awaitLock pending second.ProcessID timeout.Token
                    return true
                with _ ->
                    return false
            }

        do! transaction.CommitAsync timeout.Token
        let! result = pending
        Expect.isTrue blocked "the tested ordering reached its lock barrier"

        match scenario, result with
        | 0, Error("23514", "command_error_live_owner") ->
            Expect.equal (scalar<string> "SELECT status FROM fsm.command") "succeeded" "success wins"
            Expect.equal (scalar<int64> "SELECT count(*) FROM fsm.command_error") 0L "late evidence was refused"
        | 1, Error("23514", "command_success_has_no_error") ->
            Expect.equal (scalar<string> "SELECT status FROM fsm.command") "ready" "failed success rolls back"

            Expect.equal
                (scalar<int64> "SELECT count(*) FROM fsm.command_error_machine")
                1L
                "the prior evidence survives"
        | 2, Ok 0
        | 3, Ok 1
        | 4, Error("23503", _)
        | 5, Ok 1 ->
            Expect.equal (scalar<int64> "SELECT count(*) FROM fsm.command") 0L "the root was purged"
            Expect.equal (scalar<int64> "SELECT count(*) FROM fsm.command_error") 0L "no orphan header"
            Expect.equal (scalar<int64> "SELECT count(*) FROM fsm.command_error_machine") 0L "no orphan child"
        | _ -> failtestf "unexpected concurrent outcome in scenario %d: %A" scenario result
    }

let tests =
    testCase "closure, failure insertion and root purge serialize in both orders" (fun () ->
        Check.One(
            Config.QuickThrowOnFailure.WithMaxTest(6),
            fun (seed: int) ->
                task {
                    for scenario in 0..5 do
                        do! race seed scenario

                    return true
                }
                |> fun work -> work.GetAwaiter().GetResult()
        ))
