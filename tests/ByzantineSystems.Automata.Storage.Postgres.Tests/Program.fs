module ByzantineSystems.Automata.Storage.Postgres.Tests.Program

open Expecto

let private configured =
    System.Environment.GetEnvironmentVariable("AUTOMATA_TEST_DB")
    |> System.String.IsNullOrWhiteSpace
    |> not

[<Tests>]
let tests =
    if configured then
        testList
            "ByzantineSystems.Automata.Storage.Postgres.Tests"
            [ PostgresStoreTests.tests
              PostgresRetryQueueTests.tests
              PostgresOutboxTests.tests
              PostgresDeadLetterTests.tests
              PostgresSupervisionStoreTests.tests
              PostgresAcceptanceTests.tests ]
        |> testSequenced
    else
        ptestCase "AUTOMATA_TEST_DB is not set" (fun _ -> ())

[<EntryPoint>]
let main argv = runTestsWithCLIArgs [] argv tests
