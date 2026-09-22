module ByzantineSystems.Automata.Storage.Postgres.Tests.Program

open Expecto

/// The database-backed suites are gated on AUTOMATA_TEST_DB so the project stays runnable
/// without PostgreSQL. SqlResourceTests is not gated: it asserts what this assembly embeds,
/// which is true with or without a database, and a packaging fault should never hide behind a
/// missing environment variable.
[<Tests>]
let tests =
    testList
        "ByzantineSystems.Automata.Storage.Postgres.Tests"
        [ SqlResourceTests.tests

          if TestContext.configured then
              testList
                  "integration"
                  [ CommandInboxTests.tests
                    SchemaContractTests.tests
                    PostgresSupervisionStoreTests.tests ]
              |> testSequenced
          else
              ptestCase "AUTOMATA_TEST_DB is not set" (fun _ -> ()) ]

[<EntryPoint>]
let main argv = runTestsWithCLIArgs [] argv tests
