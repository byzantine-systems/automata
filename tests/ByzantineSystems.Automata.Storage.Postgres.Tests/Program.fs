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
        [ SchemaNameTests.tests
          PostgresStoreSettingsTests.tests
          SqlResourceTests.tests
          ListenerTests.tests
          MaintenanceTests.pureTests
          MaintenanceTests.migratorTests

          if TestContext.configured then
              testList
                  "integration"
                  [ SqlTests.tests
                    CommandInboxTests.tests
                    ConformanceTests.tests
                    ChartRegistryTests.tests
                    TemporalTests.tests
                    TemporalReaderTests.tests
                    CorrectionTests.tests
                    TemporalProperties.tests
                    FinalizeTests.tests
                    ActionQueueTests.tests
                    ProcessorIntegrationTests.tests
                    ReplayIntegrationTests.tests
                    ReplayRetentionProperties.tests
                    ReplayFinalizeProperties.tests
                    SchemaContractTests.tests
                    MaintenanceTests.integration
                    PostgresSupervisionStoreTests.tests
                    TapTests.tests
                    TwoSchemaTests.tests ]
              |> testSequenced
          else
              ptestCase "AUTOMATA_TEST_DB is not set" (fun _ -> ()) ]

[<EntryPoint>]
let main argv = runTestsWithCLIArgs [] argv tests
