module ByzantineSystems.Automata.Storage.Sqlite.Tests.Program

open Expecto

/// Nothing here is gated. An embedded database needs no service, so every suite runs wherever
/// the unit suites do, and each creates its files under one temporary directory.
[<Tests>]
let tests =
    testList
        "ByzantineSystems.Automata.Storage.Sqlite.Tests"
        [ SqlResourceTests.tests
          DbTests.tests
          MigrationTests.tests
          SchemaContractTests.tests
          CommandInboxTests.tests
          FinalizeTests.tests
          ActionQueueTests.tests
          HistoryTests.tests
          ChartRegistryTests.tests
          BootTests.tests
          ProcessorIntegrationTests.tests
          CrossProcessTests.tests ]

/// The binary is also the second process of the cross-process suite: started with the worker
/// flag, it claims commands instead of running tests.
[<EntryPoint>]
let main argv =
    match argv with
    | [| CrossProcessTests.WorkerFlag; db; go |] -> CrossProcessTests.runWorker db go
    | _ ->
        try
            runTestsWithCLIArgs [] argv tests
        finally
            TestContext.cleanup ()
