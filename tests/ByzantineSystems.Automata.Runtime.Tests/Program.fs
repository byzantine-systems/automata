module ByzantineSystems.Automata.Runtime.Tests.Program

open Expecto

[<Tests>]
let tests =
    testList
        "ByzantineSystems.Automata.Runtime.Tests"
        [ InMemoryStoreTests.tests
          MachineOutcomeTests.tests
          RuntimeRefactorTests.tests
          ParallelismTests.tests
          WorkSignalsTests.tests
          WorkerTests.tests ]

[<EntryPoint>]
let main argv = runTestsWithCLIArgs [] argv tests
