module ByzantineSystems.Automata.Runtime.Tests.Program

open Expecto

[<EntryPoint>]
let main argv =
    testList
        "ByzantineSystems.Automata.Runtime.Tests"
        [ InMemoryStoreTests.tests
          MachineOutcomeTests.tests
          RuntimeRefactorTests.tests
          ParallelismTests.tests
          WorkSignalsTests.tests
          WorkerTests.tests ]
    |> runTestsWithCLIArgs [] argv
