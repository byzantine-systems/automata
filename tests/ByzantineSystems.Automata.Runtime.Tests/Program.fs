module ByzantineSystems.Automata.Runtime.Tests.Program

open Expecto

[<Tests>]
let tests =
    testList
        "ByzantineSystems.Automata.Runtime.Tests"
        [ DraftTests.tests
          DispositionTests.tests
          ProcessorTests.tests
          MachineTests.tests
          WorkSignalsTests.tests ]

[<EntryPoint>]
let main argv = runTestsWithCLIArgs [] argv tests
