module ByzantineSystems.Automata.Core.Tests.Program

open Expecto

[<Tests>]
let tests =
    testList
        "ByzantineSystems.Automata.Core"
        [ CoreTypeTests.tests
          ChartTests.tests
          ChartPropertyTests.tests
          ChartFingerprintTests.tests
          FragmentTests.tests
          FragmentTests.targetTests ]

[<EntryPoint>]
let main argv = runTestsWithCLIArgs [] argv tests
