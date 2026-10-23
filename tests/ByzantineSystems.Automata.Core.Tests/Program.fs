module ByzantineSystems.Automata.Core.Tests.Program

open Expecto

[<Tests>]
let tests =
    testList
        "ByzantineSystems.Automata.Core"
        [ CoreTypeTests.tests
          ChartTests.tests
          ChartPropertyTests.tests
          ResolvePropertyTests.tests
          HierarchyPropertyTests.tests
          ChartFingerprintTests.tests
          FragmentTests.tests
          FragmentTests.targetTests
          RulePropertyTests.tests ]

[<EntryPoint>]
let main argv = runTestsWithCLIArgs [] argv tests
