module ByzantineSystems.Automata.Core.Tests.Program

open Expecto

[<Tests>]
let tests =
    testList
        "ByzantineSystems.Automata.Core"
        [ ConditionPropertyTests.tests
          CoreTypeTests.tests
          ChartTests.tests
          ChartPropertyTests.tests
          ResolvePropertyTests.tests
          HierarchyPropertyTests.tests
          ChartFingerprintTests.tests
          ChartEquivalenceTests.tests
          FragmentTests.tests
          FragmentTests.targetTests
          FragmentPropertyTests.tests
          RulePropertyTests.tests ]

[<EntryPoint>]
let main argv = runTestsWithCLIArgs [] argv tests
