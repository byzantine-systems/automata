module ByzantineSystems.Automata.Resilience.Tests.Program

open Expecto

[<Tests>]
let tests =
    testList "ByzantineSystems.Automata.Resilience.Tests" [ ResiliencePipelineTests.tests; SupervisorTests.tests ]

[<EntryPoint>]
let main argv = runTestsWithCLIArgs [] argv tests
