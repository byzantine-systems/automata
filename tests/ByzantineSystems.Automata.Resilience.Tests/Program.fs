module ByzantineSystems.Automata.Resilience.Tests.Program

open Expecto

[<EntryPoint>]
let main argv =
    testList "ByzantineSystems.Automata.Resilience.Tests" [ ResiliencePipelineTests.tests; SupervisorTests.tests ]
    |> runTestsWithCLIArgs [] argv
