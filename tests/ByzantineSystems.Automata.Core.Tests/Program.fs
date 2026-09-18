module ByzantineSystems.Automata.Core.Tests.Program

open Expecto

[<EntryPoint>]
let main argv =
    testList "ByzantineSystems.Automata.Core" [ CoreTypeTests.tests; ChartTests.tests; ChartPropertyTests.tests ]
    |> runTestsWithCLIArgs [] argv
