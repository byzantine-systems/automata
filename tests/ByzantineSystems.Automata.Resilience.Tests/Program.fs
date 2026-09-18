module ByzantineSystems.Automata.Resilience.Tests.Program

open Expecto

[<EntryPoint>]
let main argv =
    testList
        "ByzantineSystems.Automata.Resilience.Tests"
        [ test "scaffold" { Expect.isTrue true "project compiles and runs" } ]
    |> runTestsWithCLIArgs [] argv
