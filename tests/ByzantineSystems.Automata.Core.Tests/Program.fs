module ByzantineSystems.Automata.Core.Tests.Program

open Expecto

[<EntryPoint>]
let main argv =
    testList
        "ByzantineSystems.Automata.Core.Tests"
        [ test "scaffold" { Expect.isTrue true "project compiles and runs" } ]
    |> runTestsWithCLIArgs [] argv
