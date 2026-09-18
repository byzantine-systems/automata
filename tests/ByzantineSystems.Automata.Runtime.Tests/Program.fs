module ByzantineSystems.Automata.Runtime.Tests.Program

open Expecto

[<EntryPoint>]
let main argv =
    testList
        "ByzantineSystems.Automata.Runtime.Tests"
        [ test "scaffold" { Expect.isTrue true "project compiles and runs" } ]
    |> runTestsWithCLIArgs [] argv
