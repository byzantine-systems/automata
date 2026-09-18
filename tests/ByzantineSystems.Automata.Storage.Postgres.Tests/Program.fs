module ByzantineSystems.Automata.Storage.Postgres.Tests.Program

open Expecto

[<EntryPoint>]
let main argv =
    let configured =
        System.Environment.GetEnvironmentVariable("AUTOMATA_TEST_DB")
        |> System.String.IsNullOrWhiteSpace
        |> not

    if not configured then
        printfn "AUTOMATA_TEST_DB is not set - skipping Postgres integration tests."
        0
    else
        testList
            "ByzantineSystems.Automata.Storage.Postgres.Tests"
            [ test "scaffold" { Expect.isTrue true "project compiles and runs" } ]
        |> runTestsWithCLIArgs [] argv
