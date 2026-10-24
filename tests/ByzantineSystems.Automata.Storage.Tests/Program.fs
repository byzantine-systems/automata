module ByzantineSystems.Automata.Storage.Tests.Program

open Expecto

[<EntryPoint>]
let main argv =
    runTestsWithCLIArgs [] argv (testList "storage" [ StoreProgramTests.tests; EnumCodecTests.tests ])
