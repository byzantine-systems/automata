module ByzantineSystems.Automata.Storage.Tests.Program

open Expecto

[<EntryPoint>]
let main argv =
    runTestsWithCLIArgs
        []
        argv
        (testList
            "storage"
            [ ArgumentTests.tests
              TapShapeTests.tests
              StoreProgramTests.tests
              EnumCodecTests.tests
              FailureTests.tests ])
