module ByzantineSystems.Automata.Runtime.Tests.Program

open Expecto

[<Tests>]
let tests =
    testList
        "ByzantineSystems.Automata.Runtime.Tests"
        [ DraftTests.tests
          DispositionTests.tests
          ProcessorTests.tests
          ProcessorTests.poisonTests
          LoopTests.tests
          ReplayTests.tests
          MachineTests.tests
          WorkSignalsTests.tests ]

[<EntryPoint>]
let main argv =
    if Array.contains "--recurring-stress" argv then
        try
            LoopTests.runRecurringStress ()
            0
        with error ->
            eprintfn "%s" (string error)
            1
    else
        runTestsWithCLIArgs [] argv tests
