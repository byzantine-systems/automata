module ByzantineSystems.Automata.DependencyInjection.Tests.Program

open Expecto

[<EntryPoint>]
let main argv =
    testList
        "ByzantineSystems.Automata.DependencyInjection.Tests"
        [ RegistrationTests.registrationTests
          ActionDispatchTests.actionDispatchTests
          SupervisionAuditTests.supervisionAuditTests
          ValidationTests.validationTests
          LifecycleTests.lifecycleTests ]
    |> runTestsWithCLIArgs [] argv
