module ByzantineSystems.Automata.DependencyInjection.Tests.Program

open Expecto

[<Tests>]
let tests =
    testList
        "ByzantineSystems.Automata.DependencyInjection.Tests"
        [ RegistrationTests.registrationTests
          ActionDispatchTests.actionDispatchTests
          SupervisionAuditTests.supervisionAuditTests
          ValidationTests.validationTests
          LifecycleTests.lifecycleTests
          MaintenanceServiceTests.tests ]

[<EntryPoint>]
let main argv = runTestsWithCLIArgs [] argv tests
