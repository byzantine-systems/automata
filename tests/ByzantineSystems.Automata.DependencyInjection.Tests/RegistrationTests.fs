module ByzantineSystems.Automata.DependencyInjection.Tests.RegistrationTests

open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Runtime
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.DependencyInjection
open Expecto
open Microsoft.Extensions.DependencyInjection
open TestSupport

let registrationTests =
    testList
        "registration"
        [ testTask "the machine factory runs once per supervised generation" {
              let store = TestStore()
              let audit = InMemorySupervisionStore()

              let built =
                  TaskCompletionSource<TestMachine>(TaskCreationOptions.RunContinuationsAsynchronously)

              let options =
                  testOptions "factory-machine" store (fun machine -> built.TrySetResult machine |> ignore)

              let provider =
                  ServiceCollection()
                      .AddSingleton<ISupervisionEventStore>(audit)
                      .AddAutomata(options)
                      .BuildServiceProvider()

              let hosted, _ = resolveHostedService provider
              do! hosted.StartAsync(noCancellation)

              let! machine = built.Task.WaitAsync(waitTimeout)
              Expect.equal (machineId "di-tests") (Machine.machineId machine) "the machine the factory produced"

              do! hosted.StopAsync(noCancellation)
          }

          testTask "AddAutomataMachine wraps a factory that cannot fail" {
              let store = TestStore()
              let audit = InMemorySupervisionStore()
              let options = testOptions "total-factory-machine" store ignore

              let provider =
                  ServiceCollection()
                      .AddSingleton<ISupervisionEventStore>(audit)
                      .AddAutomataMachine(options, fun _ -> expectMachine (testMachineOver (store :> MachineStore)))
                      .BuildServiceProvider()

              let hosted, _ = resolveHostedService provider
              do! hosted.StartAsync(noCancellation)

              do!
                  pollUntil "the Started audit record" (fun () ->
                      audit.Recorded() |> List.exists (fun r -> r.Kind = SupervisionAuditKind.Started))

              do! hosted.StopAsync(noCancellation)
          }

          test "an empty machine key is refused at registration" {
              let store = TestStore()

              Expect.throws
                  (fun () -> ServiceCollection().AddAutomata(testOptions "" store ignore) |> ignore)
                  "a machine key is what the registration is identified by"
          } ]
