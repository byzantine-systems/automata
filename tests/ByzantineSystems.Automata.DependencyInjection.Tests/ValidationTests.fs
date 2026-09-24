module ByzantineSystems.Automata.DependencyInjection.Tests.ValidationTests

open System
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Runtime
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.DependencyInjection
open Expecto
open Microsoft.Extensions.DependencyInjection
open TestSupport

let validationTests =
    testList
        "configuration validation"
        [ test "invalid supervisor settings fail host startup deterministically" {
              // There is no retry configuration to validate here any more. Short-horizon retry
              // moved to the store, where the driver exceptions it retries still exist, and
              // durable retry belongs to the inbox. What the host still owns is supervision.
              let store = TestStore()
              let audit = InMemorySupervisionStore()

              let options =
                  { testOptions "bad-supervisor-machine" store ignore with
                      Supervisor =
                          { supervisorDefaults with
                              Intensity = 0 } }

              let provider =
                  ServiceCollection()
                      .AddSingleton<ISupervisionEventStore>(audit)
                      .AddAutomata(options)
                      .BuildServiceProvider()

              let hosted, _ = resolveHostedService provider

              Expect.throwsC (fun () -> hosted.StartAsync(noCancellation).GetAwaiter().GetResult()) (fun error ->
                  Expect.isTrue (error :? InvalidOperationException) "the failure is an InvalidOperationException"

                  Expect.stringContains
                      (error.Message.ToLowerInvariant())
                      "supervisor"
                      "the failure names the supervisor configuration")
          }

          test "a machine that will not configure fails the generation" {
              let store = TestStore()
              let audit = InMemorySupervisionStore()

              let options =
                  { testOptions "bad-machine" store ignore with
                      MachineFactory = fun _ -> Error [ MissingStore ] }

              let provider =
                  ServiceCollection()
                      .AddSingleton<ISupervisionEventStore>(audit)
                      .AddAutomata(options)
                      .BuildServiceProvider()

              let hosted, _ = resolveHostedService provider

              Expect.throws
                  (fun () -> hosted.StartAsync(noCancellation).GetAwaiter().GetResult())
                  "a generation that cannot be configured must not start"
          } ]
