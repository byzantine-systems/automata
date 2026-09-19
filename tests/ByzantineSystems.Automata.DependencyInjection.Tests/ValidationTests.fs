module ByzantineSystems.Automata.DependencyInjection.Tests.ValidationTests

open System
open ByzantineSystems.Automata.Resilience
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.InMemory
open ByzantineSystems.Automata.DependencyInjection
open Expecto
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open TestSupport

let validationTests =
    testList
        "configuration validation"
        [ test "an invalid retry configuration fails host startup deterministically" {
              let store = TestStore()
              let audit = InMemorySupervisionStore()

              let options =
                  { testOptions "bad-retry-machine" store ignore with
                      Retry =
                          { RetryConfig.defaults<string> with
                              MaxAttempts = 0 } }

              let provider =
                  ServiceCollection()
                      .AddSingleton<ISupervisionEventStore>(audit)
                      .AddAutomata(options)
                      .BuildServiceProvider()

              // The keyed pipeline builds lazily, so resolving the hosted service validates.
              Expect.throwsC
                  (fun () ->
                      provider.GetServices<IHostedService>()
                      |> Seq.iter (fun hosted -> hosted.StartAsync(noCancellation).GetAwaiter().GetResult()))
                  (fun error ->
                      Expect.isTrue (error :? InvalidOperationException) "the failure is an InvalidOperationException"

                      Expect.stringContains
                          (error.Message.ToLowerInvariant())
                          "retry"
                          "the failure names the retry configuration")
          }

          test "invalid supervisor settings fail host startup deterministically" {
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
          } ]
