module ByzantineSystems.Automata.DependencyInjection.Tests.LifecycleTests

open ByzantineSystems.Automata.Storage

open ByzantineSystems.Automata.DependencyInjection
open Expecto
open Microsoft.Extensions.DependencyInjection
open TestSupport

let lifecycleTests =
    testList
        "lifecycle"
        [ testTask "stopping the host cancels the execute task without faulting" {
              let store = TestStore()
              let audit = InMemorySupervisionStore()
              let options = testOptions "lifecycle-machine" store ignore

              let provider =
                  ServiceCollection()
                      .AddSingleton<ISupervisionEventStore>(audit)
                      .AddAutomata(options)
                      .BuildServiceProvider()

              let hosted, background = resolveHostedService provider
              do! hosted.StartAsync(noCancellation)

              do!
                  pollUntil "the Started audit record" (fun () ->
                      audit.Recorded() |> List.exists (fun r -> r.Kind = SupervisionAuditKind.Started))

              do! hosted.StopAsync(noCancellation)

              Expect.isTrue background.ExecuteTask.IsCompleted "the execute task finished"
              Expect.isTrue background.ExecuteTask.IsCanceled "cancellation is the observed stop signal"
              Expect.isFalse background.ExecuteTask.IsFaulted "a normal stop never faults"
          } ]
