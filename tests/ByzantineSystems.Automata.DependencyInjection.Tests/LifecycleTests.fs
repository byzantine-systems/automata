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

              // Stopping ends the watch by cancellation, and that is an ending rather than a
              // failure. It used to surface as a caught TaskCanceledException, logged as the
              // service having failed on every ordinary shutdown.
              Expect.isTrue background.ExecuteTask.IsCompletedSuccessfully "an orderly stop completes"
          }

          testTask "a store that refuses to boot starts no worker" {
              // Workers used to start with the generation object, before its boot ran, so a
              // dispatcher could poll a queue the boot had not created yet.
              let store = RefusingStore()
              let audit = InMemorySupervisionStore()

              let options =
                  { testOptions "refusing-machine" store ignore with
                      Supervisor =
                          { supervisorDefaults with
                              Restart = ByzantineSystems.Automata.Resilience.RestartKind.Temporary } }

              let provider =
                  ServiceCollection()
                      .AddSingleton<ISupervisionEventStore>(audit)
                      .AddAutomata(options)
                      .BuildServiceProvider()

              let hosted, _ = resolveHostedService provider

              try
                  do! hosted.StartAsync(noCancellation)
              with _ ->
                  ()

              do! System.Threading.Tasks.Task.Delay 200
              Expect.equal store.Claims 0 "nothing claimed, because nothing started"

              try
                  do! hosted.StopAsync(noCancellation)
              with _ ->
                  ()
          } ]
