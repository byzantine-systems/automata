module ByzantineSystems.Automata.DependencyInjection.Tests.RegistrationTests

open System
open System.Threading.Tasks
open ByzantineSystems.Automata.Resilience
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.InMemory
open ByzantineSystems.Automata.DependencyInjection
open Expecto
open Microsoft.Extensions.DependencyInjection
open Polly
open Polly.Registry
open TestSupport

let registrationTests =
    testList
        "registration"
        [ testTask "AddAutomataMachine feeds the keyed provider pipeline to the machine factory" {
              let machineKey = "pipeline-machine"
              let store = TestStore()
              let audit = InMemorySupervisionStore()

              let captured =
                  TaskCompletionSource<ResiliencePipeline<PipelineResult<string>>>(
                      TaskCreationOptions.RunContinuationsAsynchronously
                  )

              let options = testOptions machineKey store ignore

              let provider =
                  ServiceCollection()
                      .AddSingleton<ISupervisionEventStore>(audit)
                      .AddAutomataMachine(
                          options,
                          fun _ pipeline ->
                              captured.TrySetResult pipeline |> ignore
                              expectMachine (testMachineWithPipeline pipeline (store :> MachineStore))
                      )
                      .BuildServiceProvider()

              let hosted, _ = resolveHostedService provider
              do! hosted.StartAsync(noCancellation)

              let! factoryPipeline = captured.Task.WaitAsync(waitTimeout)

              let pipelines = provider.GetRequiredService<ResiliencePipelineProvider<string>>()

              let expected = pipelines.GetPipeline<PipelineResult<string>>(machineKey)

              Expect.isTrue
                  (obj.ReferenceEquals(factoryPipeline, expected))
                  "the factory received the exact keyed pipeline instance"

              Expect.isTrue
                  (obj.ReferenceEquals(pipelines.GetPipeline<PipelineResult<string>>(machineKey), expected))
                  "the keyed pipeline is cached by the provider"

              do! hosted.StopAsync(noCancellation)
          } ]
