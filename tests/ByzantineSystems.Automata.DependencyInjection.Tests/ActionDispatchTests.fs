module ByzantineSystems.Automata.DependencyInjection.Tests.ActionDispatchTests

open System
open System.Collections.Concurrent
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Runtime
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.InMemory
open ByzantineSystems.Automata.DependencyInjection
open Expecto
open Microsoft.Extensions.DependencyInjection
open TestSupport

/// What the handler observed, shared with the test as a singleton.
type HandledLog() =
    let entries = ConcurrentBag<Guid * string>()
    let disposed = ConcurrentBag<Guid>()
    member _.Add(markerId, text) = entries.Add(markerId, text)
    member _.RecordDisposed(markerId) = disposed.Add(markerId)
    member _.Entries = entries |> Seq.toList
    member _.Disposed = disposed |> Seq.toList

/// Scoped marker: one instance per IServiceScope, so handled entries prove scope identity.
type ScopeMarker(log: HandledLog) =
    let id = Guid.NewGuid()
    member _.Id = id

    interface IDisposable with
        member _.Dispose() = log.RecordDisposed(id)

type RecordingHandler(marker: ScopeMarker, log: HandledLog) =
    interface IActionHandler<Entity, TestAction, string> with

        member _.HandleAsync(_key, action, _ct) =
            let (Log text) = action
            log.Add(marker.Id, text)
            Task.FromResult(Result<unit, string>.Ok())

let actionDispatchTests =
    testList
        "action dispatch"
        [ testTask "DispatchActions resolves the handler through one async scope per action" {
              let store = TestStore()
              let audit = InMemorySupervisionStore()

              let machineReady =
                  TaskCompletionSource<TestMachine>(TaskCreationOptions.RunContinuationsAsynchronously)

              let options =
                  { testOptions "dispatch-machine" store (fun m -> machineReady.TrySetResult m |> ignore) with
                      DispatchActions = true }

              let provider =
                  ServiceCollection()
                      .AddSingleton<ISupervisionEventStore>(audit)
                      .AddSingleton<HandledLog>()
                      .AddScoped<ScopeMarker>()
                      .AddScoped<IActionHandler<Entity, TestAction, string>, RecordingHandler>()
                      .AddAutomata(options)
                      .BuildServiceProvider()

              let hosted, background = resolveHostedService provider
              do! hosted.StartAsync(noCancellation)

              let! machine = machineReady.Task.WaitAsync(waitTimeout)
              let log = provider.GetRequiredService<HandledLog>()

              // One committed event enqueues both actions through the transactional outbox.
              let! sent =
                  Machine.send machine (entityId "ORDER-1") (EventEnvelope.create "evt-1" (Start 1)) noCancellation

              Expect.equal sent (Ok SendOutcome.Committed) "the action-producing event committed"

              do!
                  pollUntil "both actions handled or the hosted service completed" (fun () ->
                      log.Entries.Length = 2 || background.ExecuteTask.IsCompleted)

              if background.ExecuteTask.IsCompleted then
                  return failtestf "the hosted service completed before dispatch: %A" background.ExecuteTask.Exception

              do! pollUntil "both scopes disposed" (fun () -> log.Disposed.Length = 2)

              let entries = log.Entries

              Expect.equal
                  (entries |> List.map snd |> List.sort)
                  [ "first"; "second" ]
                  "both enqueued actions were delivered"

              let scopeIds = entries |> List.map fst |> List.distinct

              Expect.equal scopeIds.Length 2 "each action ran in its own IServiceScope"

              Expect.equal
                  (log.Disposed |> List.distinct |> List.sort)
                  (scopeIds |> List.sort)
                  "each async scope was disposed after its item"

              do! hosted.StopAsync(noCancellation)
          } ]
