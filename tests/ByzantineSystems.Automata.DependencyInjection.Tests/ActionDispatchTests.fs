module ByzantineSystems.Automata.DependencyInjection.Tests.ActionDispatchTests

open System
open System.Collections.Concurrent
open System.Threading.Tasks
open ByzantineSystems.Automata.Storage
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

        /// The whole leased action arrives, not just the payload, so a real handler could hand
        /// its destination the (CommandId, Ordinal) pair and let it recognise a redelivery.
        member _.HandleAsync(action, _ct) =
            let (Log text) = action.Work.Action
            log.Add(marker.Id, text)
            Task.FromResult(Result<unit, string>.Ok())

let actionDispatchTests =
    testList
        "action dispatch"
        [ testTask "DispatchActions resolves the handler through one async scope per action" {
              let store = TestStore()
              let audit = InMemorySupervisionStore()

              let options =
                  { testOptions "dispatch-machine" store ignore with
                      DispatchActions = true }

              let provider =
                  ServiceCollection()
                      .AddSingleton<ISupervisionEventStore>(audit)
                      .AddSingleton<HandledLog>()
                      .AddScoped<ScopeMarker>()
                      .AddScoped<IActionHandler<Entity, TestAction, string>, RecordingHandler>()
                      .AddAutomata(options)
                      .BuildServiceProvider()

              // Two actions waiting in the queue, as a commit would have left them.
              store.OfferAction(leasedAction 0)
              store.OfferAction(leasedAction 1)

              let hosted, background = resolveHostedService provider
              do! hosted.StartAsync(noCancellation)

              let log = provider.GetRequiredService<HandledLog>()

              do!
                  pollUntil "both actions handled or the hosted service completed" (fun () ->
                      log.Entries.Length = 2 || background.ExecuteTask.IsCompleted)

              if background.ExecuteTask.IsCompleted then
                  return failtestf "the hosted service completed before dispatch: %A" background.ExecuteTask.Exception

              do! pollUntil "both scopes disposed" (fun () -> log.Disposed.Length = 2)

              let entries = log.Entries

              Expect.equal
                  (entries |> List.map snd |> List.sort)
                  [ "effect-0"; "effect-1" ]
                  "both queued actions were delivered"

              let scopeIds = entries |> List.map fst |> List.distinct

              Expect.equal scopeIds.Length 2 "each action ran in its own IServiceScope"

              Expect.equal
                  (log.Disposed |> List.distinct |> List.sort)
                  (scopeIds |> List.sort)
                  "each async scope was disposed after its item"

              Expect.equal [ 0; 1 ] (List.ofSeq store.Delivered |> List.sort) "both were completed, and fenced"

              do! hosted.StopAsync(noCancellation)
          }

          testTask "a host that does not dispatch leaves the queue alone" {
              // A deployment may run processors on one fleet and dispatchers on another. The
              // queue is durable, so neither needs the other in the same process.
              let store = TestStore()
              let audit = InMemorySupervisionStore()
              let options = testOptions "no-dispatch-machine" store ignore

              let provider =
                  ServiceCollection()
                      .AddSingleton<ISupervisionEventStore>(audit)
                      .AddAutomata(options)
                      .BuildServiceProvider()

              store.OfferAction(leasedAction 0)

              let hosted, _ = resolveHostedService provider
              do! hosted.StartAsync(noCancellation)
              do! Task.Delay 100
              Expect.isEmpty store.Delivered "nothing was delivered by this host"
              do! hosted.StopAsync(noCancellation)
          } ]
