module ByzantineSystems.Automata.Runtime.Tests.ParallelismTests

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Resilience
open ByzantineSystems.Automata.Runtime
open ByzantineSystems.Automata.Storage
open Expecto
open TestSupport

let private noCancellation = CancellationToken.None

let tests =
    testList
        "entity parallelism"
        [ testTask "different entities commit concurrently" {
              let time = newTime ()
              let inner = TestStore(time)
              let tracking = TrackingStore(inner :> MachineStore)

              let machine =
                  buildMachine (RetryConfig.defaults<string>.Classify) (tracking :> MachineStore) time

              tracking.EnableGate()

              let sendA =
                  Machine.send machine (entityId "PAR-A") (EventEnvelope.create "a-1" (Start 1)) noCancellation

              do! waitForEntries tracking.EntrySignals 1 noCancellation

              let sendB =
                  Machine.send machine (entityId "PAR-B") (EventEnvelope.create "b-1" (Start 1)) noCancellation

              do! waitForEntries tracking.EntrySignals 1 noCancellation

              Expect.equal 2 tracking.InFlight "both commits are in flight at once"

              tracking.Release 2 |> ignore

              let! results = Task.WhenAll(sendA, sendB)

              Expect.isTrue
                  (results
                   |> Array.forall (function
                       | Ok _ -> true
                       | Error _ -> false))
                  "both entities commit"
          }

          testTask "same-entity events serialize and preserve order" {
              let time = newTime ()
              let inner = TestStore(time)
              let tracking = TrackingStore(inner :> MachineStore)

              let machine =
                  buildMachine (RetryConfig.defaults<string>.Classify) (tracking :> MachineStore) time

              tracking.EnableGate()
              let entity = entityId "PAR-SER"

              let send1 =
                  Machine.send machine entity (EventEnvelope.create "s-1" (Start 1)) noCancellation

              do! waitForEntries tracking.EntrySignals 1 noCancellation
              Expect.equal 1 tracking.InFlight "the first event entered commit"

              let send2 =
                  Machine.send machine entity (EventEnvelope.create "s-2" Finish) noCancellation

              // Release the first commit; the second may only begin afterwards.
              tracking.Release 1 |> ignore
              let! _ = send1
              do! waitForEntries tracking.EntrySignals 1 noCancellation

              Expect.equal 1 tracking.MaxInFlight "commits for one entity never overlap"

              tracking.Release 1 |> ignore
              let! _ = send2

              let! snapshot = Machine.state machine entity noCancellation |> mapTask (fun r -> r)

              match snapshot with
              | Ok(Some s) ->
                  Expect.equal Done s.State "the second event applied last"
                  Expect.equal 2UL (Epoch.value s.Epoch) "two gapless commits"
              | other -> failtestf "expected a snapshot, got %A" other
          }

          testTask "concurrent first sends start exactly one actor" {
              let time = newTime ()
              let inner = TestStore(time)

              let machine =
                  buildMachine (RetryConfig.defaults<string>.Classify) (inner :> MachineStore) time

              let entity = entityId "PAR-ONE"

              let sends =
                  [ for i in 1..8 do
                        yield Machine.send machine entity (EventEnvelope.create $"key-%d{i}" (Start i)) noCancellation ]

              let! _ = Task.WhenAll(sends)

              Expect.equal 1 machine.RegistryValue.ActorCount "one actor serves the entity"
          }

          testTask "idle eviction removes only the idle actor and the entity can be recreated" {
              let time = newTime ()
              let inner = TestStore(time)

              let machine =
                  buildMachine (RetryConfig.defaults<string>.Classify) (inner :> MachineStore) time

              let entity = entityId "PAR-EVICT"

              let! _ =
                  Machine.send machine entity (EventEnvelope.create "e-1" (Start 1)) noCancellation
                  |> mapTask (fun _ -> ())

              Expect.equal 1 machine.RegistryValue.ActorCount "the entity has one actor"

              time.Advance(TimeSpan.FromMinutes 10.)

              machine.RegistryValue.TryEvictIdle entity

              Expect.equal 0 machine.RegistryValue.ActorCount "the idle actor is evicted"

              let! outcome =
                  Machine.send machine entity (EventEnvelope.create "e-2" Finish) noCancellation
                  |> mapTask (fun r ->
                      match r with
                      | Ok _ -> ()
                      | Error e -> failtestf "recreated send failed: %A" e)

              Expect.equal 1 machine.RegistryValue.ActorCount "a fresh actor is created"
          } ]
