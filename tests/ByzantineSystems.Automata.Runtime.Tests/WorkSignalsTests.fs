module ByzantineSystems.Automata.Runtime.Tests.WorkSignalsTests

open System.Threading
open ByzantineSystems.Automata.Runtime
open Expecto

let tests =
    testList
        "work signals"
        [ testTask "TrySignal never blocks and wakes a waiter" {
              let signal = new WorkSignal()
              let ct = CancellationToken.None

              signal.TrySignal()
              let! signalOpen = signal.WaitAsync(ct)
              Expect.isTrue signalOpen "the hint was consumed"
              signal.Complete()
          }

          testTask "a full signal coalesces dropped hints" {
              let signal = new WorkSignal()
              let ct = CancellationToken.None

              signal.TrySignal()
              signal.TrySignal()
              signal.TrySignal()

              let! signalOpen = signal.WaitAsync(ct)
              Expect.isTrue signalOpen "one coalesced hint was consumed"
              signal.Complete()
          }

          testTask "complete releases pending waiters" {
              let signal = new WorkSignal()
              let ct = CancellationToken.None

              let wait = signal.WaitAsync(ct)
              signal.Complete()
              let! signalOpen = wait
              Expect.isFalse signalOpen "completion is distinguishable from a hint"
          } ]
