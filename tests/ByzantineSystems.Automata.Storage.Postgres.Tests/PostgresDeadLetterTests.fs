module ByzantineSystems.Automata.Storage.Postgres.Tests.PostgresDeadLetterTests

open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open Expecto
open TestContext

let tests =
    testList
        "Postgres IDeadLetterStore"
        [ testTask "record appends an inspectable dead letter" {
              do! reset ()
              let time = newTime ()
              let store = newStore time
              let d = store :> IDeadLetterStore<Entity, TestEvent>
              let entity = entityId "ORD-13"

              let letter: DeadLetter<Entity, TestEvent> =
                  { MachineId = machine
                    EntityId = entity
                    IdempotencyKey = "pay-1"
                    Event = Start 1
                    FinalError = "max attempts exceeded"
                    Attempts = 5
                    DiedAt = startTime }

              do! d.Record(letter, noCancellation) |> mapTask (expectOkUnit "record")

              // A second record with the same key is also allowed (append-only, no uniqueness).
              do! d.Record(letter, noCancellation) |> mapTask (expectOkUnit "record again")
          } ]
