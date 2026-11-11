module ByzantineSystems.Automata.Runtime.Tests.DraftTests

open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Runtime
open ByzantineSystems.Automata.Runtime.Tests.TestSupport
open ByzantineSystems.Automata.Storage
open Expecto

type private DraftInput =
    { Machine: int
      Entity: int
      FromState: TestState
      Event: TestEvent
      EffectiveOffset: int
      HandledBy: int
      Exited: int list
      Entered: int list
      Actions: TestAction list
      Next: TestState }

let private generatedStateId prefix value = stateId $"%s{prefix}-%d{value}"

let tests =
    testList
        "draft"
        [ Property.property "a draft is the exact projection of its resolution" (fun input ->
              let machine = machineId $"machine-%d{input.Machine}"
              let entity: EntityId<TestEntity> = entityId $"entity-%d{input.Entity}"
              let effectiveAt = startTime.AddSeconds(float input.EffectiveOffset)

              let resolution =
                  { HandledBy = generatedStateId "handled" input.HandledBy
                    Exited = input.Exited |> List.map (generatedStateId "exited")
                    Entered = input.Entered |> List.map (generatedStateId "entered")
                    Actions = input.Actions
                    Next = input.Next }

              let expected: TransitionDraft<EntityId<TestEntity>, TestState, TestEvent, TestAction> =
                  { MachineId = machine
                    EntityId = entity
                    Event = input.Event
                    Actions = resolution.Actions
                    FromState = input.FromState
                    ToState = resolution.Next
                    HandledBy = resolution.HandledBy
                    Exited = resolution.Exited
                    Entered = resolution.Entered
                    Status =
                      if Chart.isTerminal testChart resolution.Next then
                          InstanceStatus.Terminated
                      else
                          InstanceStatus.Running
                    EffectiveAt = effectiveAt }

              Draft.ofResolution machine entity testChart input.FromState input.Event effectiveAt resolution = expected) ]
