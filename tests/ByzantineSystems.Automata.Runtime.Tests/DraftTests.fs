module ByzantineSystems.Automata.Runtime.Tests.DraftTests

open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Runtime
open ByzantineSystems.Automata.Runtime.Tests.TestSupport
open Expecto

/// The one pure step the processor performs, and the only piece of the command path that can be
/// tested without a store at all.
let private draftFor state event =
    match Chart.resolve testChart state event with
    | Error error -> failtestf "the test chart should have resolved this, but reported %A" error
    | Ok resolution -> Draft.ofResolution testMachine (entityId "E-1") testChart state event startTime resolution

let tests =
    testList
        "draft"
        [ test "a draft carries the resolution and the state it came from" {
              let draft = draftFor Idle (Start 5)

              Expect.equal Idle draft.FromState "the state resolved against"
              Expect.equal (Active 5) draft.ToState "the state the rule produced"
              Expect.equal [ Log "started" ] draft.Actions "the actions the rule emitted"
              Expect.equal (stateId "idle") draft.HandledBy "the node that handled the event"
          }
          test "a non-terminal target leaves the instance running" {
              Expect.equal InstanceStatus.Running (draftFor Idle (Start 5)).Status "still accepting commands"
          }
          test "a terminal target ends the instance" {
              // Asked of the chart rather than tracked separately, so the lifecycle cannot drift
              // from the structure that defines it.
              Expect.equal InstanceStatus.Terminated (draftFor (Active 5) Finish).Status "no further commands"
          }
          test "business time is the caller's, never a clock" {
              Expect.equal startTime (draftFor Idle (Start 5)).EffectiveAt "the supplied instant is used verbatim"
          }
          test "the same inputs always produce the same draft" {
              // Purity is what lets a correction replay a command and get the same answer.
              Expect.equal (draftFor Idle (Start 5)) (draftFor Idle (Start 5)) "no hidden state"
          } ]
