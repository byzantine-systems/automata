module ByzantineSystems.Automata.Core.Tests.ChartEquivalenceTests

open ByzantineSystems.Automata.Core
open Expecto

type private State =
    | Waiting
    | Running
    | Finished

type private Event =
    | Start
    | Complete
    | Refuse
    | Ignore

let private stateIdOf =
    function
    | Waiting -> stateId "waiting"
    | Running -> stateId "running"
    | Finished -> stateId "finished"

let private expectChart result =
    match result with
    | Ok chart -> chart
    | Result.Error errors -> failtestf "chart equivalence fixture is invalid: %A" errors

let private chart =
    statechart<State, Event, string, string> {
        root "root"
        classify stateIdOf

        state "waiting" { on (fun _ event -> event = Start) (fun _ _ -> [ "started" ], Running) }

        state "running" {
            on (fun _ event -> event = Complete) (fun _ _ -> [ "completed" ], Finished)
            attempt (fun _ event -> event = Refuse) (fun _ _ -> Result.Error "refused")
        }

        state "finished" { terminal }
    }
    |> expectChart

let private declarationChart order condition action =
    let root = stateId "root"

    let node id terminal rules =
        { Id = stateId id
          Parent = Some root
          InitialChild = None
          Terminal = terminal
          OnEntry = fun _ _ -> []
          OnExit = fun _ _ -> []
          Rules = rules }

    let nodes =
        Map
            [ "root",
              { Id = root
                Parent = None
                InitialChild = Some(stateId "waiting")
                Terminal = false
                OnEntry = fun _ _ -> []
                OnExit = fun _ _ -> []
                Rules = [] }
              "waiting", node "waiting" false [ Rule.transitionWhen condition (fun _ _ -> [ action ], Running) ]
              "running", node "running" false []
              "finished", node "finished" true [] ]

    order
    |> List.map (fun id -> nodes.[id])
    |> Chart.create root stateIdOf
    |> expectChart

let tests =
    testList
        "chart equivalence"
        [ test "trace observes leaves, terminal status, and typed actions" {
              let actual = ChartEquivalence.trace chart Waiting [ Start; Complete ]

              Expect.equal
                  actual
                  (Ok
                      [ { Leaf = stateId "running"
                          Terminal = false
                          Actions = [ "started" ] }
                        { Leaf = stateId "finished"
                          Terminal = true
                          Actions = [ "completed" ] } ])
                  "a trace observes each successful resolution"
          }

          test "trace returns the successful prefix with the first typed failure" {
              let actual = ChartEquivalence.trace chart Waiting [ Start; Refuse; Complete ]

              Expect.equal
                  actual
                  (Result.Error
                      { Steps =
                          [ { Leaf = stateId "running"
                              Terminal = false
                              Actions = [ "started" ] } ]
                        Error = TransitionError.Rejected "refused" })
                  "a failed trace preserves prior observations and its typed error"
          }

          test "equivalentOn compares successful and failed sampled traces" {
              let samples = [ []; [ Start ]; [ Start; Complete ]; [ Start; Refuse ]; [ Ignore ] ]

              let equivalent =
                  ChartEquivalence.equivalentOn (=) (=) samples (chart, Waiting) (chart, Waiting)

              Expect.isTrue equivalent "a chart agrees with itself over successful and failed samples"
          }

          Property.property "equivalentOn agrees over generated event sequences" (fun (samples: int list list) ->
              let eventOf seed =
                  match Property.bounded 4 seed with
                  | 0 -> Start
                  | 1 -> Complete
                  | 2 -> Refuse
                  | _ -> Ignore

              let eventSequences = samples |> List.map (List.map eventOf)

              ChartEquivalence.equivalentOn (=) (=) eventSequences (chart, Waiting) (chart, Waiting))

          test "equivalentOn detects asymmetric failures" {
              let alwaysStart =
                  statechart<State, Event, string, string> {
                      root "root"
                      classify stateIdOf

                      state "waiting" { on (fun _ _ -> true) (fun _ _ -> [ "started" ], Running) }

                      state "running" { }
                      state "finished" { terminal }
                  }
                  |> expectChart

              let equivalent =
                  ChartEquivalence.equivalentOn (=) (=) [ [ Ignore ] ] (chart, Waiting) (alwaysStart, Waiting)

              Expect.isFalse equivalent "a failure on only one side is observable"
          }

          Property.property
              "declaredEquivalent ignores sibling order and normalized condition spelling"
              (fun (reverse: bool) ->
                  let eventStart = Condition.eventIs "event.start" (fun event -> event = Start)
                  let leftCondition = Condition.allOf [ eventStart; Condition.always ]

                  let rightCondition = Condition.allOf [ Condition.always; eventStart ]

                  let order = [ "root"; "waiting"; "running"; "finished" ]
                  let rightOrder = if reverse then List.rev order else order
                  let left = declarationChart order leftCondition "same"
                  let right = declarationChart rightOrder rightCondition "same"
                  ChartEquivalence.declaredEquivalent left right)

          test "declaredEquivalent detects canonical syntax changes" {
              let order = [ "root"; "waiting"; "running"; "finished" ]

              let left =
                  declarationChart order (Condition.eventIs "event.start" (fun event -> event = Start)) "same"

              let right =
                  declarationChart order (Condition.eventIs "event.begin" (fun event -> event = Start)) "same"

              Expect.isFalse
                  (ChartEquivalence.declaredEquivalent left right)
                  "predicate names are part of the declaration"
          }

          test "equal declarations can have different opaque behavior" {
              let order = [ "root"; "waiting"; "running"; "finished" ]
              let condition = Condition.eventIs "event.start" (fun event -> event = Start)
              let left = declarationChart order condition "left"
              let right = declarationChart order condition "right"

              Expect.isTrue (ChartEquivalence.declaredEquivalent left right) "transforms are outside declared syntax"

              Expect.isFalse
                  (ChartEquivalence.equivalentOn (=) (=) [ [ Start ] ] (left, Waiting) (right, Waiting))
                  "typed actions remain behaviorally observable"
          } ]
