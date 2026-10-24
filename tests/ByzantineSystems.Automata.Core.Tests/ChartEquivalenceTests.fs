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
          } ]
