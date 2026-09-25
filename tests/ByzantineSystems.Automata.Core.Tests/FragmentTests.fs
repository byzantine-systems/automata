module ByzantineSystems.Automata.Core.Tests.FragmentTests

open System
open ByzantineSystems.Automata.Core
open Expecto

/// An order paid by card first and by bank transfer if the card gives up. Both payment methods
/// behave the same way, which is exactly when a chart wants one fragment used twice.
type Step =
    | Start
    | CardWaiting
    | CardFailed
    | BankWaiting
    | BankFailed
    | Done

type Signal =
    | Go
    | Fail
    | Retry
    | Finish
    | GiveUp

/// Waiting for a payment, failing, retrying. It sends a retry back to its own compound, which
/// the prefix has to follow, and gives up to a state its host declares, which the prefix has to
/// leave alone.
let private attempt (waiting: Step) (failed: Step) : NodeDraft<Step, Signal, string, string> =
    compound "attempt" {
        initial "waiting"

        state "waiting" {
            on (fun _ e -> e = Fail) (fun _ _ -> [], failed)
            on (fun _ e -> e = Finish) (fun _ _ -> [ "paid" ], Done)
        }

        state "failed" {
            goto (fun _ e -> e = Retry) (stateId "attempt") (fun _ _ -> [], waiting)

            NodeRule(
                Rule.guarded
                    (fun _ _ -> true)
                    "may give up"
                    (Rule.goto (fun _ e -> e = GiveUp) (stateId "done") (fun _ _ -> [], Done))
            )
        }
    }

let private classifyStep step =
    match step with
    | Start -> stateId "start"
    | CardWaiting -> stateId "card.waiting"
    | CardFailed -> stateId "card.failed"
    | BankWaiting -> stateId "bank.waiting"
    | BankFailed -> stateId "bank.failed"
    | Done -> stateId "done"

let private order =
    statechart<Step, Signal, string, string> {
        root "order"
        classify classifyStep

        state "start" { goto (fun _ e -> e = Go) (stateId "card.attempt") (fun _ _ -> [], CardWaiting) }

        yield!
            [ Fragment.prefix "card" (attempt CardWaiting CardFailed)
              Fragment.prefix "bank" (attempt BankWaiting BankFailed) ]

        state "done" { terminal }
    }

let private expectChart result =
    match result with
    | Ok chart -> chart
    | Error errors -> failtestf "the chart should have built, but reported %A" errors

let private next chart state signal =
    match Chart.resolve chart state signal with
    | Ok resolution -> resolution.Next
    | Error error -> failtestf "%A on %A should have resolved, but failed with %A" signal state error

let private ids (draft: NodeDraft<'S, 'E, 'A, 'Err>) =
    let rec walk (node: NodeDraft<'S, 'E, 'A, 'Err>) =
        StateId.value node.Id :: List.collect walk node.Children

    walk draft

let private node (name: string) (draft: NodeDraft<'S, 'E, 'A, 'Err>) =
    let rec find (current: NodeDraft<'S, 'E, 'A, 'Err>) =
        if StateId.value current.Id = name then
            Some current
        else
            List.tryPick find current.Children

    find draft
    |> Option.defaultWith (fun () -> failtestf "no node %s in the fragment" name)

let tests =
    testList
        "fragments"
        [ test "one fragment value yields into two charts" {
              let fragment = attempt CardWaiting CardFailed

              let chartWith name =
                  statechart<Step, Signal, string, string> {
                      root name
                      classify (fun _ -> stateId "waiting")
                      fragment
                      state "done" { terminal }
                  }

              expectChart (chartWith "first") |> ignore
              expectChart (chartWith "second") |> ignore
          }

          test "yield! splices every node of a list" {
              let chart = expectChart order

              for name in [ "card.attempt"; "bank.attempt"; "card.waiting"; "bank.failed" ] do
                  Expect.isSome (Chart.tryNode chart (stateId name)) $"{name} was spliced in"
          }

          test "a list splices into a compound too" {
              let chart =
                  statechart<Step, Signal, string, string> {
                      root "order"
                      classify (fun _ -> stateId "a")

                      compound "outer" {
                          initial "a"

                          yield!
                              [ state "a" { on (fun _ e -> e = Go) (fun _ _ -> [], Done) }
                                state "b" { terminal } ]
                      }
                  }
                  |> expectChart

              Expect.equal
                  (Chart.children chart (stateId "outer") |> List.map StateId.value |> List.sort)
                  [ "a"; "b" ]
                  "both children belong to the compound"
          }

          test "prefix renames every node, the initial child and nested children" {
              let prefixed = Fragment.prefix "card" (attempt CardWaiting CardFailed)

              Expect.equal (ids prefixed) [ "card.attempt"; "card.waiting"; "card.failed" ] "every id"

              Expect.equal
                  (prefixed.InitialChild |> Option.map StateId.value)
                  (Some "card.waiting")
                  "the initial child follows the node it names"
          }

          test "a goto inside the fragment follows it, and one to the host does not" {
              let failed =
                  Fragment.prefix "card" (attempt CardWaiting CardFailed) |> node "card.failed"

              Expect.equal
                  (failed.Rules |> List.map (Rule.target >> Option.map StateId.value))
                  [ Some "card.attempt"; Some "done" ]
                  "the retry is renamed; giving up still reaches the host's state, through its guard"
          }

          test "two prefixed copies of one fragment coexist in one chart" { expectChart order |> ignore }

          test "a prefixed chart resolves end to end" {
              // The retry reaches card.attempt through the rewritten goto, then descends to its
              // renamed initial child. The classifier returns prefixed ids, as it has to.
              let chart = expectChart order
              let started = next chart Start Go
              Expect.equal started CardWaiting "into the card attempt"

              let failed = next chart started Fail
              Expect.equal failed CardFailed "the card failed"

              Expect.equal (next chart failed Retry) CardWaiting "and the retry went back into the card attempt"
              Expect.equal (next chart failed GiveUp) Done "while giving up still leaves the fragment"
              Expect.equal (next chart BankFailed Retry) BankWaiting "the bank copy has its own retry"
          }

          test "a prefixed fragment fingerprints differently from the unprefixed one" {
              let chartOf (fragment: NodeDraft<Step, Signal, string, string>) =
                  statechart<Step, Signal, string, string> {
                      root "order"
                      classify classifyStep
                      fragment
                      state "done" { terminal }
                  }
                  |> expectChart

              Expect.notEqual
                  (Chart.fingerprint (chartOf (attempt CardWaiting CardFailed)))
                  (Chart.fingerprint (chartOf (Fragment.prefix "card" (attempt CardWaiting CardFailed))))
                  "renaming a node is a structural change"
          }

          test "a blank prefix is refused" {
              Expect.throwsT<ArgumentException>
                  (fun () -> Fragment.prefix " " (attempt CardWaiting CardFailed) |> ignore)
                  "a prefix of nothing renames nothing and collides with the original"
          } ]

let targetTests =
    testList
        "goto targets"
        [ test "a goto to a state the chart does not declare fails to build" {
              let result =
                  statechart<Step, Signal, string, string> {
                      root "order"
                      classify (fun _ -> stateId "start")
                      state "start" { goto (fun _ e -> e = Go) (stateId "nowhere") (fun _ _ -> [], Done) }
                  }

              match result with
              | Error errors ->
                  Expect.contains
                      errors
                      (ChartError.UnknownGotoTarget(stateId "start", stateId "nowhere"))
                      "caught at construction rather than on the first event that reaches it"
              | Ok _ -> failtest "a chart with a dangling goto should not build"
          }

          test "a guard does not hide a dangling goto" {
              let result =
                  statechart<Step, Signal, string, string> {
                      root "order"
                      classify (fun _ -> stateId "start")

                      state "start" {
                          NodeRule(
                              Rule.guarded
                                  (fun _ _ -> true)
                                  "open"
                                  (Rule.goto (fun _ _ -> true) (stateId "nowhere") (fun _ _ -> [], Done))
                          )
                      }
                  }

              match result with
              | Error errors ->
                  Expect.contains
                      errors
                      (ChartError.UnknownGotoTarget(stateId "start", stateId "nowhere"))
                      "seen through the guard"
              | Ok _ -> failtest "a guarded dangling goto should not build"
          }

          test "an unprefixed classifier against a prefixed fragment is caught, not trusted" {
              // The limitation Fragment.prefix documents: the classifier is the host's code. When it
              // names an id the chart no longer has, resolution says so.
              let chart =
                  statechart<Step, Signal, string, string> {
                      root "order"

                      classify (function
                          | CardFailed -> stateId "failed"
                          | other -> classifyStep other)

                      state "start" { goto (fun _ e -> e = Go) (stateId "card.attempt") (fun _ _ -> [], CardWaiting) }
                      Fragment.prefix "card" (attempt CardWaiting CardFailed)
                      state "done" { terminal }
                  }
                  |> expectChart

              match Chart.resolve chart CardWaiting Fail with
              | Error(UnknownState state) -> Expect.equal (StateId.value state) "failed" "the id it named"
              | other -> failtestf "expected the stale id to be reported, got %A" other
          } ]
