module ByzantineSystems.Automata.Core.Tests.ChartTests

open System
open ByzantineSystems.Automata.Core
open Expecto

type PaymentState =
    | Idle
    | Processing of amount: decimal * customer: string
    | AwaitingConfirmation of reference: string
    | Captured of reference: string
    | Failed of reason: string
    | Cancelled of reason: string

type PaymentEvent =
    | InitiatePayment of amount: decimal * customer: string
    | GatewayProcessed of reference: string
    | ConfirmationReceived of reference: string
    | GatewayFailed of reason: string
    | Overcharge of amount: decimal
    | Settle
    | BadSettle
    | Restart
    | Refresh
    | Ping
    | Nudge
    | Cancel

type PaymentAction =
    | ReserveGatewaySlot
    | ReleaseGatewaySlot
    | NotifyCustomer of string
    | Log of string

/// The chart from REFACTOR.md §4.3: one Cancel rule on `active` inherited by every descendant.
let private paymentChart =
    statechart<PaymentState, PaymentEvent, PaymentAction, string> {
        root "root"

        classify (function
            | Idle -> stateId "idle"
            | Processing _ -> stateId "active.processing"
            | AwaitingConfirmation _ -> stateId "active.awaiting"
            | Captured _ -> stateId "active.settled.captured"
            | Failed _ -> stateId "failed"
            | Cancelled _ -> stateId "cancelled")

        state "idle" {
            on
                (fun _ e ->
                    match e with
                    | InitiatePayment _ -> true
                    | _ -> false)
                (fun _ e ->
                    match e with
                    | InitiatePayment(amount, customer) -> [], Processing(amount, customer)
                    | _ -> [], Idle)

            goto (fun _ e -> e = Settle) (stateId "active") (fun _ _ -> [], Processing(0m, "seed"))

            goto (fun _ e -> e = BadSettle) (stateId "active") (fun _ _ -> [], Idle)
        }

        compound "active" {
            initial "active.processing"
            onEntry (fun _ _ -> [ ReserveGatewaySlot ])
            onExit (fun _ _ -> [ ReleaseGatewaySlot ])

            // One rule, inherited by every descendant.
            on (fun _ e -> e = Cancel) (fun _ _ -> [ NotifyCustomer "cancelled" ], Cancelled "")

            // Self-transition through an ancestor handler: keep the same leaf.
            on (fun _ e -> e = Refresh) (fun s _ -> [], s)

            // Gateway failure is handled once, on the compound.
            on
                (fun _ e ->
                    match e with
                    | GatewayFailed _ -> true
                    | _ -> false)
                (fun _ e ->
                    match e with
                    | GatewayFailed reason -> [], Failed reason
                    | _ -> [], Idle)

            goto (fun _ e -> e = Restart) (stateId "active") (fun _ _ -> [], Processing(1m, "restarted"))

            state "active.processing" {
                attempt
                    (fun _ e ->
                        match e with
                        | GatewayProcessed _ -> true
                        | _ -> false)
                    (fun _ e ->
                        match e with
                        | GatewayProcessed reference -> Ok([], AwaitingConfirmation reference)
                        | _ -> Error "expected GatewayProcessed")

                attempt
                    (fun _ e ->
                        match e with
                        | Overcharge _ -> true
                        | _ -> false)
                    (fun _ e ->
                        match e with
                        | Overcharge amount -> Error $"overcharge: {amount}"
                        | _ -> Error "expected Overcharge")

                internalOn (fun _ e -> e = Nudge) (fun _ _ -> [ Log "nudged" ])
            }

            state "active.awaiting" {
                on
                    (fun _ e ->
                        match e with
                        | ConfirmationReceived _ -> true
                        | _ -> false)
                    (fun _ e ->
                        match e with
                        | ConfirmationReceived reference -> [], Captured reference
                        | _ -> [], Idle)

                on (fun _ e -> e = Ping) (fun s _ -> [ Log "ping" ], s)
            }

            compound "active.settled" {
                initial "active.settled.captured"
                state "active.settled.captured" { }
            }
        }

        state "failed" { terminal }
        state "cancelled" { terminal }
    }

let private chart =
    match paymentChart with
    | Ok c -> c
    | Result.Error errors -> failtestf "payment chart failed to construct: %A" errors

let private resolveOk state event =
    match Chart.resolve chart state event with
    | Ok resolution -> resolution
    | Result.Error e -> failtestf "expected Ok, got %A" e

let private resolveError state event =
    match Chart.resolve chart state event with
    | Ok r -> failtestf "expected Error, got %A" r
    | Result.Error e -> e

let private constructionTests =
    testList
        "construction"
        [ test "payment chart constructs without errors" {
              Expect.isOk paymentChart "the reference chart is structurally valid"
          }
          test "root is an implicit container with the declared children" {
              Expect.equal (stateId "root") (Chart.root chart) "root id"

              Expect.equal
                  (List.map stateId [ "idle"; "active"; "failed"; "cancelled" ])
                  (Chart.children chart (stateId "root"))
                  "top-level nodes in declaration order"
          }
          test "missing root is a typed construction error" {
              let result = statechart<int, int, int, string> { classify (fun _ -> stateId "a") }

              match result with
              | Result.Error [ ChartError.MissingRoot ] -> ()
              | other -> failtestf "expected [MissingRoot], got %A" other
          }
          test "missing classifier is a typed construction error" {
              let result = statechart<int, int, int, string> { root "root" }

              match result with
              | Result.Error [ ChartError.MissingClassify ] -> ()
              | other -> failtestf "expected [MissingClassify], got %A" other
          } ]

let private mkNode id parent initial terminal rules : Node<int, int, int, string> =
    { Id = stateId id
      Parent = parent |> Option.map stateId
      InitialChild = initial |> Option.map stateId
      Terminal = terminal
      OnEntry = fun _ _ -> []
      OnExit = fun _ _ -> []
      Rules = rules }

let private createFrom nodes =
    // every fixture gets the implicit root container declared, like the CE would
    Chart.create (stateId "root") (fun (_: int) -> stateId "root") (mkNode "root" None None false [] :: nodes)

let private validationTests =
    testList
        "validation"
        [ test "duplicate state ids are rejected" {
              let result =
                  createFrom [ mkNode "a" None None false []; mkNode "a" (Some "root") None false [] ]

              match result with
              | Result.Error errors ->
                  Expect.isTrue
                      (errors
                       |> List.exists (function
                           | DuplicateState _ -> true
                           | _ -> false))
                      "reports DuplicateState"
              | Ok _ -> failtest "expected duplicate detection"
          }
          test "unknown parent is rejected" {
              let result = createFrom [ mkNode "a" (Some "ghost") None false [] ]

              match result with
              | Result.Error errors ->
                  Expect.isTrue
                      (errors
                       |> List.exists (function
                           | UnknownParent(_, p) -> p = stateId "ghost"
                           | _ -> false))
                      "reports UnknownParent"
              | Ok _ -> failtest "expected unknown-parent detection"
          }
          test "parent cycles are rejected with the cycle path" {
              let result =
                  createFrom [ mkNode "a" (Some "b") None false []; mkNode "b" (Some "a") None false [] ]

              match result with
              | Result.Error errors ->
                  Expect.isTrue
                      (errors
                       |> List.exists (function
                           | CyclicHierarchy _ -> true
                           | _ -> false))
                      "reports CyclicHierarchy"
              | Ok _ -> failtest "expected cycle detection"
          }
          test "non-root compound without initial child is rejected" {
              let result =
                  createFrom [ mkNode "a" (Some "root") None false []; mkNode "b" (Some "a") None false [] ]

              match result with
              | Result.Error errors ->
                  Expect.isTrue
                      (errors
                       |> List.exists (function
                           | CompoundWithoutInitial id -> id = stateId "a"
                           | _ -> false))
                      "reports CompoundWithoutInitial"
              | Ok _ -> failtest "expected compound-without-initial detection"
          }
          test "initial child that is not a child is rejected" {
              let result =
                  createFrom
                      [ mkNode "a" (Some "root") (Some "idle") false []
                        mkNode "idle" (Some "root") None false [] ]

              match result with
              | Result.Error errors ->
                  Expect.isTrue
                      (errors
                       |> List.exists (function
                           | InitialNotAChild _ -> true
                           | _ -> false))
                      "reports InitialNotAChild"
              | Ok _ -> failtest "expected initial-not-a-child detection"
          }
          test "unreachable nodes are rejected" {
              let result =
                  createFrom
                      [ mkNode "a" (Some "root") (Some "b") false []
                        mkNode "b" (Some "a") None false []
                        mkNode "orphan" None None false [] ]

              match result with
              | Result.Error errors ->
                  Expect.isTrue
                      (errors
                       |> List.exists (function
                           | UnreachableState id -> id = stateId "orphan"
                           | _ -> false))
                      "reports UnreachableState"
              | Ok _ -> failtest "expected unreachable detection"
          }
          test "terminal nodes may have no rules or children" {
              let withRules =
                  createFrom
                      [ mkNode "a" (Some "root") None true [ Rule.transition (fun _ _ -> true) (fun _ _ -> [], 0) ] ]

              let withChild =
                  createFrom
                      [ mkNode "a" (Some "root") (Some "b") true []
                        mkNode "b" (Some "a") None false [] ]

              match withRules, withChild with
              | Result.Error e1, Result.Error e2 ->
                  Expect.isTrue
                      (e1
                       |> List.exists (function
                           | InvalidTerminal _ -> true
                           | _ -> false))
                      "rules on terminal"

                  Expect.isTrue
                      (e2
                       |> List.exists (function
                           | InvalidTerminal _ -> true
                           | _ -> false))
                      "children on terminal"
              | _ -> failtest "expected InvalidTerminal in both"
          }
          test "root is exempt from requiring an initial child" {
              let result = createFrom [ mkNode "idle" (Some "root") None false [] ]

              Expect.isOk result "root container needs no initial child"
          } ]

let private bubblingTests =
    testList
        "bubbling (the phase 2 acceptance test)"
        [ test "Cancel is honoured from processing" {
              let r = resolveOk (Processing(10m, "C1")) Cancel

              Expect.equal (stateId "active") r.HandledBy "handled once, on the compound"
              Expect.equal [ stateId "active.processing"; stateId "active" ] r.Exited "exits leaf then compound"
              Expect.equal [ stateId "cancelled" ] r.Entered "enters the terminal leaf"
              Expect.equal (Cancelled "") r.Next "next state"
              Expect.contains r.Actions (NotifyCustomer "cancelled") "rule action present"
              Expect.contains r.Actions ReleaseGatewaySlot "compound exit action present"
          }
          test "Cancel is honoured from awaiting" {
              let r = resolveOk (AwaitingConfirmation "R1") Cancel

              Expect.equal (stateId "active") r.HandledBy "same rule"
              Expect.equal [ stateId "active.awaiting"; stateId "active" ] r.Exited ""
              Expect.equal (Cancelled "") r.Next ""
          }
          test "Cancel is honoured from the nested captured leaf" {
              let r = resolveOk (Captured "P1") Cancel

              Expect.equal (stateId "active") r.HandledBy "same rule"

              Expect.equal
                  [ stateId "active.settled.captured"
                    stateId "active.settled"
                    stateId "active" ]
                  r.Exited
                  "whole subtree exits"

              Expect.equal (Cancelled "") r.Next ""
          }
          test "no rule anywhere yields typed Unhandled" {
              match resolveError Idle Cancel with
              | Unhandled(state, event) ->
                  Expect.equal (stateId "idle") state "reports the active leaf"
                  Expect.equal "Cancel" event "reports the event as text"
              | _ -> failtest "expected Unhandled"
          }
          test "a rejecting rule stops bubbling with the domain error" {
              let e = resolveError (Processing(1m, "C")) (Overcharge 5m)

              match e with
              | TransitionError.Rejected err -> Expect.equal "overcharge: 5" err "domain error surfaces"
              | _ -> failtest "expected Rejected"
          } ]

let private transitionTests =
    testList
        "transition paths"
        [ test "entering a compound descends through initial children" {
              let r = resolveOk Idle (InitiatePayment(99.99m, "CUST-001"))

              Expect.equal [ stateId "idle" ] r.Exited "leaf exits"
              Expect.equal [ stateId "active"; stateId "active.processing" ] r.Entered "outermost first"
              Expect.equal [ ReserveGatewaySlot ] r.Actions "entry action of the compound runs"
              Expect.equal (Processing(99.99m, "CUST-001")) r.Next "concrete parameterised state"
          }
          test "sibling transition exits and enters around the LCA" {
              let r = resolveOk (Processing(5m, "C")) (GatewayProcessed "REF")

              Expect.equal [ stateId "active.processing" ] r.Exited ""
              Expect.equal [ stateId "active.awaiting" ] r.Entered ""
              Expect.equal (AwaitingConfirmation "REF") r.Next ""
          }
          test "descending into a nested compound enters outermost first" {
              let r = resolveOk (AwaitingConfirmation "REF") (ConfirmationReceived "PAY")

              Expect.equal [ stateId "active.awaiting" ] r.Exited ""
              Expect.equal [ stateId "active.settled"; stateId "active.settled.captured" ] r.Entered ""
              Expect.equal (Captured "PAY") r.Next ""
          }
          test "handled on the compound, the whole subtree exits innermost first" {
              let r = resolveOk (Captured "P1") (GatewayFailed "timeout")

              Expect.equal (stateId "active") r.HandledBy "rule lives on the compound"

              Expect.equal
                  [ stateId "active.settled.captured"
                    stateId "active.settled"
                    stateId "active" ]
                  r.Exited
                  "innermost first"

              Expect.equal [ stateId "failed" ] r.Entered ""
              Expect.equal [ ReleaseGatewaySlot ] r.Actions "exit actions only, in order"
              Expect.equal (Failed "timeout") r.Next ""
          }
          test "external self-transition on the leaf exits and re-enters it" {
              let r = resolveOk (AwaitingConfirmation "R") Ping

              Expect.equal [ stateId "active.awaiting" ] r.Exited "exits"
              Expect.equal [ stateId "active.awaiting" ] r.Entered "re-enters"
              Expect.equal (AwaitingConfirmation "R") r.Next "state value unchanged"
              Expect.equal [ Log "ping" ] r.Actions ""
          }
          test "self-transition handled above the leaf exits and re-enters the handler chain" {
              let r = resolveOk (Captured "P1") Refresh

              Expect.equal (stateId "active") r.HandledBy ""

              Expect.equal
                  [ stateId "active.settled.captured"
                    stateId "active.settled"
                    stateId "active" ]
                  r.Exited
                  "up to the handler inclusive"

              Expect.equal
                  [ stateId "active"
                    stateId "active.settled"
                    stateId "active.settled.captured" ]
                  r.Entered
                  "back down to the same leaf"

              Expect.equal (Captured "P1") r.Next ""
          }
          test "internal transition performs no exit or entry" {
              let r = resolveOk (Processing(2m, "C")) Nudge

              Expect.equal [] r.Exited "no exits"
              Expect.equal [] r.Entered "no entries"
              Expect.equal [ Log "nudged" ] r.Actions "rule actions only"
              Expect.equal (Processing(2m, "C")) r.Next "state unchanged"
          } ]

let private gotoTests =
    testList
        "goto"
        [ test "goto a compound follows the initial-child path" {
              let r = resolveOk Idle Settle

              Expect.equal [ stateId "idle" ] r.Exited ""
              Expect.equal [ stateId "active"; stateId "active.processing" ] r.Entered "descends through initials"
              Expect.equal (Processing(0m, "seed")) r.Next "concrete state from the rule"
          }
          test "a concrete state that misses the resolved leaf is a TargetMismatch" {
              match resolveError Idle BadSettle with
              | TargetMismatch(expected, actual) ->
                  Expect.equal (stateId "active.processing") expected "resolved leaf"
                  Expect.equal (stateId "idle") actual "what the classifier said"
              | _ -> failtest "expected TargetMismatch"
          }
          test "goto an ancestor exits it inclusive and re-enters down the initial path" {
              let r = resolveOk (Captured "P1") Restart

              Expect.equal
                  [ stateId "active.settled.captured"
                    stateId "active.settled"
                    stateId "active" ]
                  r.Exited
                  "through the target inclusive"

              Expect.equal [ stateId "active"; stateId "active.processing" ] r.Entered "target then initial path"
              Expect.equal (Processing(1m, "restarted")) r.Next ""
          } ]

let private classifierTests =
    testList
        "classifier failures"
        [ let ghostChart =
              Chart.create (stateId "root") (fun (_: int) -> stateId "ghost") [ mkNode "root" None None false [] ]

          test "unknown classifier output is typed" {
              match ghostChart with
              | Ok c ->
                  match Chart.resolve c 0 0 with
                  | Result.Error(UnknownState id) -> Expect.equal (stateId "ghost") id ""
                  | _ -> failtest "expected UnknownState"
              | Result.Error _ -> failtest "chart should construct"
          }

          test "classifier output that is a compound node is rejected" {
              let ok =
                  match resolveError (Processing(1m, "C")) Settle with
                  | UnknownState _ -> true
                  | _ -> false

              Expect.isFalse ok "Settle from processing is not an UnknownState (sanity)"

              let compoundChart =
                  Chart.create
                      (stateId "root")
                      (fun (_: int) -> stateId "active")
                      [ mkNode "root" None (Some "active") false []
                        mkNode "active" (Some "root") (Some "leaf") false []
                        mkNode "leaf" (Some "active") None false [] ]

              match compoundChart with
              | Ok c ->
                  match Chart.resolve c 0 0 with
                  | Result.Error(UnknownState id) -> Expect.equal (stateId "active") id "compound leaf rejected"
                  | _ -> failtest "expected UnknownState for compound target"
              | Result.Error _ -> failtest "chart should construct"
          }

          test "Next to an unknown state is typed" {
              let badChart =
                  Chart.create
                      (stateId "root")
                      (fun (s: int) -> if s = 0 then stateId "root-leaf" else stateId "ghost")
                      [ mkNode "root" None None false []
                        mkNode
                            "root-leaf"
                            (Some "root")
                            None
                            false
                            [ Rule.transition (fun _ e -> e = 1) (fun _ _ -> [], 9) ] ]

              match badChart with
              | Ok c ->
                  match Chart.resolve c 0 1 with
                  | Result.Error(UnknownState id) -> Expect.equal (stateId "ghost") id ""
                  | _ -> failtest "expected UnknownState"
              | Result.Error _ -> failtest "chart should construct"
          } ]

let tests =
    testList
        "chart"
        [ constructionTests
          validationTests
          bubblingTests
          transitionTests
          gotoTests
          classifierTests ]
