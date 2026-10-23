module ByzantineSystems.Automata.Core.Tests.FragmentPropertyTests

open ByzantineSystems.Automata.Core
open Expecto

let private expectChart result =
    match result with
    | Ok chart -> chart
    | Result.Error errors -> failwithf "fragment property fixture is invalid: %A" errors

let private rulePart action : NodePart<int, int, int, string> =
    NodeRule(Rule.transition (fun _ _ -> true) (fun state _ -> [ action ], state))

let private runParts (parts: NodePart<int, int, int, string> list) =
    NodeBuilder<int, int, int, string>("leaf").Run parts

let private observeRules state event (draft: NodeDraft<int, int, int, string>) =
    draft.Rules |> List.map (fun rule -> Rule.invoke rule state event)

let private chartOfParts parts =
    statechart<int, int, int, string> {
        root "root"
        classify (fun _ -> stateId "leaf")
        runParts parts
    }
    |> expectChart

type private PrefixState =
    | Waiting
    | Failed

type private PrefixEvent = Retry of token: int

let private retryFragment: NodeDraft<PrefixState, PrefixEvent, int, string> =
    compound "attempt" {
        initial "waiting"
        state "waiting" { on (fun _ _ -> false) (fun state _ -> [], state) }

        state "failed" {
            goto (fun _ _ -> true) (stateId "attempt") (fun _ event ->
                match event with
                | Retry token -> [ token ], Waiting)
        }
    }

let private prefixedChart (draft: NodeDraft<PrefixState, PrefixEvent, int, string>) =
    statechart<PrefixState, PrefixEvent, int, string> {
        root "root"

        classify (function
            | Waiting -> stateId "a.b.waiting"
            | Failed -> stateId "a.b.failed")

        draft
    }
    |> expectChart

let private emptyLeaf name : NodeDraft<int, int, int, string> = state name { }

let private childChart (drafts: NodeDraft<int, int, int, string> list) =
    statechart<int, int, int, string> {
        root "root"
        classify (fun _ -> stateId "p.a")

        compound "outer" {
            initial "p.a"
            yield! drafts
        }
    }
    |> expectChart

let private staleClassifierChart =
    let fragment: NodeDraft<int, int, int, string> =
        state "leaf" { on (fun _ _ -> true) (fun state event -> [ event ], state) }

    statechart<int, int, int, string> {
        root "root"
        classify (fun _ -> stateId "leaf")
        Fragment.prefix "p" fragment
    }
    |> expectChart

let private siblingsChart (drafts: NodeDraft<int, int, int, string> list) =
    statechart<int, int, int, string> {
        root "root"
        classify (fun _ -> stateId "a")
        yield! drafts
    }
    |> expectChart

let private orderedRulesChart transitionFirst =
    let draft: NodeDraft<int, int, string, string> =
        if transitionFirst then
            state "leaf" {
                on (fun _ _ -> true) (fun state _ -> [ "transition" ], state)
                internalOn (fun _ _ -> true) (fun _ _ -> [ "internal" ])
            }
        else
            state "leaf" {
                internalOn (fun _ _ -> true) (fun _ _ -> [ "internal" ])
                on (fun _ _ -> true) (fun state _ -> [ "transition" ], state)
            }

    statechart<int, int, string, string> {
        root "root"
        classify (fun _ -> stateId "leaf")
        draft
    }
    |> expectChart

let tests =
    testList
        "fragment laws"
        [ Property.property "part splicing is associative and Zero is its identity" (fun (state: int, event: int) ->
              let builder = NodeBuilder<int, int, int, string>("leaf")
              let first = [ rulePart 1 ]
              let second = [ rulePart 2 ]
              let third = [ rulePart 3 ]
              let zero = builder.Zero()
              let left = builder.Combine(builder.Combine(first, second), third)
              let right = builder.Combine(first, builder.Combine(second, third))
              let withLeftIdentity = builder.Combine(zero, left)
              let withRightIdentity = builder.Combine(left, zero)
              let expected = observeRules state event (runParts left)

              expected = observeRules state event (runParts right)
              && expected = observeRules state event (runParts withLeftIdentity)
              && expected = observeRules state event (runParts withRightIdentity))

          Property.property
              "splicing preserves rule order and therefore first match wins"
              (fun (state: int, event: int) ->
                  let builder = NodeBuilder<int, int, int, string>("leaf")

                  let parts =
                      builder.Combine([ rulePart 1 ], builder.Combine([ rulePart 2 ], [ rulePart 3 ]))

                  let draft = runParts parts
                  let verdicts = observeRules state event draft

                  match Chart.resolve (chartOfParts parts) state event with
                  | Ok resolution ->
                      verdicts = [ RuleVerdict.Handled([ 1 ], RuleOutcome.Next state)
                                   RuleVerdict.Handled([ 2 ], RuleOutcome.Next state)
                                   RuleVerdict.Handled([ 3 ], RuleOutcome.Next state) ]
                      && resolution.Actions = [ 1 ]
                  | Result.Error _ -> false)

          Property.property "prefix a after prefix b is prefix a.b" (fun (token: int) ->
              let nested =
                  retryFragment |> Fragment.prefix "b" |> Fragment.prefix "a" |> prefixedChart

              let composed = retryFragment |> Fragment.prefix "a.b" |> prefixedChart

              Chart.fingerprint nested = Chart.fingerprint composed
              && Chart.resolve nested Failed (Retry token) = Chart.resolve composed Failed (Retry token))

          Property.property "prefixing independent children distributes over child splicing" (fun (splitSeed: int) ->
              let fragments = [ emptyLeaf "a"; emptyLeaf "b"; emptyLeaf "c" ]
              let split = Property.bounded (fragments.Length + 1) splitSeed
              let before, after = List.splitAt split fragments
              let prefix = Fragment.prefix "p"
              let together = (before @ after) |> List.map prefix |> childChart
              let separately = (List.map prefix before @ List.map prefix after) |> childChart

              Chart.fingerprint together = Chart.fingerprint separately)

          Property.property
              "prefix cannot rewrite classifier output and stale output gives UnknownState"
              (fun (state: int, event: int) ->
                  match Chart.resolve staleClassifierChart state event with
                  | Result.Error(TransitionError.UnknownState id) -> id = stateId "leaf"
                  | _ -> false)

          Property.property "sibling declaration order does not change the fingerprint" (fun (selector: int) ->
              let siblings = [ emptyLeaf "a"; emptyLeaf "b"; emptyLeaf "c" ]
              let shift = Property.bounded siblings.Length selector
              let before, after = List.splitAt shift siblings
              let reordered = after @ before

              Chart.fingerprint (siblingsChart siblings) = Chart.fingerprint (siblingsChart reordered))

          Property.property
              "reordering structurally distinct rules changes the fingerprint and first match"
              (fun (state: int, event: int) ->
                  let transitionFirst = orderedRulesChart true
                  let internalFirst = orderedRulesChart false

                  let actions chart =
                      Chart.resolve chart state event
                      |> Result.map (fun resolution -> resolution.Actions)

                  Chart.fingerprint transitionFirst <> Chart.fingerprint internalFirst
                  && actions transitionFirst = Ok [ "transition" ]
                  && actions internalFirst = Ok [ "internal" ]) ]
