module ByzantineSystems.Automata.Core.Tests.ResolvePropertyTests

open ByzantineSystems.Automata.Core
open Expecto

type private Location =
    | LeftOne
    | LeftTwo
    | RightOne
    | RightTwo
    | Done

type private Model = { At: Location; Payload: int }

type private Decision =
    | Handle
    | Reject
    | Refuse

type private Event =
    | Stay of token: int
    | Race of decision: Decision * first: Model * later: Model * token: int
    | Move of next: Model * token: int
    | ResetLeft of next: Model * token: int
    | Noise of token: int

type private Action =
    | ExitedNode of node: StateId * oldPayload: int
    | RuleRan of token: int
    | EnteredNode of node: StateId * newPayload: int
    | InternalRan of token: int
    | WinnerRan of token: int
    | LaterRan of token: int
    | SelfRan of token: int
    | GotoRan of token: int

let private rootId = stateId "root"
let private leftId = stateId "left"
let private leftDeepId = stateId "left.deep"
let private leftOneId = stateId "left.deep.one"
let private leftTwoId = stateId "left.deep.two"
let private rightId = stateId "right"
let private rightOneId = stateId "right.one"
let private rightTwoId = stateId "right.two"
let private doneId = stateId "done"

let private classifyModel model =
    match model.At with
    | LeftOne -> leftOneId
    | LeftTwo -> leftTwoId
    | RightOne -> rightOneId
    | RightTwo -> rightTwoId
    | Done -> doneId

let private isMove _ event =
    match event with
    | Move _ -> true
    | _ -> false

let private performMove state event =
    match event with
    | Move(next, token) -> [ RuleRan token ], next
    | _ -> [], state

let private firstDecisionRule: Rule<Model, Event, Action, string> =
    Rule.attempt
        (fun _ event ->
            match event with
            | Race _ -> true
            | _ -> false)
        (fun current event ->
            match event with
            | Race(Handle, first, _, token) -> Ok([ WinnerRan token ], first)
            | Race(Reject, _, _, _) -> Result.Error "rejected"
            | Race(Refuse, first, _, token) -> Ok([ WinnerRan token ], first)
            | _ -> Ok([], current))
    |> Rule.guarded
        (fun _ event ->
            match event with
            | Race(Refuse, _, _, _) -> false
            | _ -> true)
        "refused"

let private chart =
    statechart<Model, Event, Action, string> {
        root "root"
        classify classifyModel

        compound "left" {
            initial "left.deep"
            onEntry (fun state _ -> [ EnteredNode(leftId, state.Payload) ])
            onExit (fun state _ -> [ ExitedNode(leftId, state.Payload) ])

            internalOn
                (fun _ event ->
                    match event with
                    | Stay _ -> true
                    | _ -> false)
                (fun _ event ->
                    match event with
                    | Stay token -> [ InternalRan token ]
                    | _ -> [])

            on
                (fun _ event ->
                    match event with
                    | Noise _ -> true
                    | _ -> false)
                (fun state event ->
                    match event with
                    | Noise token -> [ SelfRan token ], state
                    | _ -> [], state)

            compound "left.deep" {
                initial "left.deep.one"
                onEntry (fun state _ -> [ EnteredNode(leftDeepId, state.Payload) ])
                onExit (fun state _ -> [ ExitedNode(leftDeepId, state.Payload) ])

                state "left.deep.one" {
                    onEntry (fun state _ -> [ EnteredNode(leftOneId, state.Payload) ])
                    onExit (fun state _ -> [ ExitedNode(leftOneId, state.Payload) ])
                    NodeRule firstDecisionRule

                    on
                        (fun _ event ->
                            match event with
                            | Race _ -> true
                            | _ -> false)
                        (fun state event ->
                            match event with
                            | Race(_, _, later, token) -> [ LaterRan token ], later
                            | _ -> [], state)

                    on isMove performMove
                }

                state "left.deep.two" {
                    onEntry (fun state _ -> [ EnteredNode(leftTwoId, state.Payload) ])
                    onExit (fun state _ -> [ ExitedNode(leftTwoId, state.Payload) ])
                    on isMove performMove
                }
            }
        }

        compound "right" {
            initial "right.one"
            onEntry (fun state _ -> [ EnteredNode(rightId, state.Payload) ])
            onExit (fun state _ -> [ ExitedNode(rightId, state.Payload) ])

            goto
                (fun _ event ->
                    match event with
                    | ResetLeft _ -> true
                    | _ -> false)
                leftId
                (fun state event ->
                    match event with
                    | ResetLeft(next, token) -> [ GotoRan token ], next
                    | _ -> [], state)

            state "right.one" {
                onEntry (fun state _ -> [ EnteredNode(rightOneId, state.Payload) ])
                onExit (fun state _ -> [ ExitedNode(rightOneId, state.Payload) ])
                on isMove performMove
            }

            state "right.two" {
                onEntry (fun state _ -> [ EnteredNode(rightTwoId, state.Payload) ])
                onExit (fun state _ -> [ ExitedNode(rightTwoId, state.Payload) ])
                on isMove performMove
            }
        }

        state "done" { terminal }
    }
    |> function
        | Ok chart -> chart
        | Result.Error errors -> failtestf "resolve property fixture is invalid: %A" errors

let private decisionOf selector =
    match Property.bounded 3 selector with
    | 0 -> Handle
    | 1 -> Reject
    | _ -> Refuse

let private distinctPair selector =
    let pairs =
        [| LeftOne, LeftTwo
           LeftOne, RightOne
           LeftOne, RightTwo
           LeftTwo, LeftOne
           LeftTwo, RightOne
           LeftTwo, RightTwo
           RightOne, LeftOne
           RightOne, LeftTwo
           RightOne, RightTwo
           RightTwo, LeftOne
           RightTwo, LeftTwo
           RightTwo, RightOne |]

    pairs.[Property.bounded pairs.Length selector]

let private chain =
    function
    | LeftOne -> [ leftOneId; leftDeepId; leftId; rootId ]
    | LeftTwo -> [ leftTwoId; leftDeepId; leftId; rootId ]
    | RightOne -> [ rightOneId; rightId; rootId ]
    | RightTwo -> [ rightTwoId; rightId; rootId ]
    | Done -> [ doneId; rootId ]

let private expectedPaths source target =
    let sourceChain = chain source
    let targetChain = chain target
    let lca = sourceChain |> List.find (fun id -> List.contains id targetChain)
    let exited = sourceChain |> List.takeWhile ((<>) lca)
    let entered = targetChain |> List.takeWhile ((<>) lca) |> List.rev
    exited, entered

let tests =
    testList
        "resolve laws"
        [ Property.property
              "internal identity: the sole matching rule names its handler and preserves the state"
              (fun (useSecondLeaf: bool, payload: int, token: int) ->
                  let location = if useSecondLeaf then LeftTwo else LeftOne
                  let state = { At = location; Payload = payload }

                  match Chart.resolve chart state (Stay token) with
                  | Ok resolution ->
                      resolution.HandledBy = leftId
                      && resolution.Exited = []
                      && resolution.Entered = []
                      && resolution.Next = state
                      && resolution.Actions = [ InternalRan token ]
                  | Result.Error _ -> false)

          Property.property
              "first match wins: handled, rejected, and guard-refused verdicts suppress later rules"
              (fun (selector: int, oldPayload: int, firstPayload: int, laterPayload: int, token: int) ->
                  let decision = decisionOf selector
                  let current = { At = LeftOne; Payload = oldPayload }
                  let first = { At = LeftTwo; Payload = firstPayload }

                  let later =
                      { At = RightOne
                        Payload = laterPayload }

                  match decision, Chart.resolve chart current (Race(decision, first, later, token)) with
                  | Handle, Ok resolution ->
                      resolution.Next = first
                      && List.contains (WinnerRan token) resolution.Actions
                      && not (List.contains (LaterRan token) resolution.Actions)
                  | Reject, Result.Error(TransitionError.Rejected "rejected") -> true
                  | Refuse, Result.Error(TransitionError.GuardFailed(handler, "refused")) -> handler = leftOneId
                  | _ -> false)

          Property.property
              "LCA path law: exited and entered are exactly the prefixes around the LCA"
              (fun (selector: int, oldPayload: int, newPayload: int, token: int) ->
                  let sourceLocation, targetLocation = distinctPair selector

                  let source =
                      { At = sourceLocation
                        Payload = oldPayload }

                  let target =
                      { At = targetLocation
                        Payload = newPayload }

                  let expectedExited, expectedEntered = expectedPaths sourceLocation targetLocation

                  match Chart.resolve chart source (Move(target, token)) with
                  | Ok resolution ->
                      resolution.Exited = expectedExited
                      && resolution.Entered = expectedEntered
                      && resolution.Next = target
                  | Result.Error _ -> false)

          Property.property
              "self-transition: an ancestor handler exits leaf-to-handler and enters the reverse path"
              (fun (useSecondLeaf: bool, payload: int, token: int) ->
                  let location, leaf =
                      if useSecondLeaf then
                          LeftTwo, leftTwoId
                      else
                          LeftOne, leftOneId

                  let state = { At = location; Payload = payload }
                  let expectedExited = [ leaf; leftDeepId; leftId ]

                  match Chart.resolve chart state (Noise token) with
                  | Ok resolution ->
                      resolution.HandledBy = leftId
                      && resolution.Exited = expectedExited
                      && resolution.Entered = List.rev expectedExited
                      && resolution.Next = state
                  | Result.Error _ -> false)

          Property.property
              "goto descent: the initial-child leaf is required and mismatches are typed"
              (fun (agrees: bool, useRightTwo: bool, oldPayload: int, newPayload: int, token: int) ->
                  let source =
                      { At = if useRightTwo then RightTwo else RightOne
                        Payload = oldPayload }

                  let next =
                      { At = if agrees then LeftOne else LeftTwo
                        Payload = newPayload }

                  match agrees, Chart.resolve chart source (ResetLeft(next, token)) with
                  | true, Ok resolution ->
                      Chart.classifyOf chart resolution.Next = leftOneId
                      && resolution.Next = next
                      && resolution.Entered = [ leftId; leftDeepId; leftOneId ]
                  | false, Result.Error(TransitionError.TargetMismatch(expected, actual)) ->
                      expected = leftOneId && actual = leftTwoId
                  | _ -> false)

          Property.property
              "actions order: exits, rule actions, and entries are concatenated in execution order"
              (fun (selector: int, oldPayload: int, newPayload: int, token: int) ->
                  let sourceLocation, targetLocation = distinctPair selector

                  let source =
                      { At = sourceLocation
                        Payload = oldPayload }

                  let target =
                      { At = targetLocation
                        Payload = newPayload }

                  let exited, entered = expectedPaths sourceLocation targetLocation

                  let expectedActions =
                      (exited |> List.map (fun id -> ExitedNode(id, oldPayload)))
                      @ [ RuleRan token ]
                      @ (entered |> List.map (fun id -> EnteredNode(id, newPayload)))

                  match Chart.resolve chart source (Move(target, token)) with
                  | Ok resolution -> resolution.Actions = expectedActions
                  | Result.Error _ -> false)

          Property.property "anchor: a terminal leaf is unhandled by construction" (fun (payload: int, token: int) ->
              let terminalState = { At = Done; Payload = payload }

              match Chart.resolve chart terminalState (Noise token) with
              | Result.Error(TransitionError.Unhandled(leaf, _)) -> leaf = doneId
              | _ -> false) ]
