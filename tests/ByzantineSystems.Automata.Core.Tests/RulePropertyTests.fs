module ByzantineSystems.Automata.Core.Tests.RulePropertyTests

open ByzantineSystems.Automata.Core
open Expecto

/// The observation the retarget law compares under: every verdict passes through untouched
/// except a Goto outcome, whose target is mapped. This is the spec of what retarget may see.
let private mapGoto (f: StateId -> StateId) (verdict: RuleVerdict<int, int, string>) =
    match verdict with
    | RuleVerdict.Handled(actions, RuleOutcome.Goto(target, nextState)) ->
        RuleVerdict.Handled(actions, RuleOutcome.Goto(f target, nextState))
    | other -> other

let tests =
    testList
        "rule laws"
        [ Property.property
              "a holding guard is transparent: the wrapped rule decides"
              (fun (seed: int, state: int, event: int) ->
                  let inner =
                      Rule.transition (fun _ e -> abs e % 3 = abs seed % 3) (fun s e -> [ e; seed ], s + event)

                  let guarded = Rule.guarded (fun _ _ -> true) "always holds" inner

                  Rule.invoke guarded state event = Rule.invoke inner state event)

          Property.property
              "a refusing guard stops the search with its reason, whatever it wraps"
              (fun (state: int, event: int) ->
                  let inner = Rule.transition (fun _ _ -> true) (fun s _ -> [], s)
                  let guarded = Rule.guarded (fun _ _ -> false) "the reason" inner

                  Rule.invoke guarded state event = RuleVerdict.GuardFailed "the reason")

          Property.property
              "transition: NoMatch exactly when the predicate refuses, Handled carrying the transform's answer"
              (fun (seed: int, state: int, event: int) ->
                  let rule =
                      Rule.transition (fun _ e -> abs e % 5 = abs seed % 5) (fun s e -> [ e * 2 ], s - event)

                  if abs event % 5 = abs seed % 5 then
                      Rule.invoke rule state event = RuleVerdict.Handled(
                          [ event * 2 ],
                          RuleOutcome.Next(state - event)
                      )
                  else
                      Rule.invoke rule state event = RuleVerdict.NoMatch)

          Property.property
              "attempt: a matched rule maps Ok to Handled and Error to Rejected"
              (fun (state: int, event: int, fail: bool) ->
                  let rule =
                      Rule.attempt (fun _ _ -> true) (fun s e -> if fail then Error "boom" else Ok([ e ], s + 1))

                  if fail then
                      Rule.invoke rule state event = RuleVerdict.Rejected "boom"
                  else
                      Rule.invoke rule state event = RuleVerdict.Handled([ event ], RuleOutcome.Next(state + 1)))

          Property.property
              "goto: a matched rule echoes its target beside the next state"
              (fun (seed: int, state: int, event: int) ->
                  let target = stateId $"t%d{abs seed % 4}"

                  let rule = Rule.goto (fun _ _ -> true) target (fun s _ -> [ seed ], s)

                  Rule.invoke rule state event = RuleVerdict.Handled([ seed ], RuleOutcome.Goto(target, state)))

          Property.property "internalOn: actions only, the state never moves" (fun (state: int, event: int) ->
              let rule = Rule.internalOn (fun _ _ -> true) (fun _ e -> [ e * 3 ])

              Rule.invoke rule state event = RuleVerdict.Handled([ event * 3 ], RuleOutcome.Internal))

          Property.property
              "kind names the combinator that built the rule, and target projects goto through guards"
              (fun (seed: int) ->
                  let target = stateId $"t%d{abs seed % 4}"
                  let transition = Rule.transition (fun _ _ -> true) (fun s _ -> [], s)
                  let attemptRule = Rule.attempt (fun _ _ -> true) (fun s _ -> Ok([], s))
                  let internalRule = Rule.internalOn (fun _ _ -> true) (fun _ _ -> [])
                  let gotoRule = Rule.goto (fun _ _ -> true) target (fun s _ -> [], s)
                  let guardedGoto = Rule.guarded (fun _ _ -> true) "open" gotoRule

                  Rule.kind transition = RuleKind.Transition
                  && Rule.kind attemptRule = RuleKind.Attempt
                  && Rule.kind internalRule = RuleKind.Internal
                  && Rule.kind gotoRule = RuleKind.Goto target
                  && Rule.kind guardedGoto = RuleKind.Guarded("open", RuleKind.Goto target)
                  && Rule.target transition = None
                  && Rule.target attemptRule = None
                  && Rule.target internalRule = None
                  && Rule.target gotoRule = Some target
                  && Rule.target guardedGoto = Some target)

          Property.property
              "retarget renames goto targets in verdicts and kinds, and touches nothing else"
              (fun (seed: int, state: int, event: int) ->
                  let source = stateId $"t%d{abs seed % 4}"
                  let destination = stateId "elsewhere"

                  let rule =
                      Rule.guarded
                          (fun _ e -> abs e % 2 = 0)
                          "even events only"
                          (Rule.goto (fun _ _ -> true) source (fun s _ -> [ seed ], s))

                  let rename (id: StateId) = if id = source then destination else id
                  let retargeted = Rule.retarget rename rule

                  Rule.invoke retargeted state event = mapGoto rename (Rule.invoke rule state event)
                  && Rule.kind retargeted = RuleKind.Guarded("even events only", RuleKind.Goto destination)) ]
