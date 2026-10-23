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
                      Rule.transition (fun _ e -> Property.bounded 3 e = Property.bounded 3 seed) (fun s e ->
                          [ e; seed ], s + event)

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
                      Rule.transition (fun _ e -> Property.bounded 5 e = Property.bounded 5 seed) (fun s e ->
                          [ e * 2 ], s - event)

                  if Property.bounded 5 event = Property.bounded 5 seed then
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
                  let target = stateId $"t%d{Property.bounded 4 seed}"

                  let rule = Rule.goto (fun _ _ -> true) target (fun s _ -> [ seed ], s)

                  Rule.invoke rule state event = RuleVerdict.Handled([ seed ], RuleOutcome.Goto(target, state)))

          Property.property "internalOn: actions only, the state never moves" (fun (state: int, event: int) ->
              let rule = Rule.internalOn (fun _ _ -> true) (fun _ e -> [ e * 3 ])

              Rule.invoke rule state event = RuleVerdict.Handled([ event * 3 ], RuleOutcome.Internal))

          Property.property
              "kind names the combinator that built the rule, and target projects goto through guards"
              (fun (seed: int) ->
                  let target = stateId $"t%d{Property.bounded 4 seed}"
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
                  let source = stateId $"t%d{Property.bounded 4 seed}"
                  let destination = stateId "elsewhere"

                  let rule =
                      Rule.guarded
                          (fun _ e -> Property.bounded 2 e = 0)
                          "even events only"
                          (Rule.goto (fun _ _ -> true) source (fun s _ -> [ seed ], s))

                  let rename (id: StateId) = if id = source then destination else id
                  let retargeted = Rule.retarget rename rule

                  Rule.invoke retargeted state event = mapGoto rename (Rule.invoke rule state event)
                  && Rule.kind retargeted = RuleKind.Guarded("even events only", RuleKind.Goto destination))

          Property.property
              "condition-based rules interpret their normalized condition syntax"
              (fun (state: int, event: int) ->
                  let condition =
                      Condition.allOf
                          [ Condition.stateIs "state.nonnegative" (fun value -> value >= 0)
                            Condition.eventIs "event.even" (fun value -> Property.bounded 2 value = 0) ]

                  let rule =
                      Rule.transitionWhen condition (fun current value -> [ value ], current + 1)

                  let expectedMatch = state >= 0 && Property.bounded 2 event = 0

                  Rule.syntax rule = RuleSyntax.Transition(Condition.syntax condition)
                  && if expectedMatch then
                         Rule.invoke rule state event = RuleVerdict.Handled([ event ], RuleOutcome.Next(state + 1))
                     else
                         Rule.invoke rule state event = RuleVerdict.NoMatch)

          test "every condition-based constructor exposes its syntax while RuleKind stays compatible" {
              let condition: Condition<int, int> =
                  Condition.eventIs "event.positive" (fun event -> event > 0)

              let syntax = Condition.syntax condition
              let target = stateId "target"
              let transition = Rule.transitionWhen condition (fun state _ -> [], state)
              let attemptRule = Rule.attemptWhen condition (fun state _ -> Ok([], state))
              let gotoRule = Rule.gotoWhen condition target (fun state _ -> [], state)
              let internalRule = Rule.internalWhen condition (fun _ _ -> [])
              let guardedRule = Rule.guardedWhen condition "allowed" gotoRule

              Expect.equal (Rule.syntax transition) (RuleSyntax.Transition syntax) "transition syntax"
              Expect.equal (Rule.syntax attemptRule) (RuleSyntax.Attempt syntax) "attempt syntax"
              Expect.equal (Rule.syntax gotoRule) (RuleSyntax.Goto(syntax, target)) "goto syntax"
              Expect.equal (Rule.syntax internalRule) (RuleSyntax.Internal syntax) "internal syntax"

              Expect.equal
                  (Rule.syntax guardedRule)
                  (RuleSyntax.Guarded("allowed", syntax, RuleSyntax.Goto(syntax, target)))
                  "guard syntax"

              Expect.equal (Rule.kind transition) RuleKind.Transition "transition kind"
              Expect.equal (Rule.kind attemptRule) RuleKind.Attempt "attempt kind"
              Expect.equal (Rule.kind gotoRule) (RuleKind.Goto target) "goto kind"
              Expect.equal (Rule.kind internalRule) RuleKind.Internal "internal kind"
              Expect.equal (Rule.kind guardedRule) (RuleKind.Guarded("allowed", RuleKind.Goto target)) "guard kind"
          } ]
