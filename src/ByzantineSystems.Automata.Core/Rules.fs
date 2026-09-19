namespace ByzantineSystems.Automata.Core

/// <summary>
/// What one rule says about a (state, event) pair. <see cref="F:ByzantineSystems.Automata.Core.RuleVerdict`3.NoMatch" />
/// means "not my event": resolution bubbles to the parent. Everything else is final for this
/// rule: a guard refusal and a domain rejection both stop the search.
/// </summary>
type RuleVerdict<'Action, 'State, 'Err> =
    | NoMatch
    | GuardFailed of reason: string
    | Rejected of error: 'Err
    | Handled of actions: 'Action list * outcome: RuleOutcome<'State>

/// <summary>
/// Where a matched rule sends the entity. Every outcome carries the concrete next
/// <c>'State</c>: an initial-child path can choose a graph route but cannot invent the data
/// a parameterised state needs.
/// </summary>
and RuleOutcome<'State> =
    /// <summary>Ordinary transition: the leaf is <c>Classify nextState</c>.</summary>
    | Next of state: 'State

    /// <summary>
    /// Compound target: follow the target's initial-child path to a leaf; resolution verifies
    /// <c>Classify nextState</c> equals that leaf, otherwise the result is
    /// <see cref="F:ByzantineSystems.Automata.Core.TransitionError`1.TargetMismatch" />.
    /// </summary>
    | Goto of target: StateId * state: 'State

    /// <summary>Internal transition: no exit or entry actions, state unchanged.</summary>
    | Internal

/// <summary>
/// One transition rule: a total function from (state, event) to a
/// <see cref="T:ByzantineSystems.Automata.Core.RuleVerdict`3" />. Private constructor keeps
/// rules built through <see cref="T:ByzantineSystems.Automata.Core.Rule" /> combinators.
/// </summary>
type Rule<'State, 'Event, 'Action, 'Err> = private Rule of ('State * 'Event -> RuleVerdict<'Action, 'State, 'Err>)

/// <summary>Rule combinators and invocation.</summary>
[<RequireQualifiedAccess>]
module Rule =

    /// <summary>Ordinary transition: when the predicate matches, transform to (actions, next state).</summary>
    let transition
        (predicate: 'State -> 'Event -> bool)
        (transform: 'State -> 'Event -> 'Action list * 'State)
        : Rule<'State, 'Event, 'Action, 'Err> =
        Rule(fun (state, event) ->
            if predicate state event then
                let actions, nextState = transform state event
                RuleVerdict.Handled(actions, RuleOutcome.Next nextState)
            else
                RuleVerdict.NoMatch)

    /// <summary>Result-returning transition: a matched rule may reject the event with a domain error.</summary>
    let attempt
        (predicate: 'State -> 'Event -> bool)
        (transform: 'State -> 'Event -> Result<'Action list * 'State, 'Err>)
        : Rule<'State, 'Event, 'Action, 'Err> =
        Rule(fun (state, event) ->
            if predicate state event then
                match transform state event with
                | Ok(actions, nextState) -> RuleVerdict.Handled(actions, RuleOutcome.Next nextState)
                | Result.Error err -> RuleVerdict.Rejected err
            else
                RuleVerdict.NoMatch)

    /// <summary>Compound-target transition: follows the target node's initial-child path.</summary>
    let goto
        (predicate: 'State -> 'Event -> bool)
        (target: StateId)
        (transform: 'State -> 'Event -> 'Action list * 'State)
        : Rule<'State, 'Event, 'Action, 'Err> =
        Rule(fun (state, event) ->
            if predicate state event then
                let actions, nextState = transform state event
                RuleVerdict.Handled(actions, RuleOutcome.Goto(target, nextState))
            else
                RuleVerdict.NoMatch)

    /// <summary>Internal transition: runs actions but performs no exit/entry and keeps the state.</summary>
    let internalOn
        (predicate: 'State -> 'Event -> bool)
        (transform: 'State -> 'Event -> 'Action list)
        : Rule<'State, 'Event, 'Action, 'Err> =
        Rule(fun (state, event) ->
            if predicate state event then
                RuleVerdict.Handled(transform state event, RuleOutcome.Internal)
            else
                RuleVerdict.NoMatch)

    /// <summary>
    /// Guard combinator: when the predicate refuses, the rule reports
    /// <see cref="F:ByzantineSystems.Automata.Core.RuleVerdict`3.GuardFailed" /> instead of
    /// silently not matching; the search stops with a reportable reason rather than bubbling.
    /// </summary>
    let guarded
        (predicate: 'State -> 'Event -> bool)
        (reason: string)
        (rule: Rule<'State, 'Event, 'Action, 'Err>)
        : Rule<'State, 'Event, 'Action, 'Err> =
        Rule(fun (state, event) ->
            if predicate state event then
                let (Rule inner) = rule in inner (state, event)
            else
                RuleVerdict.GuardFailed reason)

    /// <summary>Applies a rule to one (state, event) pair.</summary>
    let invoke
        (rule: Rule<'State, 'Event, 'Action, 'Err>)
        (state: 'State)
        (event: 'Event)
        : RuleVerdict<'Action, 'State, 'Err> =
        let (Rule inner) = rule
        inner (state, event)
