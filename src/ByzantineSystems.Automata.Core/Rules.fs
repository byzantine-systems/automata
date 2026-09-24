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
/// What a rule is, stripped of the closures that decide when it fires. A rule's behaviour lives
/// in predicates and transforms, which are opaque functions; this is the part of it that can be
/// compared, printed, or hashed into a chart fingerprint.
/// </summary>
type RuleKind =
    /// <summary>An ordinary transition, built by <c>Rule.transition</c>.</summary>
    | Transition

    /// <summary>A transition that may reject the event, built by <c>Rule.attempt</c>.</summary>
    | Attempt

    /// <summary>A transition to a compound target's initial-child path, built by <c>Rule.goto</c>.</summary>
    | Goto of target: StateId

    /// <summary>An internal transition, built by <c>Rule.internalOn</c>.</summary>
    | Internal

    /// <summary>The kind a <c>Rule.guarded</c> wrapper reports, keeping the wrapped rule visible.</summary>
    | Guarded of reason: string * inner: RuleKind

/// <summary>
/// One transition rule: a total function from (state, event) to a
/// <see cref="T:ByzantineSystems.Automata.Core.RuleVerdict`3" />, paired with the
/// <see cref="T:ByzantineSystems.Automata.Core.RuleKind" /> that says which combinator built it.
/// Private constructor keeps rules built through
/// <see cref="T:ByzantineSystems.Automata.Core.Rule" /> combinators.
/// </summary>
type Rule<'State, 'Event, 'Action, 'Err> =
    private
        { Kind: RuleKind
          Invoke: 'State * 'Event -> RuleVerdict<'Action, 'State, 'Err> }

/// <summary>Rule combinators and invocation.</summary>
[<RequireQualifiedAccess>]
module Rule =

    /// <summary>Ordinary transition: when the predicate matches, transform to (actions, next state).</summary>
    let transition
        (predicate: 'State -> 'Event -> bool)
        (transform: 'State -> 'Event -> 'Action list * 'State)
        : Rule<'State, 'Event, 'Action, 'Err> =
        { Kind = RuleKind.Transition
          Invoke =
            fun (state, event) ->
                if predicate state event then
                    let actions, nextState = transform state event
                    RuleVerdict.Handled(actions, RuleOutcome.Next nextState)
                else
                    RuleVerdict.NoMatch }

    /// <summary>Result-returning transition: a matched rule may reject the event with a domain error.</summary>
    let attempt
        (predicate: 'State -> 'Event -> bool)
        (transform: 'State -> 'Event -> Result<'Action list * 'State, 'Err>)
        : Rule<'State, 'Event, 'Action, 'Err> =
        { Kind = RuleKind.Attempt
          Invoke =
            fun (state, event) ->
                if predicate state event then
                    match transform state event with
                    | Ok(actions, nextState) -> RuleVerdict.Handled(actions, RuleOutcome.Next nextState)
                    | Result.Error err -> RuleVerdict.Rejected err
                else
                    RuleVerdict.NoMatch }

    /// <summary>Compound-target transition: follows the target node's initial-child path.</summary>
    let goto
        (predicate: 'State -> 'Event -> bool)
        (target: StateId)
        (transform: 'State -> 'Event -> 'Action list * 'State)
        : Rule<'State, 'Event, 'Action, 'Err> =
        { Kind = RuleKind.Goto target
          Invoke =
            fun (state, event) ->
                if predicate state event then
                    let actions, nextState = transform state event
                    RuleVerdict.Handled(actions, RuleOutcome.Goto(target, nextState))
                else
                    RuleVerdict.NoMatch }

    /// <summary>Internal transition: runs actions but performs no exit/entry and keeps the state.</summary>
    let internalOn
        (predicate: 'State -> 'Event -> bool)
        (transform: 'State -> 'Event -> 'Action list)
        : Rule<'State, 'Event, 'Action, 'Err> =
        { Kind = RuleKind.Internal
          Invoke =
            fun (state, event) ->
                if predicate state event then
                    RuleVerdict.Handled(transform state event, RuleOutcome.Internal)
                else
                    RuleVerdict.NoMatch }

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
        { Kind = RuleKind.Guarded(reason, rule.Kind)
          Invoke =
            fun (state, event) ->
                if predicate state event then
                    rule.Invoke(state, event)
                else
                    RuleVerdict.GuardFailed reason }

    /// <summary>
    /// Which combinator built this rule. Everything deciding whether the rule fires is a closure
    /// and stays invisible; this is the structure a chart fingerprint can see.
    /// </summary>
    let kind (rule: Rule<'State, 'Event, 'Action, 'Err>) : RuleKind = rule.Kind

    /// <summary>
    /// The compound node a rule sends the entity to, looking through any guards, or <c>None</c>
    /// for a rule that does not goto anywhere. Read from the kind, so a chart can check every
    /// target it names without firing a single rule.
    /// </summary>
    let target (rule: Rule<'State, 'Event, 'Action, 'Err>) : StateId option =
        let rec targetOf kind =
            match kind with
            | RuleKind.Goto target -> Some target
            | RuleKind.Guarded(_, inner) -> targetOf inner
            | RuleKind.Transition
            | RuleKind.Attempt
            | RuleKind.Internal -> None

        targetOf rule.Kind

    /// <summary>
    /// Renames the goto target a rule can produce, both where the chart reads it (the kind,
    /// through any guards) and where resolution does (the outcome its closure returns). The
    /// second is only reachable by wrapping the closure, which is why this lives beside the
    /// private fields rather than in the fragment code that calls it. Every other verdict and
    /// outcome passes through untouched.
    /// </summary>
    let retarget
        (rename: StateId -> StateId)
        (rule: Rule<'State, 'Event, 'Action, 'Err>)
        : Rule<'State, 'Event, 'Action, 'Err> =
        let rec renameKind kind =
            match kind with
            | RuleKind.Goto target -> RuleKind.Goto(rename target)
            | RuleKind.Guarded(reason, inner) -> RuleKind.Guarded(reason, renameKind inner)
            | RuleKind.Transition
            | RuleKind.Attempt
            | RuleKind.Internal -> kind

        let renameVerdict verdict =
            match verdict with
            | RuleVerdict.Handled(actions, RuleOutcome.Goto(target, state)) ->
                RuleVerdict.Handled(actions, RuleOutcome.Goto(rename target, state))
            | other -> other

        { Kind = renameKind rule.Kind
          Invoke = rule.Invoke >> renameVerdict }

    /// <summary>Applies a rule to one (state, event) pair.</summary>
    let invoke
        (rule: Rule<'State, 'Event, 'Action, 'Err>)
        (state: 'State)
        (event: 'Event)
        : RuleVerdict<'Action, 'State, 'Err> =
        rule.Invoke(state, event)
