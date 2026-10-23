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
/// The inspectable syntax of a rule: its combinator, normalized condition syntax, goto target,
/// and any guard wrappers. Transforms and emitted actions remain opaque callbacks.
/// </summary>
[<RequireQualifiedAccess>]
type RuleSyntax =
    | Transition of condition: ConditionSyntax
    | Attempt of condition: ConditionSyntax
    | Goto of condition: ConditionSyntax * target: StateId
    | Internal of condition: ConditionSyntax
    | Guarded of reason: string * condition: ConditionSyntax * inner: RuleSyntax

/// <summary>
/// One transition rule: a total function from (state, event) to a
/// <see cref="T:ByzantineSystems.Automata.Core.RuleVerdict`3" />, paired with the
/// <see cref="T:ByzantineSystems.Automata.Core.RuleSyntax" /> that describes its combinator and
/// normalized condition syntax.
/// Private constructor keeps rules built through
/// <see cref="T:ByzantineSystems.Automata.Core.Rule" /> combinators.
/// </summary>
type Rule<'State, 'Event, 'Action, 'Err> =
    private
        { Syntax: RuleSyntax
          Invoke: 'State * 'Event -> RuleVerdict<'Action, 'State, 'Err> }

/// <summary>Rule combinators and invocation.</summary>
[<RequireQualifiedAccess>]
module Rule =

    /// <summary>Ordinary transition controlled by inspectable condition syntax.</summary>
    let transitionWhen
        (condition: Condition<'State, 'Event>)
        (transform: 'State -> 'Event -> 'Action list * 'State)
        : Rule<'State, 'Event, 'Action, 'Err> =
        { Syntax = RuleSyntax.Transition(Condition.syntax condition)
          Invoke =
            fun (state, event) ->
                if Condition.matches condition state event then
                    let actions, nextState = transform state event
                    RuleVerdict.Handled(actions, RuleOutcome.Next nextState)
                else
                    RuleVerdict.NoMatch }

    /// <summary>
    /// Ordinary transition controlled by an opaque legacy predicate. Use <c>transitionWhen</c>
    /// when condition syntax should distinguish predicates structurally.
    /// </summary>
    let transition predicate transform =
        transitionWhen (Condition.custom "legacy.transition" predicate) transform

    /// <summary>Result-returning transition controlled by inspectable condition syntax.</summary>
    let attemptWhen
        (condition: Condition<'State, 'Event>)
        (transform: 'State -> 'Event -> Result<'Action list * 'State, 'Err>)
        : Rule<'State, 'Event, 'Action, 'Err> =
        { Syntax = RuleSyntax.Attempt(Condition.syntax condition)
          Invoke =
            fun (state, event) ->
                if Condition.matches condition state event then
                    match transform state event with
                    | Ok(actions, nextState) -> RuleVerdict.Handled(actions, RuleOutcome.Next nextState)
                    | Result.Error err -> RuleVerdict.Rejected err
                else
                    RuleVerdict.NoMatch }

    /// <summary>Result-returning transition controlled by an opaque legacy predicate.</summary>
    let attempt predicate transform =
        attemptWhen (Condition.custom "legacy.attempt" predicate) transform

    /// <summary>Compound-target transition controlled by inspectable condition syntax.</summary>
    let gotoWhen
        (condition: Condition<'State, 'Event>)
        (target: StateId)
        (transform: 'State -> 'Event -> 'Action list * 'State)
        : Rule<'State, 'Event, 'Action, 'Err> =
        { Syntax = RuleSyntax.Goto(Condition.syntax condition, target)
          Invoke =
            fun (state, event) ->
                if Condition.matches condition state event then
                    let actions, nextState = transform state event
                    RuleVerdict.Handled(actions, RuleOutcome.Goto(target, nextState))
                else
                    RuleVerdict.NoMatch }

    /// <summary>Compound-target transition controlled by an opaque legacy predicate.</summary>
    let goto predicate target transform =
        gotoWhen (Condition.custom "legacy.goto" predicate) target transform

    /// <summary>Internal transition controlled by inspectable condition syntax.</summary>
    let internalWhen
        (condition: Condition<'State, 'Event>)
        (transform: 'State -> 'Event -> 'Action list)
        : Rule<'State, 'Event, 'Action, 'Err> =
        { Syntax = RuleSyntax.Internal(Condition.syntax condition)
          Invoke =
            fun (state, event) ->
                if Condition.matches condition state event then
                    RuleVerdict.Handled(transform state event, RuleOutcome.Internal)
                else
                    RuleVerdict.NoMatch }

    /// <summary>Internal transition controlled by an opaque legacy predicate.</summary>
    let internalOn predicate transform =
        internalWhen (Condition.custom "legacy.internal" predicate) transform

    /// <summary>
    /// Guard combinator controlled by inspectable condition syntax. When it refuses, the rule reports
    /// <see cref="F:ByzantineSystems.Automata.Core.RuleVerdict`3.GuardFailed" /> instead of
    /// silently not matching; the search stops with a reportable reason rather than bubbling.
    /// </summary>
    let guardedWhen
        (condition: Condition<'State, 'Event>)
        (reason: string)
        (rule: Rule<'State, 'Event, 'Action, 'Err>)
        : Rule<'State, 'Event, 'Action, 'Err> =
        { Syntax = RuleSyntax.Guarded(reason, Condition.syntax condition, rule.Syntax)
          Invoke =
            fun (state, event) ->
                if Condition.matches condition state event then
                    rule.Invoke(state, event)
                else
                    RuleVerdict.GuardFailed reason }

    /// <summary>Guard combinator controlled by an opaque legacy predicate.</summary>
    let guarded predicate reason rule =
        guardedWhen (Condition.custom "legacy.guard" predicate) reason rule

    let rec private kindOf syntax =
        match syntax with
        | RuleSyntax.Transition _ -> RuleKind.Transition
        | RuleSyntax.Attempt _ -> RuleKind.Attempt
        | RuleSyntax.Goto(_, target) -> RuleKind.Goto target
        | RuleSyntax.Internal _ -> RuleKind.Internal
        | RuleSyntax.Guarded(reason, _, inner) -> RuleKind.Guarded(reason, kindOf inner)

    /// <summary>
    /// Compatibility projection naming the combinator that built a rule. Use <c>syntax</c> when
    /// the normalized condition structure is also required.
    /// </summary>
    let kind (rule: Rule<'State, 'Event, 'Action, 'Err>) : RuleKind = kindOf rule.Syntax

    /// <summary>The rule's normalized, function-free structural syntax.</summary>
    let syntax (rule: Rule<'State, 'Event, 'Action, 'Err>) : RuleSyntax = rule.Syntax

    /// <summary>
    /// The compound node a rule sends the entity to, looking through any guards, or <c>None</c>
    /// for a rule that does not goto anywhere. Read from the kind, so a chart can check every
    /// target it names without firing a single rule.
    /// </summary>
    let target (rule: Rule<'State, 'Event, 'Action, 'Err>) : StateId option =
        let rec targetOf syntax =
            match syntax with
            | RuleSyntax.Goto(_, target) -> Some target
            | RuleSyntax.Guarded(_, _, inner) -> targetOf inner
            | RuleSyntax.Transition _
            | RuleSyntax.Attempt _
            | RuleSyntax.Internal _ -> None

        targetOf rule.Syntax

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
        let rec renameSyntax syntax =
            match syntax with
            | RuleSyntax.Goto(condition, target) -> RuleSyntax.Goto(condition, rename target)
            | RuleSyntax.Guarded(reason, condition, inner) -> RuleSyntax.Guarded(reason, condition, renameSyntax inner)
            | RuleSyntax.Transition _
            | RuleSyntax.Attempt _
            | RuleSyntax.Internal _ -> syntax

        let renameVerdict verdict =
            match verdict with
            | RuleVerdict.Handled(actions, RuleOutcome.Goto(target, state)) ->
                RuleVerdict.Handled(actions, RuleOutcome.Goto(rename target, state))
            | other -> other

        { Syntax = renameSyntax rule.Syntax
          Invoke = rule.Invoke >> renameVerdict }

    /// <summary>Applies a rule to one (state, event) pair.</summary>
    let invoke
        (rule: Rule<'State, 'Event, 'Action, 'Err>)
        (state: 'State)
        (event: 'Event)
        : RuleVerdict<'Action, 'State, 'Err> =
        rule.Invoke(state, event)
