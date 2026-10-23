namespace ByzantineSystems.Automata.Core

/// <summary>
/// The inspectable, normalized description of a condition. Predicate implementations are never
/// represented here: their names are caller-supplied semantic identifiers, not hashes of delegates.
/// </summary>
[<RequireQualifiedAccess>]
type ConditionSyntax =
    | Always
    | Never
    | AllOf of ConditionSyntax list
    | AnyOf of ConditionSyntax list
    | Not of ConditionSyntax
    | EventIs of name: string
    | StateIs of name: string
    | Both of name: string
    | Custom of name: string

/// <summary>
/// A condition pairs normalized, inspectable syntax with the predicate that interprets it. Build
/// values through <see cref="T:ByzantineSystems.Automata.Core.Condition" /> module functions so
/// equivalent conjunction and disjunction spellings receive the same syntax.
/// </summary>
[<NoEquality; NoComparison>]
type Condition<'State, 'Event> = private Condition of syntax: ConditionSyntax * matches: ('State -> 'Event -> bool)

/// <summary>Construction, normalization, inspection, and interpretation of conditions.</summary>
[<RequireQualifiedAccess>]
module Condition =

    let rec private normalize syntax =
        match syntax with
        | ConditionSyntax.Always
        | ConditionSyntax.Never
        | ConditionSyntax.EventIs _
        | ConditionSyntax.StateIs _
        | ConditionSyntax.Both _
        | ConditionSyntax.Custom _ -> syntax
        | ConditionSyntax.Not inner ->
            match normalize inner with
            | ConditionSyntax.Always -> ConditionSyntax.Never
            | ConditionSyntax.Never -> ConditionSyntax.Always
            | ConditionSyntax.Not nested -> nested
            | normalized -> ConditionSyntax.Not normalized
        | ConditionSyntax.AllOf terms -> normalizeAll terms
        | ConditionSyntax.AnyOf terms -> normalizeAny terms

    and private normalizeAll terms =
        let flattened =
            terms
            |> List.map normalize
            |> List.collect (function
                | ConditionSyntax.AllOf nested -> nested
                | term -> [ term ])

        if flattened |> List.contains ConditionSyntax.Never then
            ConditionSyntax.Never
        else
            match flattened |> List.filter ((<>) ConditionSyntax.Always) |> List.sort with
            | [] -> ConditionSyntax.Always
            | [ term ] -> term
            | normalized -> ConditionSyntax.AllOf normalized

    and private normalizeAny terms =
        let flattened =
            terms
            |> List.map normalize
            |> List.collect (function
                | ConditionSyntax.AnyOf nested -> nested
                | term -> [ term ])

        if flattened |> List.contains ConditionSyntax.Always then
            ConditionSyntax.Always
        else
            match flattened |> List.filter ((<>) ConditionSyntax.Never) |> List.sort with
            | [] -> ConditionSyntax.Never
            | [ term ] -> term
            | normalized -> ConditionSyntax.AnyOf normalized

    let private named caseName name syntax predicate =
        if System.String.IsNullOrWhiteSpace name then
            invalidArg (nameof name) $"A {caseName} condition requires a non-empty stable name."

        Condition(syntax name, predicate)

    /// <summary>A condition that matches every state and event.</summary>
    let always<'State, 'Event> : Condition<'State, 'Event> =
        Condition(ConditionSyntax.Always, fun _ _ -> true)

    /// <summary>A condition that never matches.</summary>
    let never<'State, 'Event> : Condition<'State, 'Event> =
        Condition(ConditionSyntax.Never, fun _ _ -> false)

    /// <summary>A named predicate over only the event.</summary>
    let eventIs (name: string) (predicate: 'Event -> bool) : Condition<'State, 'Event> =
        named "EventIs" name ConditionSyntax.EventIs (fun _ event -> predicate event)

    /// <summary>A named predicate over only the state.</summary>
    let stateIs (name: string) (predicate: 'State -> bool) : Condition<'State, 'Event> =
        named "StateIs" name ConditionSyntax.StateIs (fun state _ -> predicate state)

    /// <summary>A named predicate that observes both state and event.</summary>
    let both (name: string) (predicate: 'State -> 'Event -> bool) : Condition<'State, 'Event> =
        named "Both" name ConditionSyntax.Both predicate

    /// <summary>
    /// An explicitly opaque predicate. Its name is inspectable and fingerprinted; its behavior is
    /// not. Reusing one name for different predicates is therefore a broken caller contract.
    /// </summary>
    let custom (name: string) (predicate: 'State -> 'Event -> bool) : Condition<'State, 'Event> =
        named "Custom" name ConditionSyntax.Custom predicate

    /// <summary>Conjunction. Empty, singleton, nested, identity, and annihilator forms normalize.</summary>
    let allOf (conditions: Condition<'State, 'Event> list) : Condition<'State, 'Event> =
        let syntax =
            conditions
            |> List.map (fun (Condition(syntax, _)) -> syntax)
            |> ConditionSyntax.AllOf
            |> normalize

        Condition(syntax, fun state event -> conditions |> List.forall (fun (Condition(_, f)) -> f state event))

    /// <summary>Disjunction. Empty, singleton, nested, identity, and annihilator forms normalize.</summary>
    let anyOf (conditions: Condition<'State, 'Event> list) : Condition<'State, 'Event> =
        let syntax =
            conditions
            |> List.map (fun (Condition(syntax, _)) -> syntax)
            |> ConditionSyntax.AnyOf
            |> normalize

        Condition(syntax, fun state event -> conditions |> List.exists (fun (Condition(_, f)) -> f state event))

    /// <summary>Boolean negation. Constants and double negation normalize.</summary>
    let negate (condition: Condition<'State, 'Event>) : Condition<'State, 'Event> =
        let (Condition(syntax, predicate)) = condition
        Condition(normalize (ConditionSyntax.Not syntax), fun state event -> not (predicate state event))

    /// <summary>Evaluates a condition for one state/event pair.</summary>
    let matches (condition: Condition<'State, 'Event>) (state: 'State) (event: 'Event) : bool =
        let (Condition(_, predicate)) = condition
        predicate state event

    /// <summary>Returns the normalized, function-free syntax used by structural observations.</summary>
    let syntax (condition: Condition<'State, 'Event>) : ConditionSyntax =
        let (Condition(syntax, _)) = condition
        syntax

    /// <summary>True when the syntax contains an explicitly opaque Custom predicate.</summary>
    let containsCustom (condition: Condition<'State, 'Event>) : bool =
        let rec contains syntax =
            match syntax with
            | ConditionSyntax.Custom _ -> true
            | ConditionSyntax.AllOf terms
            | ConditionSyntax.AnyOf terms -> List.exists contains terms
            | ConditionSyntax.Not inner -> contains inner
            | ConditionSyntax.Always
            | ConditionSyntax.Never
            | ConditionSyntax.EventIs _
            | ConditionSyntax.StateIs _
            | ConditionSyntax.Both _ -> false

        condition |> syntax |> contains
