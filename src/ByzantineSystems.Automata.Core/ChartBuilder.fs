namespace ByzantineSystems.Automata.Core

/// <summary>
/// A node under assembly, before parent links are assigned. Produced by
/// <see cref="T:ByzantineSystems.Automata.Core.NodeBuilder`4" /> blocks.
/// </summary>
type NodeDraft<'State, 'Event, 'Action, 'Err> =
    { Id: StateId
      InitialChild: StateId option
      Terminal: bool
      OnEntry: 'State -> 'Event -> 'Action list
      OnExit: 'State -> 'Event -> 'Action list
      Rules: Rule<'State, 'Event, 'Action, 'Err> list
      Children: NodeDraft<'State, 'Event, 'Action, 'Err> list }

/// <summary>One statement inside a <c>state</c> or <c>compound</c> block.</summary>
type NodePart<'State, 'Event, 'Action, 'Err> =
    | NodeRule of Rule<'State, 'Event, 'Action, 'Err>
    | EntryAction of ('State -> 'Event -> 'Action list)
    | ExitAction of ('State -> 'Event -> 'Action list)
    | InitialChildPart of StateId
    | TerminalPart
    | ChildNode of NodeDraft<'State, 'Event, 'Action, 'Err>

/// <summary>One statement at the top level of a <c>statechart</c> block.</summary>
type ChartPart<'State, 'Event, 'Action, 'Err> =
    | RootState of StateId
    | Classifier of classify: ('State -> StateId)
    | ChartNode of NodeDraft<'State, 'Event, 'Action, 'Err>

/// <summary>
/// Builder for the inside of <c>state "name" { ... }</c> and <c>compound "name" { ... }</c>.
/// Statements are plain values (rules, entry/exit actions, initial-child, terminal flag,
/// child nodes) implicitly yielded and combined; no custom operations, so implicit yields
/// stay enabled.
/// </summary>
type NodeBuilder<'State, 'Event, 'Action, 'Err>(name: string) =

    member _.Yield(part: NodePart<'State, 'Event, 'Action, 'Err>) : NodePart<'State, 'Event, 'Action, 'Err> list =
        [ part ]

    member _.Yield(draft: NodeDraft<'State, 'Event, 'Action, 'Err>) : NodePart<'State, 'Event, 'Action, 'Err> list =
        [ ChildNode draft ]

    /// <summary>Several child nodes at once, as a fragment library exports them.</summary>
    member _.Yield
        (drafts: NodeDraft<'State, 'Event, 'Action, 'Err> list)
        : NodePart<'State, 'Event, 'Action, 'Err> list =
        List.map ChildNode drafts

    /// <summary>The same, spliced with <c>yield!</c>.</summary>
    member _.YieldFrom
        (drafts: NodeDraft<'State, 'Event, 'Action, 'Err> list)
        : NodePart<'State, 'Event, 'Action, 'Err> list =
        List.map ChildNode drafts

    member _.Yield(()) : NodePart<'State, 'Event, 'Action, 'Err> list = []

    member _.Zero() : NodePart<'State, 'Event, 'Action, 'Err> list = []

    member _.Combine
        (parts: NodePart<'State, 'Event, 'Action, 'Err> list, rest: NodePart<'State, 'Event, 'Action, 'Err> list)
        : NodePart<'State, 'Event, 'Action, 'Err> list =
        parts @ rest

    member _.Delay
        (computation: unit -> NodePart<'State, 'Event, 'Action, 'Err> list)
        : NodePart<'State, 'Event, 'Action, 'Err> list =
        computation ()

    member _.Run(parts: NodePart<'State, 'Event, 'Action, 'Err> list) : NodeDraft<'State, 'Event, 'Action, 'Err> =
        let folder (entries, exits, rules, initial, terminal, children) part =
            match part with
            | NodeRule rule -> entries, exits, rule :: rules, initial, terminal, children
            | EntryAction f -> f :: entries, exits, rules, initial, terminal, children
            | ExitAction f -> entries, f :: exits, rules, initial, terminal, children
            | InitialChildPart id -> entries, exits, rules, Some id, terminal, children
            | TerminalPart -> entries, exits, rules, initial, true, children
            | ChildNode draft -> entries, exits, rules, initial, terminal, draft :: children

        let entries, exits, rules, initial, terminal, children =
            List.fold folder ([], [], [], None, false, []) parts

        let compose (fs: ('State -> 'Event -> 'Action list) list) =
            fun state event -> List.rev fs |> List.collect (fun f -> f state event)

        { Id = stateId name
          InitialChild = initial
          Terminal = terminal
          OnEntry = compose entries
          OnExit = compose exits
          Rules = List.rev rules
          Children = List.rev children }

/// <summary>
/// Builder for <c>statechart { ... }</c>. Returns
/// <c>Result&lt;Chart, ChartError list&gt;</c>: a missing root or classifier is a typed
/// construction error, never a runtime throw.
/// </summary>
type ChartBuilder<'State, 'Event, 'Action, 'Err>() =

    member _.Yield(part: ChartPart<'State, 'Event, 'Action, 'Err>) : ChartPart<'State, 'Event, 'Action, 'Err> list =
        [ part ]

    member _.Yield(draft: NodeDraft<'State, 'Event, 'Action, 'Err>) : ChartPart<'State, 'Event, 'Action, 'Err> list =
        [ ChartNode draft ]

    /// <summary>Several top-level nodes at once, as a fragment library exports them.</summary>
    member _.Yield
        (drafts: NodeDraft<'State, 'Event, 'Action, 'Err> list)
        : ChartPart<'State, 'Event, 'Action, 'Err> list =
        List.map ChartNode drafts

    /// <summary>The same, spliced with <c>yield!</c>.</summary>
    member _.YieldFrom
        (drafts: NodeDraft<'State, 'Event, 'Action, 'Err> list)
        : ChartPart<'State, 'Event, 'Action, 'Err> list =
        List.map ChartNode drafts

    member _.Yield(()) : ChartPart<'State, 'Event, 'Action, 'Err> list = []

    member _.Zero() : ChartPart<'State, 'Event, 'Action, 'Err> list = []

    member _.Combine
        (parts: ChartPart<'State, 'Event, 'Action, 'Err> list, rest: ChartPart<'State, 'Event, 'Action, 'Err> list)
        : ChartPart<'State, 'Event, 'Action, 'Err> list =
        parts @ rest

    member _.Delay
        (computation: unit -> ChartPart<'State, 'Event, 'Action, 'Err> list)
        : ChartPart<'State, 'Event, 'Action, 'Err> list =
        computation ()

    member _.Run
        (parts: ChartPart<'State, 'Event, 'Action, 'Err> list)
        : Result<Chart<'State, 'Event, 'Action, 'Err>, ChartError list> =
        let rootId =
            parts
            |> List.tryPick (function
                | RootState id -> Some id
                | _ -> None)

        let classifier =
            parts
            |> List.tryPick (function
                | Classifier f -> Some f
                | _ -> None)

        match rootId, classifier with
        | Some root, Some classify ->
            let drafts =
                parts
                |> List.choose (function
                    | ChartNode d -> Some d
                    | _ -> None)

            // The root is an implicit container node; the machine supplies the initial state.
            let rootDraft: NodeDraft<'State, 'Event, 'Action, 'Err> =
                { Id = root
                  InitialChild = None
                  Terminal = false
                  OnEntry = fun _ _ -> []
                  OnExit = fun _ _ -> []
                  Rules = []
                  Children = drafts }

            let rec flatten parent (draft: NodeDraft<'State, 'Event, 'Action, 'Err>) =
                { Id = draft.Id
                  Parent = parent
                  InitialChild = draft.InitialChild
                  Terminal = draft.Terminal
                  OnEntry = draft.OnEntry
                  OnExit = draft.OnExit
                  Rules = draft.Rules }
                :: List.collect (flatten (Some draft.Id)) draft.Children

            Chart.create root classify (flatten None rootDraft)
        | None, _ -> Result.Error [ ChartError.MissingRoot ]
        | Some _, None -> Result.Error [ ChartError.MissingClassify ]

/// <summary>
/// Reusing a piece of chart.
///
/// A fragment is an ordinary <see cref="T:ByzantineSystems.Automata.Core.NodeDraft`4" /> value:
/// build it once with <c>state</c> or <c>compound</c>, and yield it into as many charts as need
/// it. When one chart needs the same fragment twice, <c>prefix</c> gives each copy its own ids.
/// </summary>
[<RequireQualifiedAccess>]
module Fragment =

    let rec private declared (draft: NodeDraft<'State, 'Event, 'Action, 'Err>) : StateId list =
        draft.Id :: List.collect declared draft.Children

    /// <summary>
    /// Renames every node the fragment declares to <c>"&lt;prefix&gt;.&lt;id&gt;"</c>, along with
    /// every initial child and goto target that names one of them. A goto target the fragment
    /// does not declare is left alone, so a fragment can still send an entity to a state its
    /// host chart owns.
    ///
    /// The classifier cannot be renamed, because it is the host's own code, so it must return
    /// the prefixed ids for states inside a prefixed fragment. A classifier that forgets is
    /// caught rather than trusted: the machine refuses to build when its initial state
    /// classifies to an undeclared node, and resolution reports <c>UnknownState</c> for any other.
    /// A goto the rewrite missed is refused when the chart is built, as
    /// <c>ChartError.UnknownGotoTarget</c>.
    /// </summary>
    /// <exception cref="T:System.ArgumentException">The prefix is empty or whitespace.</exception>
    let prefix
        (prefix: string)
        (draft: NodeDraft<'State, 'Event, 'Action, 'Err>)
        : NodeDraft<'State, 'Event, 'Action, 'Err> =
        if System.String.IsNullOrWhiteSpace prefix then
            invalidArg (nameof prefix) "A fragment prefix must be a non-empty string."

        let inside = declared draft |> Set.ofList

        let rename (id: StateId) =
            if Set.contains id inside then
                StateId.create $"{prefix.Trim()}.{StateId.value id}"
            else
                id

        let rec rewrite (node: NodeDraft<'State, 'Event, 'Action, 'Err>) =
            { node with
                Id = rename node.Id
                InitialChild = Option.map rename node.InitialChild
                Rules = List.map (Rule.retarget rename) node.Rules
                Children = List.map rewrite node.Children }

        rewrite draft

/// <summary>Syntax surface for the <c>statechart</c> computation expression.</summary>
[<AutoOpen>]
module ChartCE =

    /// <summary>The chart builder value: <c>statechart&lt;S, E, A, Err&gt; { ... }</c>.</summary>
    let statechart<'State, 'Event, 'Action, 'Err> =
        ChartBuilder<'State, 'Event, 'Action, 'Err>()

    /// <summary>Declares the chart's root container: <c>root "root"</c>.</summary>
    let root (name: string) : ChartPart<'State, 'Event, 'Action, 'Err> = RootState(stateId name)

    /// <summary>Declares the state-to-node classifier.</summary>
    let classify (classifier: 'State -> StateId) : ChartPart<'State, 'Event, 'Action, 'Err> = Classifier classifier

    /// <summary>Declares a leaf state node: <c>state "idle" { ... }</c>.</summary>
    let state (name: string) : NodeBuilder<'State, 'Event, 'Action, 'Err> = NodeBuilder name

    /// <summary>Declares a compound state node: <c>compound "active" { ... }</c>.</summary>
    let compound (name: string) : NodeBuilder<'State, 'Event, 'Action, 'Err> = NodeBuilder name

    /// <summary>Inside a compound: the initial child path. The concrete state data still comes from rules.</summary>
    let initial (name: string) : NodePart<'State, 'Event, 'Action, 'Err> = InitialChildPart(stateId name)

    /// <summary>Inside a state: marks the node terminal (no rules, no children).</summary>
    let terminal<'State, 'Event, 'Action, 'Err> : NodePart<'State, 'Event, 'Action, 'Err> =
        TerminalPart

    /// <summary>Entry actions for a node; multiple declarations concatenate.</summary>
    let onEntry (action: 'State -> 'Event -> 'Action list) : NodePart<'State, 'Event, 'Action, 'Err> =
        EntryAction action

    /// <summary>Exit actions for a node; multiple declarations concatenate.</summary>
    let onExit (action: 'State -> 'Event -> 'Action list) : NodePart<'State, 'Event, 'Action, 'Err> = ExitAction action

    /// <summary>Ordinary transition rule.</summary>
    let on
        (predicate: 'State -> 'Event -> bool)
        (transform: 'State -> 'Event -> 'Action list * 'State)
        : NodePart<'State, 'Event, 'Action, 'Err> =
        NodeRule(Rule.transition predicate transform)

    /// <summary>Result-returning rule: may reject the event with a domain error.</summary>
    let attempt
        (predicate: 'State -> 'Event -> bool)
        (transform: 'State -> 'Event -> Result<'Action list * 'State, 'Err>)
        : NodePart<'State, 'Event, 'Action, 'Err> =
        NodeRule(Rule.attempt predicate transform)

    /// <summary>Compound-target rule: follows the target's initial-child path to a leaf.</summary>
    let goto
        (predicate: 'State -> 'Event -> bool)
        (target: StateId)
        (transform: 'State -> 'Event -> 'Action list * 'State)
        : NodePart<'State, 'Event, 'Action, 'Err> =
        NodeRule(Rule.goto predicate target transform)

    /// <summary>Internal transition rule: actions without exit/entry or a state change.</summary>
    let internalOn
        (predicate: 'State -> 'Event -> bool)
        (transform: 'State -> 'Event -> 'Action list)
        : NodePart<'State, 'Event, 'Action, 'Err> =
        NodeRule(Rule.internalOn predicate transform)
