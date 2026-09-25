namespace ByzantineSystems.Automata.Core

open System
open System.Security.Cryptography
open System.Text

/// <summary>
/// One node of a state chart. A node with children (some other node declares it as
/// <c>Parent</c>) is compound and must declare <c>InitialChild</c>. Build nodes with the
/// <c>statechart</c> computation expression or directly; either way
/// <see cref="M:ByzantineSystems.Automata.Core.Chart.create" /> validates the result.
/// </summary>
type Node<'State, 'Event, 'Action, 'Err> =
    { Id: StateId
      Parent: StateId option
      InitialChild: StateId option
      Terminal: bool
      OnEntry: 'State -> 'Event -> 'Action list
      OnExit: 'State -> 'Event -> 'Action list
      Rules: Rule<'State, 'Event, 'Action, 'Err> list }

/// <summary>
/// Structural defects accumulated by <see cref="M:ByzantineSystems.Automata.Core.Chart.create" />.
/// <c>MissingRoot</c> and <c>MissingClassify</c> are raised by the computation expression when
/// those declarations are absent; every other case describes an invalid node graph.
/// </summary>
type ChartError =
    | DuplicateState of StateId
    | UnknownParent of child: StateId * parent: StateId
    | CyclicHierarchy of StateId list
    | CompoundWithoutInitial of StateId
    | InitialNotAChild of parent: StateId * child: StateId
    | UnreachableState of StateId
    | InvalidTerminal of StateId
    | MissingRoot
    | MissingClassify
    /// <summary>
    /// A goto rule on <paramref name="from" /> names a node the chart does not declare. Without
    /// this the rule builds, and fails only when an event first reaches it.
    /// </summary>
    | UnknownGotoTarget of from: StateId * target: StateId

/// <summary>
/// A validated state chart: an invalid hierarchy cannot exist as a value. The private
/// constructor is reachable only through
/// <see cref="M:ByzantineSystems.Automata.Core.Chart.create" />.
/// </summary>
[<NoEquality; NoComparison>]
type Chart<'State, 'Event, 'Action, 'Err> =
    private
        { Nodes: Map<StateId, Node<'State, 'Event, 'Action, 'Err>>
          Root: StateId
          Classify: 'State -> StateId
          Children: Map<StateId, StateId list> }

/// <summary>Construction, queries, and event resolution.</summary>
[<RequireQualifiedAccess>]
module Chart =

    /// <summary>
    /// The only way to build a <see cref="T:ByzantineSystems.Automata.Core.Chart`4" />.
    /// Accumulates every structural defect instead of failing on the first: an empty error
    /// list yields the chart. The root node is exempt from
    /// <see cref="F:ByzantineSystems.Automata.Core.ChartError.CompoundWithoutInitial" />
    /// because the machine configuration, not the chart, supplies the initial state.
    /// </summary>
    let create
        (root: StateId)
        (classify: 'State -> StateId)
        (nodes: Node<'State, 'Event, 'Action, 'Err> list)
        : Result<Chart<'State, 'Event, 'Action, 'Err>, ChartError list> =
        let duplicates =
            nodes
            |> List.groupBy (fun n -> n.Id)
            |> List.filter (fun (_, group) -> group.Length > 1)
            |> List.map (fun (id, _) -> ChartError.DuplicateState id)

        let byId = nodes |> List.map (fun n -> n.Id, n) |> Map.ofList

        let children =
            nodes
            |> List.choose (fun n -> n.Parent |> Option.map (fun p -> p, n.Id))
            |> List.groupBy fst
            |> List.map (fun (p, kids) -> p, kids |> List.map snd)
            |> Map.ofList

        let parentOf (id: StateId) =
            byId.TryFind id |> Option.bind (fun n -> n.Parent)

        let kidsOf (id: StateId) =
            children.TryFind id |> Option.defaultValue []

        let unknownParents =
            nodes
            |> List.choose (fun n ->
                match n.Parent with
                | Some p when not (byId.ContainsKey p) -> Some(ChartError.UnknownParent(n.Id, p))
                | _ -> None)

        // Chase parent links once per node; a revisit within the walk is a cycle.
        let cycles =
            nodes
            |> List.choose (fun n ->
                let rec chase walked current =
                    match parentOf current with
                    | None -> None
                    | Some p when List.contains p walked -> Some(walked @ [ p ])
                    | Some p -> chase (walked @ [ p ]) p

                chase [ n.Id ] n.Id)
            |> List.map (fun walked ->
                let head = List.last walked
                walked |> List.skipWhile (fun id -> id <> head))
            |> List.distinct
            |> List.map ChartError.CyclicHierarchy

        let invalidTerminal =
            nodes
            |> List.choose (fun n ->
                let hasKids = kidsOf n.Id |> List.isEmpty |> not

                if n.Terminal && (hasKids || not n.Rules.IsEmpty || n.InitialChild <> None) then
                    Some(ChartError.InvalidTerminal n.Id)
                else
                    None)

        let structure =
            nodes
            |> List.collect (fun n ->
                let kids = kidsOf n.Id

                if not kids.IsEmpty then
                    [ if not (n.Id = root) && n.InitialChild = None then
                          ChartError.CompoundWithoutInitial n.Id

                      match n.InitialChild with
                      | Some c when (byId.TryFind c |> Option.bind (fun m -> m.Parent)) <> Some n.Id ->
                          ChartError.InitialNotAChild(n.Id, c)
                      | _ -> () ]
                else
                    [ match n.InitialChild with
                      | Some c -> ChartError.InitialNotAChild(n.Id, c)
                      | _ -> () ])

        let unreachable =
            let rec bfs frontier seen =
                let next =
                    frontier
                    |> List.collect kidsOf
                    |> List.filter (fun id -> not (Set.contains id seen))
                    |> Set.ofList

                if next.IsEmpty then
                    seen
                else
                    bfs (Set.toList next) (Set.union seen next)

            let seen = bfs [ root ] (Set [ root ])

            nodes
            |> List.filter (fun n -> not (Set.contains n.Id seen))
            |> List.map (fun n -> ChartError.UnreachableState n.Id)

        let unknownTargets =
            nodes
            |> List.collect (fun n ->
                n.Rules
                |> List.choose Rule.target
                |> List.filter (fun target -> not (byId.ContainsKey target))
                |> List.map (fun target -> ChartError.UnknownGotoTarget(n.Id, target)))

        let rootErrors =
            match byId.TryFind root with
            | None -> [ ChartError.MissingRoot ]
            | Some rootNode ->
                match rootNode.Parent with
                | Some p -> [ ChartError.CyclicHierarchy [ root; p ] ]
                | None -> []

        let errors =
            duplicates
            @ unknownParents
            @ cycles
            @ invalidTerminal
            @ structure
            @ unreachable
            @ unknownTargets
            @ rootErrors

        if errors.IsEmpty then
            Ok
                { Nodes = byId
                  Root = root
                  Classify = classify
                  Children = children }
        else
            Result.Error errors

    /// The canonicalisation this fingerprint scheme describes. It is hashed along with the chart,
    /// so a future scheme cannot produce the same digest as this one for a different structure.
    /// Bump it in the same commit that changes what is hashed below.
    let private fingerprintScheme = "automata.chart.fingerprint/1"

    /// Appends a length-prefixed field. Length prefixes rather than a delimiter with escaping:
    /// the encoding stays injective without having to defend against a state id that happens to
    /// contain whatever the delimiter is.
    let private appendField (builder: StringBuilder) (value: string) =
        builder.Append(value.Length).Append(':').Append(value) |> ignore

    let rec private appendKind (builder: StringBuilder) (kind: RuleKind) =
        match kind with
        | RuleKind.Transition -> builder.Append 't' |> ignore
        | RuleKind.Attempt -> builder.Append 'a' |> ignore
        | RuleKind.Internal -> builder.Append 'i' |> ignore
        | RuleKind.Goto target ->
            builder.Append 'g' |> ignore
            appendField builder (StateId.value target)
        | RuleKind.Guarded(reason, inner) ->
            builder.Append 'u' |> ignore
            appendField builder reason
            appendKind builder inner

    /// <summary>
    /// Hashes the chart's structure: the root, and for every node its id, parent, initial child,
    /// terminal flag and the kind of each of its rules.
    ///
    /// Two things about the ordering carry the meaning. Nodes are sorted by id, because sibling
    /// declaration order decides nothing at resolution time (the walk follows parent links, and
    /// an initial child is named explicitly), so two spellings of one chart must agree. Rules are
    /// left in declaration order, because first-match-wins makes their order part of what the
    /// chart does.
    ///
    /// What is deliberately invisible: the classifier, every predicate and transform, and entry
    /// and exit actions. All are closures. The builder has also already composed several
    /// <c>onEntry</c> declarations into one function by the time a chart exists, so not even
    /// their number survives. This is why the fingerprint is a tripwire against editing a chart
    /// without bumping its version, and not a proof that two charts decide alike.
    /// </summary>
    let fingerprint (chart: Chart<'State, 'Event, 'Action, 'Err>) : ChartFingerprint =
        let builder = StringBuilder()
        builder.Append(fingerprintScheme).Append('\n').Append('r') |> ignore
        appendField builder (StateId.value chart.Root)
        builder.Append '\n' |> ignore

        chart.Nodes
        |> Map.toList
        |> List.map snd
        |> List.sortWith (fun left right -> String.CompareOrdinal(StateId.value left.Id, StateId.value right.Id))
        |> List.iter (fun node ->
            builder.Append 'n' |> ignore
            appendField builder (StateId.value node.Id)

            match node.Parent with
            | Some parent ->
                builder.Append 'p' |> ignore
                appendField builder (StateId.value parent)
            | None -> builder.Append '-' |> ignore

            match node.InitialChild with
            | Some child ->
                builder.Append 'i' |> ignore
                appendField builder (StateId.value child)
            | None -> builder.Append '-' |> ignore

            builder.Append(if node.Terminal then '1' else '0') |> ignore
            builder.Append('k').Append(node.Rules.Length).Append(':') |> ignore
            node.Rules |> List.iter (fun rule -> appendKind builder (Rule.kind rule))
            builder.Append '\n' |> ignore)

        // Hashed explicitly rather than through GetHashCode, which .NET randomises per process:
        // two pods running one chart have to agree, and so does a value stored last week.
        Encoding.UTF8.GetBytes(builder.ToString())
        |> SHA256.HashData
        |> Convert.ToHexStringLower
        |> ChartFingerprint.create

    /// <summary>The chart's root node id.</summary>
    let root (chart: Chart<'State, 'Event, 'Action, 'Err>) : StateId = chart.Root

    /// <summary>The classifier supplied at construction.</summary>
    let classifyOf (chart: Chart<'State, 'Event, 'Action, 'Err>) (state: 'State) : StateId = chart.Classify state

    /// <summary>Looks a node up by id.</summary>
    let tryNode
        (chart: Chart<'State, 'Event, 'Action, 'Err>)
        (id: StateId)
        : Node<'State, 'Event, 'Action, 'Err> option =
        chart.Nodes.TryFind id

    /// <summary>The declared children of a node, in declaration order.</summary>
    let children (chart: Chart<'State, 'Event, 'Action, 'Err>) (id: StateId) : StateId list =
        chart.Children.TryFind id |> Option.defaultValue []

    /// Recognises a declared node and exposes its definition to a match expression.
    let private (|DeclaredNode|_|) (chart: Chart<'State, 'Event, 'Action, 'Err>) (id: StateId) = chart.Nodes.TryFind id

    /// Recognises only declared leaf nodes. Resolution may start and finish only at a leaf.
    let private (|LeafNode|_|) (chart: Chart<'State, 'Event, 'Action, 'Err>) (id: StateId) =
        match id with
        | DeclaredNode chart node when children chart id |> List.isEmpty -> Some node
        | _ -> None

    /// <summary>A compound node has children; a leaf does not.</summary>
    let isCompound (chart: Chart<'State, 'Event, 'Action, 'Err>) (id: StateId) : bool =
        children chart id |> List.isEmpty |> not

    /// <summary>True when the classifier maps the state to a terminal leaf.</summary>
    let isTerminal (chart: Chart<'State, 'Event, 'Action, 'Err>) (state: 'State) : bool =
        match chart.Classify state with
        | LeafNode chart node -> node.Terminal
        | _ -> false

    /// <summary>
    /// Resolves one event from the current state: bubbling from the classified leaf outward,
    /// first matching rule wins, exit/entry paths around the least common ancestor. Expected
    /// failures are typed; user callbacks that throw propagate (the runtime boundary owns them).
    /// </summary>
    let resolve
        (chart: Chart<'State, 'Event, 'Action, 'Err>)
        (state: 'State)
        (event: 'Event)
        : Result<Resolution<'State, 'Action>, TransitionError<'Err>> =
        let parentOf (id: StateId) =
            chart.Nodes.TryFind id |> Option.bind (fun n -> n.Parent)

        let leafId = chart.Classify state

        match leafId with
        | LeafNode chart _ ->
            let chain = Hierarchy.chain parentOf leafId

            let pathsFor targetLeaf =
                let lca = Hierarchy.leastCommonAncestor parentOf leafId targetLeaf

                let exited = Hierarchy.chain parentOf leafId |> List.takeWhile (fun id -> id <> lca)

                let entered =
                    Hierarchy.chain parentOf targetLeaf
                    |> List.takeWhile (fun id -> id <> lca)
                    |> List.rev

                exited, entered

            let commit handlerId ruleActions nextState exited entered =
                let exitActions =
                    exited |> List.collect (fun id -> chart.Nodes.[id].OnExit state event)

                let entryActions =
                    entered |> List.collect (fun id -> chart.Nodes.[id].OnEntry nextState event)

                Ok
                    { HandledBy = handlerId
                      Exited = exited
                      Entered = entered
                      Actions = exitActions @ ruleActions @ entryActions
                      Next = nextState }

            let rec walkChain =
                function
                | [] -> Result.Error(TransitionError.Unhandled(leafId, sprintf "%A" event))
                | handlerId :: rest ->
                    let node = chart.Nodes.[handlerId]

                    let rec tryRules =
                        function
                        | [] -> walkChain rest
                        | rule :: more ->
                            match Rule.invoke rule state event with
                            | RuleVerdict.NoMatch -> tryRules more
                            | RuleVerdict.GuardFailed reason ->
                                Result.Error(TransitionError.GuardFailed(handlerId, reason))
                            | RuleVerdict.Rejected err -> Result.Error(TransitionError.Rejected err)
                            | RuleVerdict.Handled(actions, outcome) ->
                                match outcome with
                                | RuleOutcome.Internal ->
                                    Ok
                                        { HandledBy = handlerId
                                          Exited = []
                                          Entered = []
                                          Actions = actions
                                          Next = state }
                                | RuleOutcome.Next nextState ->
                                    let target = chart.Classify nextState

                                    match target with
                                    | LeafNode chart _ ->
                                        let exited, entered =
                                            if target = leafId then
                                                // External self-transition: exit and re-enter the handling node chain.
                                                let path = Hierarchy.pathUpTo parentOf handlerId leafId
                                                path, List.rev path
                                            else
                                                pathsFor target

                                        commit handlerId actions nextState exited entered
                                    | _ -> Result.Error(TransitionError.UnknownState target)
                                | RuleOutcome.Goto(targetId, nextState) ->
                                    match targetId with
                                    | DeclaredNode chart _ ->
                                        // Descend the initial-child path to the concrete leaf.
                                        let rec descend current =
                                            match chart.Nodes.[current].InitialChild with
                                            | Some child -> descend child
                                            | None -> current

                                        let targetLeaf = descend targetId
                                        let actualLeaf = chart.Classify nextState

                                        if actualLeaf <> targetLeaf then
                                            Result.Error(TransitionError.TargetMismatch(targetLeaf, actualLeaf))
                                        else
                                            let exited, entered =
                                                if Hierarchy.isAncestorOrSelf parentOf targetId leafId then
                                                    // Ancestor transition: exit through the target inclusive, re-enter down the initial path.
                                                    let exited = Hierarchy.pathUpTo parentOf targetId leafId

                                                    let entered =
                                                        targetId
                                                        :: (Hierarchy.chain parentOf targetLeaf
                                                            |> List.takeWhile (fun id -> id <> targetId)
                                                            |> List.rev)

                                                    exited, entered
                                                else
                                                    pathsFor targetLeaf

                                            commit handlerId actions nextState exited entered
                                    | _ -> Result.Error(TransitionError.UnknownState targetId)

                    tryRules node.Rules

            walkChain chain
        | _ -> Result.Error(TransitionError.UnknownState leafId)
