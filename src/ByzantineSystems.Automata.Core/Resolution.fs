namespace ByzantineSystems.Automata.Core

/// <summary>
/// The outcome of resolving one event against a chart: who handled it, the exit and entry
/// paths around the least common ancestor, the concatenated action list (exit ▸ transition ▸
/// entry), and the concrete next state.
/// </summary>
type Resolution<'State, 'Action> =
    {
        /// <summary>Which node in the chain actually handled the event.</summary>
        HandledBy: StateId

        /// <summary>States exited, innermost first. Empty for an internal transition.</summary>
        Exited: StateId list

        /// <summary>States entered, outermost first. Empty for an internal transition.</summary>
        Entered: StateId list

        /// <summary>Exit actions, then the rule's own, then entry actions, in execution order.</summary>
        Actions: 'Action list

        /// <summary>The concrete next state the entity will hold after commit.</summary>
        Next: 'State
    }

/// <summary>
/// Pure hierarchy algorithms over a parent function. Kept free of the
/// <see cref="T:ByzantineSystems.Automata.Core.Chart`4" /> type so they are testable in
/// isolation and reusable if the node representation changes.
/// </summary>
[<RequireQualifiedAccess>]
module Hierarchy =

    /// <summary>The chain from a node to the root, both inclusive: [node; parent; ...; root].</summary>
    let chain (parent: StateId -> StateId option) (node: StateId) : StateId list =
        let rec go acc current =
            match parent current with
            | Some p -> go (p :: acc) p
            | None -> acc

        node :: List.rev (go [] node)

    /// <summary>The prefix of <c>chain node</c> up to and including <c>upto</c> (which must be an ancestor-or-self).</summary>
    let pathUpTo (parent: StateId -> StateId option) (upto: StateId) (node: StateId) : StateId list =
        (chain parent node |> List.takeWhile (fun id -> id <> upto)) @ [ upto ]

    /// <summary>True when <c>ancestor</c> appears in <c>chain node</c>.</summary>
    let isAncestorOrSelf (parent: StateId -> StateId option) (ancestor: StateId) (node: StateId) : bool =
        chain parent node |> List.contains ancestor

    /// <summary>
    /// The deepest node present in both chains. With <c>a = b</c> the result is <c>a</c> itself.
    /// </summary>
    let leastCommonAncestor (parent: StateId -> StateId option) (a: StateId) (b: StateId) : StateId =
        let ancestorsOfA = chain parent a |> Set.ofList

        chain parent b |> List.find (fun id -> Set.contains id ancestorsOfA)
