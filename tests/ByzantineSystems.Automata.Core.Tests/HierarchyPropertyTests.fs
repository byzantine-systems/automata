module ByzantineSystems.Automata.Core.Tests.HierarchyPropertyTests

open ByzantineSystems.Automata.Core
open Expecto

type private Tree =
    { Root: StateId
      Nodes: StateId list
      Parent: StateId -> StateId option }

/// Node zero is the root. Every later node chooses a lower-indexed parent, so every generated
/// parent function is connected and acyclic by construction.
let private buildTree (shape: int list) =
    let idOf i = stateId $"n%d{i}"
    let root = idOf 0
    let nodes = [ 0 .. shape.Length ] |> List.map idOf

    let parentEntries =
        shape
        |> List.mapi (fun offset seed ->
            let childIndex = offset + 1
            idOf childIndex, Some(idOf (Property.bounded childIndex seed)))

    let parents = Map.ofList ((root, None) :: parentEntries)

    { Root = root
      Nodes = nodes
      Parent = fun id -> Map.find id parents }

let private choose seed (items: 'a list) =
    items.[Property.bounded items.Length seed]

let tests =
    testList
        "hierarchy laws"
        [ Property.property "least common ancestor is commutative" (fun (shape: int list, aSeed: int, bSeed: int) ->
              let tree = buildTree shape
              let a = choose aSeed tree.Nodes
              let b = choose bSeed tree.Nodes

              Hierarchy.leastCommonAncestor tree.Parent a b = Hierarchy.leastCommonAncestor tree.Parent b a)

          Property.property "a node is its own least common ancestor" (fun (shape: int list, nodeSeed: int) ->
              let tree = buildTree shape
              let node = choose nodeSeed tree.Nodes

              Hierarchy.leastCommonAncestor tree.Parent node node = node)

          Property.property
              "chain starts at the node, ends at the root, is parent-linked and duplicate-free"
              (fun (shape: int list, nodeSeed: int) ->
                  let tree = buildTree shape
                  let node = choose nodeSeed tree.Nodes
                  let chain = Hierarchy.chain tree.Parent node

                  List.head chain = node
                  && List.last chain = tree.Root
                  && (chain
                      |> List.pairwise
                      |> List.forall (fun (child, parent) -> tree.Parent child = Some parent))
                  && List.length chain = List.length (List.distinct chain))

          Property.property
              "pathUpTo is the prefix of chain ending at the selected ancestor"
              (fun (shape: int list, nodeSeed: int, ancestorSeed: int) ->
                  let tree = buildTree shape
                  let node = choose nodeSeed tree.Nodes
                  let chain = Hierarchy.chain tree.Parent node
                  let ancestorIndex = Property.bounded chain.Length ancestorSeed
                  let ancestor = chain.[ancestorIndex]

                  Hierarchy.pathUpTo tree.Parent ancestor node = List.take (ancestorIndex + 1) chain) ]
