module ByzantineSystems.Automata.Core.Tests.ChartPropertyTests

open ByzantineSystems.Automata.Core
open Expecto

/// A generated random tree under the chart's implicit root: node 0 is the top generated node and
/// node i's parent is some node j &lt; i, which keeps the graph acyclic and connected. Every node k
/// carries exactly one rule that matches event k and targets a fixed leaf, so the expected handler
/// of (leaf j, event k) is node k if k sits on j's chain, and Unhandled otherwise.
type private Tree =
    { Chart: Chart<int, int, int, string>
      Size: int
      Leaves: int list
      Target: int
      ParentIndex: int -> int option }

let private build (shape: int list) : Tree option =
    let size =
        match shape with
        | [] -> 1
        | head :: _ -> 1 + Property.bounded 12 head

    let parentOf (i: int) =
        if i = 0 then
            None
        else
            Some(Property.bounded i shape.[i % shape.Length])

    let idOf i = stateId $"n%d{i}"

    let isParentOfSomeone i =
        [ 0 .. size - 1 ] |> List.exists (fun j -> parentOf j = Some i)

    let leaves = [ 0 .. size - 1 ] |> List.filter (fun i -> not (isParentOfSomeone i))

    if leaves.IsEmpty then
        None
    else
        let target = leaves |> List.min

        let rec mkDraft (i: int) : NodeDraft<int, int, int, string> =
            let kids = [ 0 .. size - 1 ] |> List.filter (fun j -> parentOf j = Some i)

            let parts =
                [ if not kids.IsEmpty then
                      InitialChildPart(kids |> List.min |> idOf)

                  NodeRule(Rule.transition (fun _ event -> event = i) (fun _ _ -> [], target))

                  for child in kids do
                      ChildNode(mkDraft child) ]

            NodeBuilder<int, int, int, string>(StateId.value (idOf i)).Run parts

        let result =
            statechart<int, int, int, string> {
                root "root"
                classify idOf
                mkDraft 0
            }

        match result with
        | Ok chart ->
            Some
                { Chart = chart
                  Size = size
                  Leaves = leaves
                  Target = target
                  ParentIndex = parentOf }
        | Result.Error _ -> None

let private chainIndex (parentOf: int -> int option) (j: int) : int list =
    let rec go acc current =
        match parentOf current with
        | Some p -> go (p :: acc) p
        | None -> acc

    j :: go [] j

let private parentId (tree: Tree) (id: StateId) =
    Chart.tryNode tree.Chart id |> Option.bind (fun n -> n.Parent)

let private expectChart result =
    match result with
    | Ok chart -> chart
    | Result.Error errors -> failtestf "constructor coverage fixture is invalid: %A" errors

let private coverageTest =
    test "public constructor coverage reaches every Rule and NodePart family" {
        let leafId = stateId "leaf"

        let leafParts: NodePart<int, int, int, string> list =
            [ EntryAction(fun _ _ -> [ 1 ])
              ExitAction(fun _ _ -> [ 2 ])
              NodeRule(Rule.transition (fun _ _ -> false) (fun state _ -> [], state))
              NodeRule(Rule.attempt (fun _ _ -> false) (fun state _ -> Ok([], state)))
              NodeRule(Rule.internalOn (fun _ _ -> false) (fun _ _ -> []))
              NodeRule(Rule.goto (fun _ _ -> false) leafId (fun state _ -> [], state))
              NodeRule(
                  Rule.transition (fun _ _ -> false) (fun state _ -> [], state)
                  |> Rule.guarded (fun _ _ -> true) "covered"
              ) ]

        let leaf = NodeBuilder<int, int, int, string>("leaf").Run leafParts

        let terminal = NodeBuilder<int, int, int, string>("terminal").Run [ TerminalPart ]

        let parentParts: NodePart<int, int, int, string> list =
            [ InitialChildPart leafId; ChildNode leaf; ChildNode terminal ]

        let parent = NodeBuilder<int, int, int, string>("parent").Run parentParts

        let chart =
            statechart<int, int, int, string> {
                root "root"
                classify (fun _ -> leafId)
                parent
            }
            |> expectChart

        let expectNode id =
            Chart.tryNode chart id
            |> Option.defaultWith (fun () -> failtestf "coverage node %A was not built" id)

        let leafNode = expectNode leafId
        let terminalNode = expectNode (stateId "terminal")

        Expect.sequenceEqual
            (leafNode.Rules |> List.map Rule.kind)
            [ RuleKind.Transition
              RuleKind.Attempt
              RuleKind.Internal
              RuleKind.Goto leafId
              RuleKind.Guarded("covered", RuleKind.Transition) ]
            "every rule constructor has a public route"

        Expect.equal (leafNode.OnEntry 0 0) [ 1 ] "entry actions survive construction"
        Expect.equal (leafNode.OnExit 0 0) [ 2 ] "exit actions survive construction"
        Expect.equal terminalNode.Terminal true "the terminal part survives construction"

        Expect.sequenceEqual
            (Chart.children chart (stateId "parent"))
            [ leafId; stateId "terminal" ]
            "child parts survive construction"
    }

let tests =
    testList
        "chart properties"
        [ Property.property
              "nearest handler: Ok exactly when the event's node is on the chain"
              (fun (shape: int list, jSeed: int, kSeed: int) ->
                  match build shape with
                  | None -> false
                  | Some tree ->
                      let j = tree.Leaves.[Property.bounded tree.Leaves.Length jSeed]
                      let k = Property.bounded tree.Size kSeed
                      let onChain = chainIndex tree.ParentIndex j |> List.contains k

                      match Chart.resolve tree.Chart j k with
                      | Ok r -> onChain && r.HandledBy = stateId $"n%d{k}"
                      | Result.Error(Unhandled(leaf, _)) -> (not onChain) && leaf = stateId $"n%d{j}"
                      | Result.Error _ -> false)

          Property.property
              "exit/entry paths are parent-linked, start at the leaf, end at the target"
              (fun (shape: int list, jSeed: int, kSeed: int) ->
                  match build shape with
                  | None -> false
                  | Some tree ->
                      let j = tree.Leaves.[Property.bounded tree.Leaves.Length jSeed]
                      let k = Property.bounded tree.Size kSeed

                      if not (chainIndex tree.ParentIndex j |> List.contains k) then
                          true
                      else
                          match Chart.resolve tree.Chart j k with
                          | Result.Error _ -> false
                          | Ok r ->
                              let exitedLinked =
                                  r.Exited
                                  |> List.pairwise
                                  |> List.forall (fun (a, b) -> parentId tree a = Some b)

                              let enteredLinked =
                                  r.Entered
                                  |> List.pairwise
                                  |> List.forall (fun (a, b) -> parentId tree b = Some a)

                              let headIsLeaf =
                                  match r.Exited with
                                  | head :: _ -> head = stateId $"n%d{j}"
                                  | [] -> false

                              let lastIsTarget =
                                  match List.rev r.Entered with
                                  | last :: _ -> last = stateId $"n%d{tree.Target}"
                                  | [] -> false

                              exitedLinked && enteredLinked && headIsLeaf && lastIsTarget)

          Property.property "chains end at the root and are strictly parent-linked" (fun (shape: int list) ->
              match build shape with
              | None -> false
              | Some tree ->
                  let parentById (id: StateId) =
                      Chart.tryNode tree.Chart id |> Option.bind (fun n -> n.Parent)

                  let idOf i = stateId $"n%d{i}"

                  tree.Leaves
                  |> List.forall (fun leaf ->
                      let chain = Hierarchy.chain parentById (idOf leaf)

                      let linked =
                          chain |> List.pairwise |> List.forall (fun (a, b) -> parentById a = Some b)

                      linked
                      && List.last chain = stateId "root"
                      && List.head chain = idOf leaf
                      && List.length chain = List.length (List.distinct chain)))

          coverageTest ]
