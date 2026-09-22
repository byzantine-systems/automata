module ByzantineSystems.Automata.Core.Tests.ChartFingerprintTests

open ByzantineSystems.Automata.Core
open Expecto

/// A chart with one of every rule kind, a compound node with two children, and a terminal leaf,
/// so that each dimension the fingerprint claims to see can be varied one at a time.
///
/// Built through Chart.create with explicit Node records rather than the statechart expression:
/// these tests are about which structural differences the hash notices, and the records let each
/// variant differ in exactly one field.
let private baseNodes: Node<int, int, int, string> list =
    let always (_: int) (_: int) = true
    let step (_: int) (_: int) = [], 0
    let emit (_: int) (_: int) = []

    let node id parent =
        { Id = stateId id
          Parent = parent |> Option.map stateId
          InitialChild = None
          Terminal = false
          OnEntry = emit
          OnExit = emit
          Rules = [] }

    [ node "root" None
      { node "a" (Some "root") with
          Rules = [ Rule.transition always step; Rule.attempt always (fun _ _ -> Ok([], 0)) ] }
      { node "b" (Some "root") with
          Rules =
              [ Rule.goto always (stateId "a") step
                Rule.guarded always "needs approval" (Rule.internalOn always emit) ] }
      { node "c" (Some "root") with
          InitialChild = Some(stateId "c.one") }
      node "c.one" (Some "c")
      node "c.two" (Some "c")
      { node "z" (Some "root") with
          Terminal = true } ]

let private fingerprintOf (nodes: Node<int, int, int, string> list) : string =
    match Chart.create (stateId "root") (fun _ -> stateId "a") nodes with
    | Ok chart -> Chart.fingerprint chart |> ChartFingerprint.value
    | Error errors -> failtestf "the test chart is invalid: %A" errors

let private baseline = fingerprintOf baseNodes

/// Replaces one node, for a variant that differs in a single field.
let private replace (id: string) (change: Node<int, int, int, string> -> Node<int, int, int, string>) =
    baseNodes
    |> List.map (fun node -> if node.Id = stateId id then change node else node)

let private differs (description: string) (nodes: Node<int, int, int, string> list) =
    test description { Expect.notEqual (fingerprintOf nodes) baseline "the fingerprint should have changed" }

let private agrees (description: string) (nodes: Node<int, int, int, string> list) =
    test description { Expect.equal (fingerprintOf nodes) baseline "the fingerprint should not have changed" }

let tests =
    testList
        "chart fingerprint"
        [
          // Pins the canonicalisation, so that changing what is hashed is a deliberate act and
          // not a side effect. It also pins the hash to something stable across processes: a
          // fingerprint derived from GetHashCode, which .NET randomises per process, would move
          // between runs of this very test.
          test "a fixed chart has a fixed fingerprint" {
              Expect.equal
                  baseline
                  "810215e3609bcc93a0f29f68b2cb00ea3bfa6f38f830b1c71cad86f9f198edba"
                  "the canonical form changed; bump fingerprintScheme in Chart.fs if that was intended"
          }

          test "the same chart built twice agrees with itself" {
              Expect.equal (fingerprintOf baseNodes) baseline "two constructions must agree"
          }

          // Sibling declaration order decides nothing at resolution time, so every way of
          // writing this chart down must hash the same. Seven nodes means 5040 orderings, which
          // is cheap enough to check exhaustively rather than sample.
          test "no declaration order changes the fingerprint" {
              let rec permutations list =
                  match list with
                  | [] -> [ [] ]
                  | _ ->
                      list
                      |> List.collect (fun head ->
                          permutations (List.filter ((<>) head) list)
                          |> List.map (fun rest -> head :: rest))

              let orderings = permutations [ 0 .. baseNodes.Length - 1 ]

              let divergent =
                  orderings
                  |> List.filter (fun order -> fingerprintOf (order |> List.map (fun i -> baseNodes[i])) <> baseline)

              Expect.isEmpty divergent $"%d{orderings.Length} orderings checked"
          }

          // Everything the fingerprint is supposed to notice.
          differs
              "a new node"
              (baseNodes
               @ [ { Id = stateId "d"
                     Parent = Some(stateId "root")
                     InitialChild = None
                     Terminal = false
                     OnEntry = (fun _ _ -> [])
                     OnExit = (fun _ _ -> [])
                     Rules = [] } ])

          differs "a node becoming terminal" (replace "c.two" (fun node -> { node with Terminal = true }))

          differs
              "a different initial child"
              (replace "c" (fun node ->
                  { node with
                      InitialChild = Some(stateId "c.two") }))

          differs
              "a rule added to a node"
              (replace "a" (fun node ->
                  { node with
                      Rules = node.Rules @ [ Rule.internalOn (fun _ _ -> true) (fun _ _ -> []) ] }))

          // Rule order is semantic: first match wins. This is the one ordering the fingerprint
          // must not normalise away, and the mirror of the node-order property above.
          differs
              "two rules swapped within a node"
              (replace "a" (fun node ->
                  { node with
                      Rules = List.rev node.Rules }))

          differs
              "a goto pointing somewhere else"
              (replace "b" (fun node ->
                  { node with
                      Rules =
                          [ Rule.goto (fun _ _ -> true) (stateId "c") (fun _ _ -> [], 0)
                            Rule.guarded
                                (fun _ _ -> true)
                                "needs approval"
                                (Rule.internalOn (fun _ _ -> true) (fun _ _ -> [])) ] }))

          differs
              "a guard reason reworded"
              (replace "b" (fun node ->
                  { node with
                      Rules =
                          [ Rule.goto (fun _ _ -> true) (stateId "a") (fun _ _ -> [], 0)
                            Rule.guarded
                                (fun _ _ -> true)
                                "needs sign-off"
                                (Rule.internalOn (fun _ _ -> true) (fun _ _ -> [])) ] }))

          test "a renamed root changes the fingerprint" {
              let renamed =
                  baseNodes
                  |> List.map (fun node ->
                      let parent =
                          if node.Parent = Some(stateId "root") then
                              Some(stateId "start")
                          else
                              node.Parent

                      if node.Id = stateId "root" then
                          { node with
                              Id = stateId "start"
                              Parent = parent }
                      else
                          { node with Parent = parent })

              let fingerprint =
                  match Chart.create (stateId "start") (fun _ -> stateId "a") renamed with
                  | Ok chart -> Chart.fingerprint chart |> ChartFingerprint.value
                  | Error errors -> failtestf "the renamed chart is invalid: %A" errors

              Expect.notEqual fingerprint baseline "the root is part of the structure"
          }

          // The stated limitation, as executable text. Everything below changes what the chart
          // does and none of it is visible to a structural hash, which is why the fingerprint is
          // a tripwire against a forgotten version bump and not a proof of equivalence.
          agrees
              "a different predicate is invisible"
              (replace "a" (fun node ->
                  { node with
                      Rules =
                          [ Rule.transition (fun _ e -> e > 41) (fun _ _ -> [], 0)
                            Rule.attempt (fun _ _ -> true) (fun _ _ -> Ok([], 0)) ] }))

          agrees
              "a different transform is invisible"
              (replace "a" (fun node ->
                  { node with
                      Rules =
                          [ Rule.transition (fun _ _ -> true) (fun _ _ -> [ 7 ], 99)
                            Rule.attempt (fun _ _ -> true) (fun _ _ -> Ok([], 0)) ] }))

          agrees
              "different entry and exit actions are invisible"
              (replace "c" (fun node ->
                  { node with
                      OnEntry = (fun _ _ -> [ 1; 2; 3 ])
                      OnExit = (fun _ _ -> [ 4 ]) }))

          test "a different classifier is invisible" {
              let other =
                  match Chart.create (stateId "root") (fun _ -> stateId "z") baseNodes with
                  | Ok chart -> Chart.fingerprint chart |> ChartFingerprint.value
                  | Error errors -> failtestf "the test chart is invalid: %A" errors

              Expect.equal other baseline "the classifier is a closure and cannot be hashed"
          }

          test "rule kinds report which combinator built them" {
              let always (_: int) (_: int) = true

              Expect.equal (Rule.kind (Rule.transition always (fun _ _ -> [], 0))) RuleKind.Transition "transition"

              Expect.equal (Rule.kind (Rule.attempt always (fun _ _ -> Ok([], 0)))) RuleKind.Attempt "attempt"

              Expect.equal
                  (Rule.kind (Rule.goto always (stateId "a") (fun _ _ -> [], 0)))
                  (RuleKind.Goto(stateId "a"))
                  "goto carries its target"

              Expect.equal (Rule.kind (Rule.internalOn always (fun _ _ -> []))) RuleKind.Internal "internal"
          }

          test "a guard wraps the kind it guards rather than replacing it" {
              let always (_: int) (_: int) = true

              let guarded =
                  Rule.guarded always "twice" (Rule.guarded always "once" (Rule.internalOn always (fun _ _ -> [])))

              Expect.equal
                  (Rule.kind guarded)
                  (RuleKind.Guarded("twice", RuleKind.Guarded("once", RuleKind.Internal)))
                  "nested guards stay nested"
          } ]
