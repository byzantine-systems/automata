module ByzantineSystems.Automata.Storage.LifecycleModel

open ByzantineSystems.Automata.Storage
open Expecto
open FsCheck

// ---------------------------------------------------------------------------------------------
// The lifecycle both providers' databases enforce. One shared, pure oracle: the status edges
// a command may legally take, what a live same-status update may still do, and the failure
// evidence each terminal closure must — or must not — carry. The provider schema tests drive
// their exhaustive matrices from this module, so the contract exists once rather than twice
// with room to drift. The SQL spellings are written here independently of the store's codec
// on purpose: a schema that disagrees with this table fails the bijection law below.
// ---------------------------------------------------------------------------------------------

/// <summary>What the database says about one attempted status assignment.</summary>
[<RequireQualifiedAccess>]
type LifecycleVerdict =

    /// <summary>A legal changed edge: claiming, rescheduling, or closing a live command.</summary>
    | LegalChange

    /// <summary>
    /// A same-status update on a live command: lease extension, expired-lease reclaim, or an
    /// operational update that leaves the status alone while naming it.
    /// </summary>
    | LegalUnchanged

    /// <summary>
    /// The write is refused: an edge the lifecycle does not contain, or any update — even a
    /// no-op — naming a terminal command.
    /// </summary>
    | Refused

/// <summary>What a command carries, or refuses to carry, at each status.</summary>
[<RequireQualifiedAccess>]
type ClosureEvidence =

    /// <summary>
    /// Closing as succeeded requires that no failure evidence exists for the command.
    /// </summary>
    | NoFailure

    /// <summary>
    /// Closing as rejected or dead-lettered requires a failure header and its selected child.
    /// </summary>
    | CompleteFailure

    /// <summary>An open command carries no closure evidence obligation.</summary>
    | Open

/// <summary>The lifecycle contract, as one pure table.</summary>
[<RequireQualifiedAccess>]
module Lifecycle =

    /// <summary>Every status, in the order the vocabulary is conventionally written.</summary>
    let all: CommandStatus list =
        [ CommandStatus.Ready
          CommandStatus.Leased
          CommandStatus.Succeeded
          CommandStatus.Rejected
          CommandStatus.DeadLettered ]

    /// <summary>
    /// The terminal statuses. A terminal command is final: nothing about it may be rewritten,
    /// and the only writer the row answers is retention's DELETE.
    /// </summary>
    let isTerminal (status: CommandStatus) : bool =
        match status with
        | CommandStatus.Succeeded
        | CommandStatus.Rejected
        | CommandStatus.DeadLettered -> true
        | CommandStatus.Ready
        | CommandStatus.Leased -> false

    /// <summary>
    /// The complete set of legal changed edges. Every source is live; every terminal target is
    /// reached only from a live source; no edge leaves a terminal status.
    /// </summary>
    let legalChanges: (CommandStatus * CommandStatus) list =
        [ CommandStatus.Ready, CommandStatus.Leased
          CommandStatus.Leased, CommandStatus.Ready
          CommandStatus.Ready, CommandStatus.Succeeded
          CommandStatus.Ready, CommandStatus.Rejected
          CommandStatus.Ready, CommandStatus.DeadLettered
          CommandStatus.Leased, CommandStatus.Succeeded
          CommandStatus.Leased, CommandStatus.Rejected
          CommandStatus.Leased, CommandStatus.DeadLettered ]

    /// <summary>Classifies one attempted status assignment.</summary>
    let classify (oldStatus: CommandStatus) (newStatus: CommandStatus) : LifecycleVerdict =
        if oldStatus = newStatus then
            if isTerminal oldStatus then
                LifecycleVerdict.Refused
            else
                LifecycleVerdict.LegalUnchanged
        elif List.contains (oldStatus, newStatus) legalChanges then
            LifecycleVerdict.LegalChange
        else
            LifecycleVerdict.Refused

    /// <summary>The closure evidence each status demands of the write that reaches it.</summary>
    let closureEvidence (status: CommandStatus) : ClosureEvidence =
        match status with
        | CommandStatus.Ready
        | CommandStatus.Leased -> ClosureEvidence.Open
        | CommandStatus.Succeeded -> ClosureEvidence.NoFailure
        | CommandStatus.Rejected
        | CommandStatus.DeadLettered -> ClosureEvidence.CompleteFailure

    /// <summary>The text both schemas store for a status, written here as an independent table.</summary>
    let sql (status: CommandStatus) : string =
        match status with
        | CommandStatus.Ready -> "ready"
        | CommandStatus.Leased -> "leased"
        | CommandStatus.Succeeded -> "succeeded"
        | CommandStatus.Rejected -> "rejected"
        | CommandStatus.DeadLettered -> "dead_letter"

// ---------------------------------------------------------------------------------------------
// Laws. The spec is restated below as literals once, then every claim is checked against it.
// The matrix is exhaustive over a finite domain, so each cell is pinned directly; the walk
// adds the sequential face: terminal statuses absorb generated attempts to leave them.
// ---------------------------------------------------------------------------------------------

/// <summary>The spec's legal changed edges, restated as texts independent of the module above.</summary>
let private expectedEdges: (string * string) list =
    [ "ready", "leased"
      "leased", "ready"
      "ready", "succeeded"
      "ready", "rejected"
      "ready", "dead_letter"
      "leased", "succeeded"
      "leased", "rejected"
      "leased", "dead_letter" ]

/// <summary>The spec's terminal statuses, as texts.</summary>
let private terminalTexts: string list = [ "succeeded"; "rejected"; "dead_letter" ]

/// <summary>The texts each evidence class owns, for the partition law.</summary>
let private evidenceTexts (evidence: ClosureEvidence) : string list =
    match evidence with
    | ClosureEvidence.Open -> [ "ready"; "leased" ]
    | ClosureEvidence.NoFailure -> [ "succeeded" ]
    | ClosureEvidence.CompleteFailure -> [ "rejected"; "dead_letter" ]

let private verdictName =
    function
    | LifecycleVerdict.LegalChange -> "legal change"
    | LifecycleVerdict.LegalUnchanged -> "legal unchanged"
    | LifecycleVerdict.Refused -> "refused"

[<Tests>]
let tests =
    testList
        "lifecycle model"
        [ testList
              "status matrix"
              (Lifecycle.all
               |> List.collect (fun oldStatus ->
                   Lifecycle.all
                   |> List.map (fun newStatus ->
                       let name = $"{Lifecycle.sql oldStatus} -> {Lifecycle.sql newStatus}"

                       let expected =
                           if oldStatus = newStatus then
                               if List.contains (Lifecycle.sql oldStatus) terminalTexts then
                                   LifecycleVerdict.Refused
                               else
                                   LifecycleVerdict.LegalUnchanged
                           elif List.contains (Lifecycle.sql oldStatus, Lifecycle.sql newStatus) expectedEdges then
                               LifecycleVerdict.LegalChange
                           else
                               LifecycleVerdict.Refused

                       testCase name (fun () ->
                           Expect.equal
                               (Lifecycle.classify oldStatus newStatus)
                               expected
                               $"the lifecycle classifies %s{name} as %s{verdictName expected}"))))

          testCase "legal changed edges are exactly the eight in the spec" (fun () ->
              let legal =
                  Lifecycle.all
                  |> List.collect (fun oldStatus ->
                      Lifecycle.all
                      |> List.choose (fun newStatus ->
                          if Lifecycle.classify oldStatus newStatus = LifecycleVerdict.LegalChange then
                              Some(Lifecycle.sql oldStatus, Lifecycle.sql newStatus)
                          else
                              None))

              Expect.equal (List.sort legal) (List.sort expectedEdges) "the classifier admits exactly the spec's edges")

          testCase "terminal statuses are absorbing" (fun () ->
              let leaks =
                  [ for oldStatus in Lifecycle.all do
                        if Lifecycle.isTerminal oldStatus then
                            for newStatus in Lifecycle.all do
                                if Lifecycle.classify oldStatus newStatus <> LifecycleVerdict.Refused then
                                    $"{Lifecycle.sql oldStatus} -> {Lifecycle.sql newStatus}" ]

              Expect.isEmpty leaks "no update of a terminal command is ever admitted")

          testCase "every legal change leaves a live source" (fun () ->
              let badSources =
                  Lifecycle.legalChanges
                  |> List.filter (fun (oldStatus, _) -> Lifecycle.isTerminal oldStatus)
                  |> List.map fst

              Expect.isEmpty badSources "legal changes never leave a terminal status")

          testCase "closure evidence partitions the statuses" (fun () ->
              let partition text =
                  match text with
                  | "ready"
                  | "leased" -> ClosureEvidence.Open
                  | "succeeded" -> ClosureEvidence.NoFailure
                  | "rejected"
                  | "dead_letter" -> ClosureEvidence.CompleteFailure
                  | other -> failtest $"unknown status text %s{other}"

              for status in Lifecycle.all do
                  Expect.equal
                      (Lifecycle.closureEvidence status)
                      (partition (Lifecycle.sql status))
                      $"evidence for %s{Lifecycle.sql status} matches the spec"

                  for kind in
                      [ ClosureEvidence.Open
                        ClosureEvidence.NoFailure
                        ClosureEvidence.CompleteFailure ] do
                      Expect.equal
                          (Lifecycle.closureEvidence status = kind)
                          (List.contains (Lifecycle.sql status) (evidenceTexts kind))
                          $"classification and membership agree for %s{Lifecycle.sql status}")

          testCase "sql spellings are a bijection" (fun () ->
              let texts = Lifecycle.all |> List.map Lifecycle.sql

              Expect.equal (texts |> List.distinct |> List.length) (List.length texts) "each status has its own text"

              Expect.equal
                  (texts |> List.sort)
                  [ "dead_letter"; "leased"; "ready"; "rejected"; "succeeded" ]
                  "the vocabulary is exactly the five documented texts")

          testCase "verdicts over the whole matrix sum to the domain" (fun () ->
              let counts =
                  Lifecycle.all
                  |> List.collect (fun oldStatus -> Lifecycle.all |> List.map (Lifecycle.classify oldStatus))
                  |> List.countBy id
                  |> Map.ofList

              Expect.equal (counts.TryFind LifecycleVerdict.LegalChange) (Some 8) "eight legal changed edges"

              Expect.equal
                  (counts.TryFind LifecycleVerdict.LegalUnchanged)
                  (Some 2)
                  "two live statuses accept same-status updates"

              Expect.equal
                  (counts.TryFind LifecycleVerdict.Refused)
                  (Some 15)
                  "twelve illegal changed edges plus three terminal no-ops")

          // A generated walk: attempts to move are classified, and only legal ones move the
          // status. The law is absorption — once the walk reaches a terminal status, no
          // remaining attempt changes it.
          testCase "terminal statuses absorb generated walks" (fun () ->
              let pick (raw: byte) = List.item (int raw % 5) Lifecycle.all

              let rec walk (status: CommandStatus) (remaining: byte list) : bool =
                  match remaining with
                  | [] -> true
                  | next :: rest ->
                      match Lifecycle.classify status (pick next) with
                      | LifecycleVerdict.LegalChange when Lifecycle.isTerminal status -> false
                      | LifecycleVerdict.LegalChange -> walk (pick next) rest
                      | LifecycleVerdict.LegalUnchanged
                      | LifecycleVerdict.Refused -> walk status rest

              Check.One(
                  Config.QuickThrowOnFailure,
                  fun (start: byte, steps: byte list) -> walk (pick start) (List.truncate 32 steps)
              )) ]
