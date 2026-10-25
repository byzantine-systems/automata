module ByzantineSystems.Automata.Runtime.Tests.ReplayTests

open System
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Runtime
open Expecto

/// A tally, which is enough to see replay reorder arithmetic.
type Tally =
    | Counting of int
    | Closed

type Change =
    | Add of int
    | Halve
    | Close

type Entity = class end

/// Version 1 adds what it is given. Version 2 adds twice as much, so a replay under the wrong
/// version shows up as the wrong number. Halving is only defined for an even tally, which is
/// how a replayed event stops resolving.
let private tally (factor: int) =
    statechart<Tally, Change, string, string> {
        root "tally"

        classify (function
            | Counting _ -> stateId "counting"
            | Closed -> stateId "closed")

        state "counting" {
            on
                (fun _ change ->
                    match change with
                    | Add _ -> true
                    | _ -> false)
                (fun state change ->
                    match state, change with
                    | Counting n, Add k -> [ $"added {k}" ], Counting(n + factor * k)
                    | other, _ -> [], other)

            on
                (fun state change ->
                    match state, change with
                    | Counting n, Halve -> n % 2 = 0
                    | _ -> false)
                (fun state _ ->
                    match state with
                    | Counting n -> [], Counting(n / 2)
                    | other -> [], other)

            on (fun _ change -> change = Close) (fun _ _ -> [ "closed" ], Closed)
        }

        state "closed" { terminal }
    }
    |> function
        | Ok chart -> chart
        | Error errors -> failwithf "the tally chart is invalid: %A" errors

let private v1 = ChartVersion.create 1
let private v2 = ChartVersion.create 2
let private machine = machineId "tally"
let private id: EntityId<Entity> = entityId "t-1"

let private at (hour: int) =
    DateTimeOffset(2026, 1, 1, hour, 0, 0, TimeSpan.Zero)

/// A committed event as the log holds it. Replay reads only the event, when it was effective, the
/// version it was decided under and who it was; the rest is filler.
let private committed (epoch: int) (change: Change) (hour: int) (version: ChartVersion) =
    { Draft =
        { MachineId = machine
          EntityId = id
          Event = change
          Actions = []
          FromState = Counting 0
          ToState = Counting 0
          HandledBy = stateId "counting"
          Exited = []
          Entered = []
          Status = InstanceStatus.Running
          EffectiveAt = at hour }
      Epoch = Epoch.ofUInt64 (uint64 epoch)
      CommandId = CommandId.ofInt64 (int64 epoch)
      ChartVersion = version
      CommittedAt = at hour }

let private correction = CommandId.ofInt64 99L

let private input
    (start: Tally)
    (missed: Change)
    (hour: int)
    (next: int)
    policy
    : ReplayInput<EntityId<Entity>, Tally, Change> =
    { MachineId = machine
      EntityId = id
      Start = start
      Live = start
      Missed = missed
      At = at hour
      Correction = correction
      Current = v1
      NextEpoch = Epoch.ofUInt64 (uint64 next)
      Policy = policy }

let private onlyV1 = ChartCatalog.ofList [ 1, tally 1 ]
let private bothVersions = ChartCatalog.ofList [ 1, tally 1; 2, tally 2 ]

let private expectPlan result =
    match result with
    | Ok plan -> plan
    | Error error -> failtestf "the replay should have planned, but failed with %A" error

let private states (plan: ReplayPlan<_, Tally, _, _>) =
    plan.Commit.Beliefs |> List.map (fun belief -> belief.Asserted.State)

// A correction scenario is generated as raw choices, which FsCheck can generate and shrink,
// and compiled into a suffix every event of which resolves when replayed. The tally model
// below decides what replay must produce; it is written from the chart's declared arithmetic,
// not from replay's own code.

/// One event's worth of choices: which change, how much, when it was effective, which version
/// decided it, and the seed that ranks its epoch.
type private StepChoice =
    { Pick: int
      Amount: int
      Hour: int
      UseV2: bool
      Seed: int }

/// A whole correction's worth of choices.
type private TraceChoices =
    { Start: int
      Live: int
      CurrentV2: bool
      MissedPick: int
      MissedAmount: int
      NextDelta: int
      Steps: StepChoice list }

let private tallyOf (raw: int) = Counting(((raw % 31) + 31) % 31)
let private hourOf (raw: int) = 2 + ((raw % 7) + 7) % 7
let private amountOf (raw: int) = ((raw % 6) + 6) % 6 - 2

/// The tally arithmetic as the chart declares it: add the amount times the version's factor,
/// halve an even tally, close ends.
let private applyFactor (factor: int) (state: Tally) (change: Change) =
    match state, change with
    | Counting n, Add k -> Counting(n + factor * k)
    | Counting n, Halve -> Counting(n / 2)
    | Counting _, Close -> Closed
    | Closed, _ -> state

/// The change a pick stands for, given the state it has to resolve against: a halve that would
/// not resolve is an add instead, so every compiled event re-decides cleanly.
let private changeFrom (pick: int) (amount: int) (state: Tally) =
    match ((pick % 5) + 5) % 5 with
    | 0
    | 1
    | 2 -> Add(amountOf amount)
    | 3 ->
        match state with
        | Counting n when n % 2 = 0 -> Halve
        | _ -> Add(amountOf amount)
    | _ -> Close

let private factorOf (version: ChartVersion) = if version = v2 then 2 else 1

let private actionsFor (change: Change) =
    match change with
    | Add k -> [ $"added {k}" ]
    | Halve -> []
    | Close -> [ "closed" ]

let private snapshotOf (state: Tally) (epoch: Epoch) : Snapshot<Tally> =
    { State = state
      Status =
        if state = Closed then
            InstanceStatus.Terminated
        else
            InstanceStatus.Running
      Epoch = epoch }

/// What the chart's structure says a decided change looks like: every rule lives on "counting",
/// which is left and re-entered for a tally change and exchanged for "closed" on a close.
let private decided fromState change next effectiveAt : TransitionDraft<EntityId<Entity>, Tally, Change, string> =
    { MachineId = machine
      EntityId = id
      Event = change
      Actions = actionsFor change
      FromState = fromState
      ToState = next
      HandledBy = stateId "counting"
      Exited = [ stateId "counting" ]
      Entered =
        [ if next = Closed then
              stateId "closed"
          else
              stateId "counting" ]
      Status =
        if next = Closed then
            InstanceStatus.Terminated
        else
            InstanceStatus.Running
      EffectiveAt = effectiveAt }

/// One candidate event, before legality decides whether it is kept.
type private Candidate =
    { Hour: int
      EpochNum: int
      Version: ChartVersion
      Pick: int
      Amount: int }

/// A correction scenario as replay sees it: the belief before the corrected instant, the live
/// state the correction transitions from, the missed change, the current version, the epoch the
/// correction commits at, and a suffix in the order replay re-decides it.
type private Scenario =
    { Start: Tally
      Live: Tally
      Missed: Change
      Current: ChartVersion
      NextEpoch: Epoch
      Suffix: CommittedTransition<EntityId<Entity>, Tally, Change, string> list }

let private compileTrace (choices: TraceChoices) : Scenario =
    let current = if choices.CurrentV2 then v2 else v1
    let steps = choices.Steps |> List.truncate 12

    // Epochs rank the steps' seeds, so the order events were decided in is independent of
    // when they were effective.
    let ranks =
        steps
        |> List.mapi (fun index (step: StepChoice) -> step.Seed, index)
        |> List.sortBy fst
        |> List.mapi (fun rank (_, index) -> index, rank + 1)
        |> Map.ofList

    let candidates =
        steps
        |> List.mapi (fun index (step: StepChoice) ->
            { Hour = hourOf step.Hour
              EpochNum = ranks.[index]
              Version = if step.UseV2 then v2 else v1
              Pick = step.Pick
              Amount = step.Amount })
        |> List.sortBy (fun candidate -> candidate.Hour, candidate.EpochNum)

    let start = tallyOf choices.Start
    let missed = changeFrom choices.MissedPick choices.MissedAmount start

    // Walk the candidates in the order replay re-decides them, from the state the missed event
    // leaves: a close ends the walk, exactly as an instance that ended accepts nothing further.
    let rec walk (state: Tally) remaining acc =
        match state, remaining with
        | Closed, _
        | _, [] -> List.rev acc
        | Counting _, candidate :: rest ->
            let change = changeFrom candidate.Pick candidate.Amount state
            let logged = committed candidate.EpochNum change candidate.Hour candidate.Version

            walk (applyFactor (factorOf candidate.Version) state change) rest (logged :: acc)

    let suffix = walk (applyFactor (factorOf current) start missed) candidates []

    { Start = start
      Live = tallyOf choices.Live
      Missed = missed
      Current = current
      NextEpoch = Epoch.ofUInt64 (uint64 (suffix.Length + 1 + max 0 (choices.NextDelta % 3)))
      Suffix = suffix }

let private randomChoices (seed: int) : TraceChoices =
    let rng = Random seed

    let step () : StepChoice =
        { Pick = rng.Next()
          Amount = rng.Next()
          Hour = rng.Next()
          UseV2 = rng.Next 2 = 1
          Seed = rng.Next() }

    { Start = rng.Next()
      Live = rng.Next()
      CurrentV2 = rng.Next 2 = 1
      MissedPick = rng.Next()
      MissedAmount = rng.Next()
      NextDelta = rng.Next()
      Steps = List.init (rng.Next 13) (fun _ -> step ()) }

let private classesOf (scenario: Scenario) =
    let events = scenario.Suffix |> List.map (fun logged -> logged.Draft.Event)
    let hours = scenario.Suffix |> List.map (fun logged -> logged.Draft.EffectiveAt)

    [ "empty-suffix", scenario.Suffix.IsEmpty
      "missed-close", scenario.Missed = Close
      "equal-instant", hours |> List.contains (at 2)
      "duplicate-hours", (hours |> List.distinct).Length < hours.Length
      "current-v2", scenario.Current = v2 && not scenario.Suffix.IsEmpty
      "mixed-versions", (scenario.Suffix |> List.map (fun logged -> logged.ChartVersion) |> List.distinct).Length > 1
      "halve", events |> List.contains Halve
      "close", events |> List.contains Close ]
    |> List.filter snd
    |> List.map fst

let tests =
    testList
        "replay"
        [ test "a version the catalog does not hold is reported, not guessed" {
              match
                  Replay.plan
                      onlyV1
                      (input (Counting 0) (Add 1) 2 2 CorrectionPolicy.defaults)
                      [ committed 1 (Add 3) 3 v2 ]
              with
              | Error(ReplayError.UnknownChartVersion version) -> Expect.equal version v2 "the missing version"
              | other -> failtestf "expected an unknown version, got %A" other
          }

          test "an event that no longer resolves fails the correction by default" {
              // Halve was fine on 2. After a missed Add 1 the tally is 3, and 3 does not halve.
              match
                  Replay.plan
                      onlyV1
                      (input (Counting 2) (Add 1) 2 2 CorrectionPolicy.defaults)
                      [ committed 1 Halve 3 v1 ]
              with
              | Error(ReplayError.Diverged(epoch, _)) -> Expect.equal (Epoch.value epoch) 1UL "the epoch that diverged"
              | other -> failtestf "expected a divergence, got %A" other
          }

          test "truncating keeps the timeline up to the divergence" {
              let policy =
                  { CorrectionPolicy.defaults with
                      OnDivergence = Divergence.Truncate }

              let plan =
                  Replay.plan
                      onlyV1
                      (input (Counting 2) (Add 1) 2 3 policy)
                      [ committed 1 Halve 3 v1; committed 2 (Add 1) 4 v1 ]
                  |> expectPlan

              Expect.equal plan.Truncated (Some(Epoch.ofUInt64 1UL)) "where it stopped"
              Expect.equal (states plan) [ Counting 3 ] "and nothing after it"
          }

          Property.property "the replay budget accepts exactly its cardinality bound" (fun (seed: byte) ->
              let limit = int seed % 31

              let policy =
                  { CorrectionPolicy.defaults with
                      ReplayLimit = limit }

              let suffix length =
                  [ 1..length ] |> List.map (fun epoch -> committed epoch (Add 1) 3 v1)

              let atLimit =
                  Replay.plan onlyV1 (input (Counting 0) (Add 1) 2 (limit + 2) policy) (suffix limit)

              let overLimit =
                  suffix (limit + 1)
                  |> List.rev
                  |> Replay.plan onlyV1 (input (Counting 0) (Add 1) 2 (limit + 2) policy)

              Result.isOk atLimit && overLimit = Error(ReplayError.BudgetExceeded limit))

          test "an instance the correction ends accepts nothing after it" {
              match
                  Replay.plan
                      onlyV1
                      (input (Counting 0) Close 2 2 CorrectionPolicy.defaults)
                      [ committed 1 (Add 1) 3 v1 ]
              with
              | Error(ReplayError.Diverged(_, reason)) ->
                  Expect.stringContains reason "Terminated" "the reason says why"
              | other -> failtestf "expected the ended instance to refuse, got %A" other
          }

          test "replay reports actions and the correction emits none" {
              let plan =
                  Replay.plan
                      onlyV1
                      (input (Counting 0) (Add 1) 2 2 CorrectionPolicy.defaults)
                      [ committed 1 Close 3 v1 ]
                  |> expectPlan

              Expect.equal
                  (plan.Replayed |> List.collect (snd >> _.Actions))
                  [ "closed" ]
                  "what replay would have emitted"

              Expect.isEmpty plan.Commit.Transition.Actions "the correction's own transition carries none"
          }

          Property.property "replay follows the documented fold" (fun (choices: TraceChoices) ->
              let scenario = compileTrace choices

              let replayInput =
                  { input
                        scenario.Start
                        scenario.Missed
                        2
                        (int (Epoch.value scenario.NextEpoch))
                        CorrectionPolicy.defaults with
                      Live = scenario.Live
                      Current = scenario.Current }

              match Replay.plan bothVersions replayInput scenario.Suffix with
              | Error _ -> false
              | Ok plan ->
                  // The reference fold, decided by the tally contract rather than by replay: the
                  // missed event under the current chart, then each logged event under the
                  // version that first decided it, from the state the previous one leaves.
                  let insertedNext =
                      applyFactor (factorOf scenario.Current) scenario.Start scenario.Missed

                  let inserted = decided scenario.Start scenario.Missed insertedNext (at 2)

                  let reference =
                      scenario.Suffix
                      |> List.fold
                          (fun (state, drafts) (logged: CommittedTransition<EntityId<Entity>, Tally, Change, string>) ->
                              let next = applyFactor (factorOf logged.ChartVersion) state logged.Draft.Event

                              let draft = decided state logged.Draft.Event next logged.Draft.EffectiveAt
                              next, (logged, draft) :: drafts)
                          (insertedNext, [])
                      |> snd
                      |> List.rev

                  let beliefs =
                      let replayed =
                          reference
                          |> List.map (fun (logged, draft) ->
                              { Asserted = snapshotOf draft.ToState logged.Epoch
                                ValidFrom = draft.EffectiveAt
                                CommandId = logged.CommandId
                                ChartVersion = logged.ChartVersion })

                      let insertedBelief =
                          { Asserted = snapshotOf insertedNext scenario.NextEpoch
                            ValidFrom = at 2
                            CommandId = correction
                            ChartVersion = scenario.Current }

                      // Two beliefs at one instant are one: the later one, since it was decided
                      // after the other.
                      insertedBelief :: replayed
                      |> List.groupBy (fun (belief: CorrectedBelief<Tally>) -> belief.ValidFrom)
                      |> List.map (snd >> List.last)

                  // The final belief is the entity's live one, so the correction owns it.
                  let attributed =
                      match List.rev beliefs with
                      | [] -> []
                      | last :: earlier ->
                          { last with
                              Asserted =
                                  { last.Asserted with
                                      Epoch = scenario.NextEpoch }
                              CommandId = correction
                              ChartVersion = scenario.Current }
                          :: earlier
                          |> List.rev

                  let finalState, finalStatus =
                      match reference |> List.tryLast |> Option.map snd with
                      | Some draft -> draft.ToState, draft.Status
                      | None -> inserted.ToState, inserted.Status

                  let transition =
                      { inserted with
                          FromState = scenario.Live
                          ToState = finalState
                          Status = finalStatus
                          Actions = [] }

                  plan.Truncated = None
                  && plan.Inserted = inserted
                  && plan.Commit.ValidFrom = at 2
                  && plan.Commit.Beliefs = attributed
                  && plan.Commit.Transition = transition
                  && (plan.Replayed, reference)
                     ||> List.forall2 (fun (_, actual) (_, expected) -> actual = expected))

          test "the generated traces reach every replay behavior class" {
              let classes =
                  seq { 0..499 }
                  |> Seq.collect (fun seed -> classesOf (compileTrace (randomChoices seed)))
                  |> Set.ofSeq

              Expect.equal
                  classes
                  (set
                      [ "empty-suffix"
                        "missed-close"
                        "equal-instant"
                        "duplicate-hours"
                        "current-v2"
                        "mixed-versions"
                        "halve"
                        "close" ])
                  "every class is reachable from five hundred random inputs"
          } ]
