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

let private expectPlan result =
    match result with
    | Ok plan -> plan
    | Error error -> failtestf "the replay should have planned, but failed with %A" error

let private states (plan: ReplayPlan<_, Tally, _, _>) =
    plan.Commit.Beliefs |> List.map (fun belief -> belief.Asserted.State)

let tests =
    testList
        "replay"
        [ test "a missed event is applied at its instant and every later event is re-decided" {
              // Add 2 at 3h and Close at 4h were decided from 0. A missed Add 1 at 2h means Add 2
              // now lands on 1.
              let plan =
                  Replay.plan
                      onlyV1
                      (input (Counting 0) (Add 1) 2 3 CorrectionPolicy.defaults)
                      [ committed 1 (Add 2) 3 v1; committed 2 Close 4 v1 ]
                  |> expectPlan

              Expect.equal (states plan) [ Counting 1; Counting 3; Closed ] "the corrected timeline"

              Expect.equal
                  (plan.Commit.Beliefs |> List.map _.ValidFrom)
                  [ at 2; at 3; at 4 ]
                  "each belief starts when its event was effective"
          }

          test "each replayed event is decided under the version that first decided it" {
              // The missed event is decided under the current version (2 doubles); the replayed
              // Add 3 was decided under version 1, and still adds 3, not 6.
              let catalog = ChartCatalog.ofList [ 1, tally 1; 2, tally 2 ]

              let plan =
                  Replay.plan
                      catalog
                      { input (Counting 0) (Add 1) 2 2 CorrectionPolicy.defaults with
                          Current = v2 }
                      [ committed 1 (Add 3) 3 v1 ]
                  |> expectPlan

              Expect.equal
                  (states plan)
                  [ Counting 2; Counting 5 ]
                  "version 2 for the correction, version 1 for the replay"
          }

          test "a version the catalog does not hold is reported, not guessed" {
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

          test "a suffix longer than the budget is refused" {
              let policy =
                  { CorrectionPolicy.defaults with
                      ReplayLimit = 1 }

              match
                  Replay.plan
                      onlyV1
                      (input (Counting 0) (Add 1) 2 3 policy)
                      [ committed 1 (Add 1) 3 v1; committed 2 (Add 1) 4 v1 ]
              with
              | Error(ReplayError.BudgetExceeded limit) -> Expect.equal limit 1 "the limit"
              | other -> failtestf "expected the budget to be exceeded, got %A" other
          }

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

          test "the last belief is attributed to the correction, earlier ones keep theirs" {
              // The next ordinary command expects the live belief's epoch to be the latest. Any
              // other attribution would leave it expecting an epoch already taken.
              let plan =
                  Replay.plan
                      onlyV1
                      (input (Counting 0) (Add 1) 2 3 CorrectionPolicy.defaults)
                      [ committed 1 (Add 2) 3 v1; committed 2 (Add 4) 4 v1 ]
                  |> expectPlan

              Expect.equal
                  (plan.Commit.Beliefs
                   |> List.map (fun b -> Epoch.value b.Asserted.Epoch, b.CommandId))
                  [ 3UL, correction; 1UL, CommandId.ofInt64 1L; 3UL, correction ]
                  "the first and last are the correction's"
          }

          test "two beliefs at one instant become one" {
              // The missed event and a recorded one share an instant. The recorded one was decided
              // after, so its belief is the one that stands.
              let plan =
                  Replay.plan
                      onlyV1
                      (input (Counting 0) (Add 1) 3 2 CorrectionPolicy.defaults)
                      [ committed 1 (Add 2) 3 v1 ]
                  |> expectPlan

              Expect.equal (states plan) [ Counting 3 ] "one belief at 3h, after both events"
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
          } ]
