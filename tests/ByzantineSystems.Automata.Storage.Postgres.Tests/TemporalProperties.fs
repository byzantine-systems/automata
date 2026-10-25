module ByzantineSystems.Automata.Storage.Postgres.Tests.TemporalProperties

open System
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open Expecto
open FsCheck
open TestContext

let private entity name : Entity = entityId name

let private baseTime = DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)

// ---------------------------------------------------------------------------------------------
// Generated timelines and the half-open model a query is checked against.
//
// Segments are compiled from positive deltas rather than generated as raw instants, so the
// timeline is strictly ascending by construction, shrinking preserves that, and no sort or
// distinct repair hides a malformed generated value.
// ---------------------------------------------------------------------------------------------

type private SegmentSeed = { Delta: byte; State: byte }

type private TimelineCase =
    { Segments: SegmentSeed list
      Cut: byte
      Replacement: SegmentSeed list }

let private stateOf (raw: byte) : TestState = Active(1 + int raw % 5)

/// Compiles seeds into a strictly ascending (instant, state, epoch) timeline. The first segment
/// sits exactly at <paramref name="origin" />, and each seed's delta separates it from the next.
let private timelineFrom
    (origin: DateTimeOffset)
    (seeds: SegmentSeed list)
    : (DateTimeOffset * TestState * int64) list =
    match seeds with
    | [] -> []
    | _ ->
        let bounds =
            seeds
            |> List.scan (fun (at: DateTimeOffset) seed -> at.AddMinutes(float (1 + int seed.Delta % 30))) origin
            |> List.take (List.length seeds)

        List.mapi2 (fun index seed instant -> instant, stateOf seed.State, int64 (index + 1)) seeds bounds

/// The state a half-open timeline answers at an instant: the last segment starting at or before it.
let private stateAt (timeline: (DateTimeOffset * TestState * int64) list) (at: DateTimeOffset) : TestState option =
    timeline
    |> List.filter (fun (start, _, _) -> start <= at)
    |> List.tryLast
    |> Option.map (fun (_, state, _) -> state)

let private correctedBelief (at: DateTimeOffset) (state: TestState) (epoch: int64) : CorrectedBelief<TestState> =
    { Asserted =
        { State = state
          Status = Running
          Epoch = Epoch.ofUInt64 (uint64 epoch) }
      ValidFrom = at
      CommandId = CommandId.ofInt64 1L
      ChartVersion = version }

/// <summary>Maps a store result to an option without failing the property on a store error.</summary>
let private okOf (result: Result<'a, StoreError>) : 'a option =
    match result with
    | Ok value -> Some value
    | Error _ -> None

/// <summary>The state <c>ValidAt</c> answers with, or <c>None</c> when the store erred.</summary>
let private validState (at: DateTimeOffset) : Task<TestState option> =
    task {
        let! result = (newTemporalReader ()).ValidAt(machine, entity "e1", at, noCancellation)

        return result |> okOf |> Option.bind (Option.map _.Snapshot.State)
    }

let private timelineProperty name maxTests =
    testCase name (fun () ->
        let config = Config.QuickThrowOnFailure.WithMaxTest(maxTests).WithEndSize(12)

        Check.One(
            config,
            fun (case: TimelineCase) ->
                task {
                    do! reset ()
                    let timeline = timelineFrom baseTime (List.truncate 6 case.Segments)
                    let mutable ok = true

                    if not (List.isEmpty timeline) then
                        for instant, state, epoch in timeline do
                            believe "e1" instant state epoch

                        // Nothing before the first belief.
                        let! before = validState (baseTime.AddHours -1.0)
                        ok <- before = None

                        // Each segment answers at its own start, half-open: the lower bound is
                        // inside the segment, and the round-trip carries the full belief.
                        for index, (instant, state, epoch) in List.indexed timeline do
                            if ok then
                                let next =
                                    if index < timeline.Length - 1 then
                                        let nextInstant, _, _ = timeline[index + 1]
                                        Some nextInstant
                                    else
                                        None

                                let! found =
                                    (newTemporalReader ()).ValidAt(machine, entity "e1", instant, noCancellation)

                                ok <-
                                    match found |> okOf with
                                    | Some(Some belief) ->
                                        belief.Snapshot.State = state
                                        && belief.Snapshot.Status = Running
                                        && belief.Snapshot.Epoch = Epoch.ofUInt64 (uint64 epoch)
                                        && belief.ValidFrom = instant
                                        && belief.ValidTo = next
                                        && belief.KnownTo = None
                                    | _ -> false

                        // The last segment stays true.
                        let lastInstant, lastState, _ = List.last timeline
                        let! after = validState (lastInstant.AddHours 1.0)
                        ok <- ok && after = Some lastState

                        // One microsecond before a boundary still belongs to the previous segment.
                        for index in 1 .. timeline.Length - 1 do
                            if ok then
                                let _, prevState, _ = timeline[index - 1]
                                let currInstant, _, _ = timeline[index]
                                let! boundary = validState (currInstant.AddTicks -1L)
                                ok <- ok && boundary = Some prevState

                    return ok
                }
        ))

let private correctionProperty name maxTests =
    testCase name (fun () ->
        let config = Config.QuickThrowOnFailure.WithMaxTest(maxTests).WithEndSize(12)

        Check.One(
            config,
            fun (case: TimelineCase) ->
                task {
                    do! reset ()
                    let original = timelineFrom baseTime (List.truncate 6 case.Segments)

                    for instant, state, epoch in original do
                        believe "e1" instant state epoch

                    let cut = baseTime.AddMinutes(float (1 + int case.Cut % 120))
                    let replacement = timelineFrom cut (List.truncate 4 case.Replacement)

                    let beliefs =
                        replacement
                        |> List.map (fun (at, state, epoch) -> correctedBelief at state epoch)

                    let! outcome = (newCorrections ()).Correct(machine, entity "e1", cut, beliefs, noCancellation)
                    let mutable ok = (outcome |> okOf).IsSome

                    if ok then
                        // Before the cut the original opinion stands; from the cut onward the
                        // replacement (or nothing, when empty) answers.
                        let! before = validState (cut.AddMinutes -1.0)
                        ok <- before = stateAt original (cut.AddMinutes -1.0)

                        let! atCut = validState cut
                        ok <- ok && atCut = stateAt replacement cut

                        let! beyond = validState (cut.AddMinutes 1.0)
                        ok <- ok && beyond = stateAt replacement (cut.AddMinutes 1.0)

                    return ok
                }
        ))

let tests =
    testList
        "Postgres temporal properties"
        [ timelineProperty "a contiguous timeline answers ValidAt half-open and round-trips" 40

          correctionProperty "a correction rewrites exactly from its cut" 40 ]
