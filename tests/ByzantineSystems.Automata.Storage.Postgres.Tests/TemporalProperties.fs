module ByzantineSystems.Automata.Storage.Postgres.Tests.TemporalProperties

open System
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Postgres
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
        let! result = (newTemporalReader ()).ValidAtAsync(machine, entity "e1", at, noCancellation)

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
                                    (newTemporalReader ()).ValidAtAsync(machine, entity "e1", instant, noCancellation)

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

                    let! outcome =
                        (newCorrections ())
                            .CorrectAsync(machine, entity "e1", Epoch.initial, cut, beliefs, noCancellation)

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
        [ testCase "belief projection preserves every row and column" (fun () ->
              Check.One(
                  Config.QuickThrowOnFailure,
                  fun (values: byte list) ->
                      let rows: BeliefBatch.EncodedBelief list =
                          values
                          |> List.mapi (fun index value ->
                              { State = string value
                                Status = if index % 2 = 0 then "running" else "suspended"
                                ValidFrom = baseTime.AddMinutes(float index)
                                Epoch = int64 index + 1L
                                CommandId = int64 value + 1L
                                ChartVersion = 1 + index })

                      match BeliefBatch.parameters rows with
                      | [ _, Param.JsonbArray states
                          _, Param.TextArray statuses
                          _, Param.TimestampArray times
                          _, Param.BigintArray epochs
                          _, Param.BigintArray commands
                          _, Param.IntArray versions ] ->
                          List.length states = rows.Length
                          && (rows
                              |> List.indexed
                              |> List.forall (fun (i, row) ->
                                  let actual = states[i], statuses[i], times[i], epochs[i], commands[i], versions[i]

                                  let expected =
                                      row.State, row.Status, row.ValidFrom, row.Epoch, row.CommandId, row.ChartVersion

                                  actual = expected))
                      | _ -> false
              ))

          testTask "malformed belief arrays are rejected without changing either timeline" {
              do! reset ()
              believe "e1" baseTime (Active 1) 1L

              let before =
                  scalar<string> "SELECT jsonb_agg(to_jsonb(s))::text FROM fsm.instance_state s"

              let historyBefore = scalar<int64> "SELECT count(*) FROM fsm.instance_state_history"

              let valid =
                  [ "ARRAY['{}'::jsonb]"
                    "ARRAY['running']::text[]"
                    "ARRAY['2026-01-01T00:00:00Z'::timestamptz]"
                    "ARRAY[1::bigint]"
                    "ARRAY[1::bigint]"
                    "ARRAY[1]" ]

              let types = [ "jsonb"; "text"; "timestamptz"; "bigint"; "bigint"; "integer" ]

              for index, elementType in List.indexed types do
                  for bad in
                      [ $"ARRAY[]::{elementType}[]"
                        $"ARRAY[NULL]::{elementType}[]"
                        $"NULL::{elementType}[]" ] do
                      let args =
                          valid
                          |> List.mapi (fun i value -> if i = index then bad else value)
                          |> String.concat ","

                      let statement =
                          $"SELECT fsm_temporal.rewrite_beliefs('pg-tests','e1','2026-01-01T00:00:00Z',{args})"

                      Expect.isError
                          (exec (statement.Replace("fsm_temporal.", $"{schemaValue}_temporal.")))
                          "invalid array rejected"

                      Expect.equal
                          (scalar<string> "SELECT jsonb_agg(to_jsonb(s))::text FROM fsm.instance_state s")
                          before
                          "live timeline unchanged"

                      Expect.equal
                          (scalar<int64> "SELECT count(*) FROM fsm.instance_state_history")
                          historyBefore
                          "system history unchanged"
          }

          timelineProperty "a contiguous timeline answers ValidAt half-open and round-trips" 40

          correctionProperty "a correction rewrites exactly from its cut" 40 ]
