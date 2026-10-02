module ByzantineSystems.Automata.Storage.Postgres.Tests.CorrectionTests

open System
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open Expecto
open TestContext

let private entity name : Entity = entityId name

let private jan (hour: int) =
    DateTimeOffset(2026, 1, 1, hour, 0, 0, TimeSpan.Zero)

let private corrected (at: DateTimeOffset) (state: TestState) (epoch: int64) : CorrectedBelief<TestState> =
    { Asserted =
        { State = state
          Status = Running
          Epoch = Epoch.ofUInt64 (uint64 epoch) }
      ValidFrom = at
      CommandId = CommandId.ofInt64 1L
      ChartVersion = version }

/// Typed so a lambda reading a belief does not have to be annotated at every call site.
let private stateOf (result: Result<Belief<Entity, TestState> option, StoreError>) (label: string) =
    result |> expectOk label |> Option.map (fun belief -> belief.Snapshot.State)

let private liveBeliefs () =
    rows "SELECT state, lower(valid_during) AS vfrom, upper(valid_during) AS vto
         FROM fsm.instance_state WHERE entity_id = @entity ORDER BY vfrom" [ "entity", box "e1" ] (fun reader ->
        reader.GetString 0, reader.GetDateTime 1, reader.GetDateTime 2)

let private archivedCount () =
    scalar<int64> "SELECT count(*) FROM fsm.instance_state_history"

let tests =
    testList
        "Postgres corrections"
        [ testTask "correcting an entity with no belief supersedes nothing" {
              do! reset ()

              let! outcome =
                  (newCorrections ())
                      .Correct(machine, entity "e1", jan 10, [ corrected (jan 10) (Active 1) 1L ], noCancellation)

              Expect.equal NothingSuperseded (outcome |> expectOk "correct") "there was nothing there to supersede"
              Expect.equal 1 (liveBeliefs ()).Length "the timeline is now the supplied one"
          }

          testTask "a correction behind the live belief is written where close_and_open refuses" {
              // This is the case the whole capability exists for. fsm.close_and_open raises on a
              // back-dated instant on purpose, because a forward commit landing behind the live
              // belief is a bug. A correction says it means it by calling the other routine.
              do! reset ()
              believe "e1" (jan 10) (Active 1) 1L
              believe "e1" (jan 14) (Active 2) 2L

              let! outcome =
                  (newCorrections ())
                      .Correct(machine, entity "e1", jan 12, [ corrected (jan 12) (Active 7) 1L ], noCancellation)

              Expect.equal (Corrected 2) (outcome |> expectOk "correct") "the split belief and the later one"

              let timeline = liveBeliefs () |> List.map (fun (_, vfrom, _) -> vfrom)

              Expect.equal
                  [ (jan 10).UtcDateTime; (jan 12).UtcDateTime ]
                  timeline
                  "the part before the correction was kept, and the rest replaced"
          }

          testTask "the part of a belief before the corrected instant is kept" {
              do! reset ()
              believe "e1" (jan 10) (Active 1) 1L

              let! _ =
                  (newCorrections ())
                      .Correct(machine, entity "e1", jan 12, [ corrected (jan 12) (Active 7) 1L ], noCancellation)

              let! before = (newTemporalReader ()).ValidAt(machine, entity "e1", jan 11, noCancellation)
              let! after = (newTemporalReader ()).ValidAt(machine, entity "e1", jan 13, noCancellation)

              Expect.equal (Some(Active 1)) (stateOf before "before") "what was true at 11 was never in question"

              Expect.equal (Some(Active 7)) (stateOf after "after") "what was true at 13 is now the corrected answer"
          }

          testTask "the superseded opinion is archived, not overwritten" {
              // Without this the audit record would claim the new answer had always been the
              // answer, which is the one thing a correction must never do.
              do! reset ()
              believe "e1" (jan 10) (Active 1) 1L
              let archivedBefore = archivedCount ()

              let! _ =
                  (newCorrections ())
                      .Correct(machine, entity "e1", jan 12, [ corrected (jan 12) (Active 7) 1L ], noCancellation)

              Expect.isGreaterThan (archivedCount ()) archivedBefore "the replaced belief went to the history twin"
          }

          testTask "the correction is visible as a correction" {
              // The scenario from the design doc: one instant in the world, two opinions, and the
              // difference is the explanation an operator is asking for.
              do! reset ()
              believe "e1" (jan 10) (Active 1) 1L

              let before = DateTimeOffset.UtcNow
              do! Task.Delay 20

              let! _ =
                  (newCorrections ())
                      .Correct(machine, entity "e1", jan 12, [ corrected (jan 12) (Active 7) 1L ], noCancellation)

              let! held = (newTemporalReader ()).AsOf(machine, entity "e1", jan 13, before, noCancellation)
              let! now = (newTemporalReader ()).ValidAt(machine, entity "e1", jan 13, noCancellation)

              Expect.equal (Some(Active 1)) (stateOf held "as of before") "what we thought before the correction"

              Expect.equal (Some(Active 7)) (stateOf now "valid at") "what we think now"
          }

          testTask "an empty timeline ends the entity's history at the corrected instant" {
              do! reset ()
              believe "e1" (jan 10) (Active 1) 1L

              let! outcome = (newCorrections ()).Correct(machine, entity "e1", jan 12, [], noCancellation)

              Expect.equal (Corrected 1) (outcome |> expectOk "correct") "the live belief was split"

              let! after = (newTemporalReader ()).ValidAt(machine, entity "e1", jan 13, noCancellation)
              Expect.isNone (after |> expectOk "after") "the entity had no belief from then on"

              let! before = (newTemporalReader ()).ValidAt(machine, entity "e1", jan 11, noCancellation)
              Expect.isSome (before |> expectOk "before") "and still had one before"
          }

          testTask "a multi-belief timeline is written contiguously" {
              do! reset ()
              believe "e1" (jan 10) (Active 1) 1L

              let! _ =
                  (newCorrections ())
                      .Correct(
                          machine,
                          entity "e1",
                          jan 12,
                          [ corrected (jan 12) (Active 7) 1L
                            corrected (jan 14) (Active 8) 2L
                            corrected (jan 16) (Active 9) 3L ],
                          noCancellation
                      )

              // Each belief ends where the next begins, which the routine derives rather than
              // accepting, so a gap an as-of query could land in is not expressible.
              let bounds = liveBeliefs () |> List.map (fun (_, vfrom, vto) -> vfrom, vto)

              Expect.equal
                  [ (jan 10).UtcDateTime, (jan 12).UtcDateTime
                    (jan 12).UtcDateTime, (jan 14).UtcDateTime
                    (jan 14).UtcDateTime, (jan 16).UtcDateTime
                    (jan 16).UtcDateTime, DateTime.MaxValue ]
                  bounds
                  "contiguous, with the last left open"
          }

          testTask "beliefs out of valid-time order are refused before anything is written" {
              do! reset ()
              believe "e1" (jan 10) (Active 1) 1L

              let! outcome =
                  (newCorrections ())
                      .Correct(
                          machine,
                          entity "e1",
                          jan 12,
                          [ corrected (jan 16) (Active 7) 1L; corrected (jan 14) (Active 8) 2L ],
                          noCancellation
                      )

              match outcome with
              | Error(StoreError.Unexpected _) -> ()
              | other -> failtestf "expected the routine to refuse an unordered timeline, got %A" other

              // Refused before anything was written, which is why the original belief is intact.
              Expect.equal
                  (Some((jan 10).UtcDateTime, DateTime.MaxValue))
                  (liveBeliefs () |> List.tryHead |> Option.map (fun (_, vfrom, vto) -> vfrom, vto))
                  "the timeline was left alone"
          }

          testTask "a belief before the corrected instant is refused" {
              do! reset ()
              believe "e1" (jan 10) (Active 1) 1L

              let! outcome =
                  (newCorrections ())
                      .Correct(machine, entity "e1", jan 12, [ corrected (jan 11) (Active 7) 1L ], noCancellation)

              match outcome with
              | Error(StoreError.Unexpected _) -> ()
              | other -> failtestf "expected the routine to refuse a belief behind the instant, got %A" other
          } ]
