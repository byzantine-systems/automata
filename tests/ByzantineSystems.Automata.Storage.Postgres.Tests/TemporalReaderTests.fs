module ByzantineSystems.Automata.Storage.Postgres.Tests.TemporalReaderTests

open System
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open Expecto
open TestContext

/// <summary>
/// The F# end of the two time axes.
///
/// The SQL underneath is already proven by TemporalTests. What is proven here is that the
/// contract above it answers the questions the design was built for, and that an open bound
/// crosses the boundary as <c>None</c> rather than as a sentinel instant.
/// </summary>
let private entity name : Entity = entityId name

let private jan (hour: int) =
    DateTimeOffset(2026, 1, 1, hour, 0, 0, TimeSpan.Zero)

/// Typed so a lambda reading a belief does not have to be annotated at every call site.
let private stateOf (result: Result<Belief<Entity, TestState> option, StoreError>) (label: string) =
    result |> expectOk label |> Option.map (fun belief -> belief.Snapshot.State)

let private expectBelief (result: Result<Belief<Entity, TestState> option, StoreError>) (label: string) =
    match result |> expectOk label with
    | Some belief -> belief
    | None -> failtestf "%s should have found a belief" label

let tests =
    testList
        "Postgres temporal reader"
        [ testTask "an entity with no belief has nothing to read" {
              do! reset ()
              let! belief = (newTemporalReader ()).ValidAt(machine, entity "missing", jan 12, noCancellation)
              Expect.isNone (belief |> expectOk "valid at") "no belief, no answer"
          }

          testTask "an instant before the entity existed has nothing to read" {
              do! reset ()
              believe "e1" (jan 10) (Active 1) 1L
              let! belief = (newTemporalReader ()).ValidAt(machine, entity "e1", jan 9, noCancellation)
              Expect.isNone (belief |> expectOk "valid at") "the entity had no belief then"
          }

          testTask "a live belief reads back with both upper bounds open" {
              do! reset ()
              believe "e1" (jan 10) (Active 1) 1L

              let! belief = (newTemporalReader ()).ValidAt(machine, entity "e1", jan 12, noCancellation)

              let belief = expectBelief belief "valid at"

              Expect.equal (Active 1) belief.Snapshot.State "the state"
              Expect.equal Running belief.Snapshot.Status "the lifecycle"
              Expect.equal (Epoch.ofUInt64 1UL) belief.Snapshot.Epoch "the transition that produced it"
              Expect.equal (jan 10) belief.ValidFrom "when it started being true"
              // 'infinity' crosses the boundary as None. A sentinel instant would read as a
              // belief that expires in the year 9999, which is not what the row says.
              Expect.isNone belief.ValidTo "still true"
              Expect.isNone belief.KnownTo "nothing has superseded it"
          }

          testTask "business time alone decides which belief answers" {
              do! reset ()
              believe "e1" (jan 10) (Active 1) 1L
              believe "e1" (jan 14) (Active 2) 2L

              let! earlier = (newTemporalReader ()).ValidAt(machine, entity "e1", jan 12, noCancellation)
              let! later = (newTemporalReader ()).ValidAt(machine, entity "e1", jan 16, noCancellation)

              Expect.equal (Some(Active 1)) (stateOf earlier "earlier") "the belief that covered noon"

              Expect.equal (Some(Active 2)) (stateOf later "later") "the belief that covered the afternoon"
          }

          testTask "a belief closed by a later one reports the bound it was closed at" {
              do! reset ()
              believe "e1" (jan 10) (Active 1) 1L
              believe "e1" (jan 14) (Active 2) 2L

              let! belief = (newTemporalReader ()).ValidAt(machine, entity "e1", jan 12, noCancellation)

              Expect.equal
                  (Some(jan 14))
                  (expectBelief belief "valid at").ValidTo
                  "it stopped being true when the next began"
          }

          testTask "AsOf with a knownAt before this database believed anything reads nothing" {
              do! reset ()
              believe "e1" (jan 10) (Active 1) 1L

              let! belief =
                  (newTemporalReader ())
                      .AsOf(
                          machine,
                          entity "e1",
                          jan 12,
                          DateTimeOffset(2020, 1, 1, 0, 0, 0, TimeSpan.Zero),
                          noCancellation
                      )

              Expect.isNone (belief |> expectOk "as of") "this database held no opinion then"
          }

          testTask "ValidAt is AsOf with the present as its second coordinate" {
              do! reset ()
              believe "e1" (jan 10) (Active 1) 1L

              let! now = (newTemporalReader ()).ValidAt(machine, entity "e1", jan 12, noCancellation)

              let! explicit =
                  (newTemporalReader ()).AsOf(machine, entity "e1", jan 12, DateTimeOffset.UtcNow, noCancellation)

              Expect.equal (stateOf now "valid at") (stateOf explicit "as of") "the same question asked two ways"
          } ]
