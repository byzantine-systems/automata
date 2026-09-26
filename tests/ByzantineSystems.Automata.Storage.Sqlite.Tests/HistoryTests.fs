module ByzantineSystems.Automata.Storage.Sqlite.Tests.HistoryTests

open System
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Sqlite.Tests.TestContext
open Expecto

type private Committed = CommittedTransition<Entity, TestState, TestEvent, TestAction>

let private entity name : Entity = entityId name
let private effectiveAt = DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)

/// Submits, claims and commits one command, which is the only way a transition gets into the log.
let private commit (fixture: Fixture) (name: string) (key: string) (toState: TestState) : Task =
    task {
        let! held = leased (inboxOf fixture) (entity name) key
        let! snapshot = (readerOf fixture).TryGetSnapshot(testMachine, entity name, noCancellation)

        let expected =
            snapshot
            |> expectOk "snapshot"
            |> Option.map _.Epoch
            |> Option.defaultValue Epoch.initial

        let! outcome =
            (processorOf fixture)
                .Commit(
                    held.Work.CommandId,
                    held.Token,
                    expected,
                    draft held.Work.EntityId Idle toState effectiveAt,
                    noCancellation
                )

        outcome |> expectOk "commit" |> ignore
    }

let private page (fixture: Fixture) (name: string) (paging: Page) : Task<Committed list> =
    task {
        let! rows = (readerOf fixture).History(testMachine, entity name, paging, noCancellation)
        return rows |> expectOk "history"
    }

let private epochsOf (rows: Committed list) =
    rows |> List.map (_.Epoch >> Epoch.value)

let tests =
    testList
        "IStateReader"
        [ testTask "an entity that never committed has no snapshot and an empty history" {
              let db = fixture "history"
              let! snapshot = (readerOf db).TryGetSnapshot(testMachine, entity "missing", noCancellation)
              Expect.isNone (snapshot |> expectOk "snapshot") "no commit, no snapshot"

              let! rows = page db "missing" (Page.create 10)
              Expect.isEmpty rows "no log, no page"
          }

          testTask "the snapshot is the latest commit's state, epoch and status" {
              let db = fixture "history"
              do! commit db "e1" "k1" (Active 1)
              do! commit db "e1" "k2" (Active 2)

              let! snapshot = (readerOf db).TryGetSnapshot(testMachine, entity "e1", noCancellation)

              match snapshot |> expectOk "snapshot" with
              | Some current ->
                  Expect.equal current.State (Active 2) "the latest state"
                  Expect.equal current.Epoch (Epoch.ofUInt64 2UL) "the latest epoch"
                  Expect.equal current.Status Running "the latest status"
              | None -> failtest "the entity has committed"

              Expect.equal (scalar db.Db "SELECT count(*) FROM fsm_entity_snapshot;") "1" "one row, replaced in place"
          }

          testTask "transitions page back oldest first" {
              let db = fixture "history"
              do! commit db "e1" "k1" (Active 1)
              do! commit db "e1" "k2" (Active 2)
              do! commit db "e1" "k3" (Active 3)

              let! rows = page db "e1" (Page.create 10)
              Expect.equal (epochsOf rows) [ 1UL; 2UL; 3UL ] "ascending, because this is a log"
          }

          testTask "a limit bounds the page" {
              let db = fixture "history"
              do! commit db "e1" "k1" (Active 1)
              do! commit db "e1" "k2" (Active 2)
              do! commit db "e1" "k3" (Active 3)

              let! rows = page db "e1" (Page.create 2)
              Expect.equal (epochsOf rows) [ 1UL; 2UL ] "only what was asked for, from the start"
          }

          testTask "the cursor is exclusive and does not move as the log grows" {
              let db = fixture "history"
              do! commit db "e1" "k1" (Active 1)
              do! commit db "e1" "k2" (Active 2)

              let! before = page db "e1" (Page.after (Epoch.ofUInt64 1UL) 10)
              do! commit db "e1" "k3" (Active 3)
              let! after = page db "e1" (Page.after (Epoch.ofUInt64 1UL) 10)

              Expect.equal (epochsOf before) [ 2UL ] "the cursor epoch itself is not repeated"
              Expect.equal (epochsOf after) [ 2UL; 3UL ] "and a page pinned to it only grows at the end"
          }

          testTask "one entity's log does not contain another's" {
              let db = fixture "history"
              do! commit db "e1" "k1" (Active 1)
              do! commit db "e2" "k2" (Active 2)

              let! rows = page db "e1" (Page.create 10)
              Expect.equal (rows |> List.map _.Draft.EntityId) [ entity "e1" ] "only this entity's transitions"
          }

          testTask "a paged transition round-trips what was committed" {
              let db = fixture "history"
              do! commit db "e1" "k1" (Active 7)

              let! rows = page db "e1" (Page.create 10)
              let committed = List.exactlyOne rows

              Expect.equal committed.Draft.Event Finish "the event"
              Expect.equal committed.Draft.Actions [ Notify "done" ] "the actions"
              Expect.equal committed.Draft.FromState Idle "the state it resolved from"
              Expect.equal committed.Draft.ToState (Active 7) "the state it produced"
              Expect.equal committed.Draft.HandledBy (stateId "root") "the node that handled it"
              Expect.equal committed.Draft.EffectiveAt effectiveAt "business time, as supplied"
              Expect.equal committed.ChartVersion version "the chart version it was decided under"
              Expect.equal committed.CommittedAt startTime "commit time, from the store's clock"
          } ]
