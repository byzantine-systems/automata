module ByzantineSystems.Automata.Storage.Postgres.Tests.HistoryTests

open System
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open Expecto
open TestContext

let private entity name : Entity = entityId name
let private lease = TimeSpan.FromSeconds 30.0
let private effectiveAt = DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)

/// Submits, claims and commits one command, which is the only way a transition gets into the log.
let private commit (name: string) (key: string) (toState: TestState) =
    task {
        let inbox = newInbox ()
        let! _ = inbox.Submit(submission (entity name) key (Start 1), noCancellation)
        let! claimed = inbox.Claim(machine, 50, lease, noCancellation)

        let held =
            claimed
            |> expectOk "claim"
            |> List.tryFind (fun (c: LeasedCommand<Entity, TestEvent>) -> c.Work.IdempotencyKey = key)
            |> Option.defaultWith (fun () -> failtestf "the command %s was not claimable" key)

        let! snapshot = (newReader ()).TryGetSnapshot(machine, entity name, noCancellation)

        let expected =
            snapshot
            |> expectOk "snapshot"
            |> Option.map (fun s -> s.Epoch)
            |> Option.defaultValue Epoch.initial

        let! outcome =
            (newProcessor ())
                .Commit(
                    held.Work.CommandId,
                    held.Token,
                    expected,
                    draft held.Work.EntityId Idle toState effectiveAt,
                    noCancellation
                )

        outcome |> expectOk "commit" |> ignore
    }

let private page
    (name: string)
    (paging: Page)
    : Task<CommittedTransition<Entity, TestState, TestEvent, TestAction> list> =
    task {
        let! rows = (newReader ()).History(machine, entity name, paging, noCancellation)
        return rows |> expectOk "history"
    }

/// Annotated so a lambda over a page does not need annotating at every call site.
let private epochsOf (rows: CommittedTransition<Entity, TestState, TestEvent, TestAction> list) =
    rows |> List.map (fun transition -> Epoch.value transition.Epoch)

let tests =
    testList
        "Postgres transition history"
        [ testTask "an entity with no transitions pages to an empty list" {
              do! reset ()
              let! rows = page "missing" (Page.create 10)
              Expect.isEmpty rows "no log, no page"
          }

          testTask "transitions page back oldest first" {
              do! reset ()
              do! commit "e1" "k1" (Active 1)
              do! commit "e1" "k2" (Active 2)
              do! commit "e1" "k3" (Active 3)

              let! rows = page "e1" (Page.create 10)

              Expect.equal [ 1UL; 2UL; 3UL ] (epochsOf rows) "ascending, because this is a log rather than a feed"
          }

          testTask "a limit bounds the page" {
              do! reset ()
              do! commit "e1" "k1" (Active 1)
              do! commit "e1" "k2" (Active 2)
              do! commit "e1" "k3" (Active 3)

              let! rows = page "e1" (Page.create 2)
              Expect.equal 2 (List.length rows) "only what was asked for"
          }

          testTask "the cursor is exclusive" {
              do! reset ()
              do! commit "e1" "k1" (Active 1)
              do! commit "e1" "k2" (Active 2)
              do! commit "e1" "k3" (Active 3)

              let! rows = page "e1" (Page.after (Epoch.ofUInt64 1UL) 10)

              Expect.equal [ 2UL; 3UL ] (epochsOf rows) "the cursor epoch itself is not repeated"
          }

          testTask "a page pinned to an epoch does not move as the log grows" {
              // This is why the cursor is an epoch rather than an offset. An offset shifts as
              // rows arrive, so paging a growing log with one silently skips or repeats.
              do! reset ()
              do! commit "e1" "k1" (Active 1)
              do! commit "e1" "k2" (Active 2)

              let! before = page "e1" (Page.after (Epoch.ofUInt64 1UL) 10)
              do! commit "e1" "k3" (Active 3)
              let! after = page "e1" (Page.after (Epoch.ofUInt64 1UL) 10)

              Expect.equal
                  (epochsOf before)
                  (epochsOf after |> List.truncate (List.length before))
                  "the rows the first page returned are still the rows it returns"
          }

          testTask "one entity's log does not contain another's" {
              do! reset ()
              do! commit "e1" "k1" (Active 1)
              do! commit "e2" "k2" (Active 2)

              let! rows = page "e1" (Page.create 10)
              Expect.equal 1 (List.length rows) "only this entity's transitions"
              Expect.equal (entity "e1") (List.head rows).Draft.EntityId "and they are this entity's"
          }

          testTask "a paged transition round-trips what was committed" {
              // The log is only replayable if what comes back decodes to what went in.
              do! reset ()
              do! commit "e1" "k1" (Active 7)

              let! rows = page "e1" (Page.create 10)
              let committed = List.head rows

              Expect.equal Finish committed.Draft.Event "the event"
              Expect.equal [ Notify "done" ] committed.Draft.Actions "the actions"
              Expect.equal Idle committed.Draft.FromState "the state it resolved from"
              Expect.equal (Active 7) committed.Draft.ToState "the state it produced"
              Expect.equal effectiveAt committed.Draft.EffectiveAt "business time, as supplied"
              Expect.equal version committed.ChartVersion "the chart version it was decided under"
          }

          testTask "a page of one is still a page" {
              do! reset ()
              do! commit "e1" "k1" (Active 1)
              let! rows = page "e1" (Page.create 1)
              Expect.equal 1 (List.length rows) "one row"
          }

          test "a page must ask for at least one row" {
              Expect.throws (fun () -> Page.create 0 |> ignore) "a page of nothing is not a question"
          } ]
