module ByzantineSystems.Automata.Storage.Postgres.Tests.ReplayRetentionProperties

open System
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open Expecto
open FsCheck
open TestContext

let private entity name : Entity = entityId name

let private baseTime = DateTimeOffset(2026, 3, 1, 0, 0, 0, TimeSpan.Zero)

let private hour = TimeSpan.FromHours 1.
let private lease = TimeSpan.FromSeconds 30.

let private keepCommandsFor span =
    { RetentionPolicy.keepEverything with
        Commands = Retain.For span }

// ---------------------------------------------------------------------------------------------
// Arranging a real timeline through the store, then ageing it and running retention. Only the
// clock is bent with SQL; every transition and command is written the way an application would.
// ---------------------------------------------------------------------------------------------

let private submit (name: string) (key: string) receivedAt =
    task {
        let request =
            { submission (entity name) key (Start 1) with
                ReceivedAt = receivedAt }

        let! outcome = (newInbox ()).SubmitAsync(request, noCancellation)
        outcome |> expectOk "submit" |> ignore
    }

let private commitAt (name: string) (key: string) (effectiveAt: DateTimeOffset) (expected: Epoch) =
    task {
        let! batch = (newInbox ()).ClaimAsync(machine, ClaimBatch.create 50, LeaseDuration.create lease, noCancellation)

        let leased =
            batch
            |> expectOk "claim"
            |> List.tryFind (fun (c: LeasedCommand<Entity, TestEvent>) -> c.Work.IdempotencyKey = key)
            |> Option.defaultWith (fun () -> failtestf "command %s was not claimable" key)

        let! outcome =
            (newProcessor ())
                .CommitAsync(
                    leased.Work.CommandId,
                    leased.Token,
                    expected,
                    draft leased.Work.EntityId Idle (Active 1) effectiveAt,
                    noCancellation
                )

        outcome |> expectOk "commit" |> ignore
    }

let private suffixIncomplete (name: string) (from: DateTimeOffset) : Task<bool> =
    task {
        let! result = (newReplayStore ()).Log.SuffixAsync(machine, entity name, from, 100, noCancellation)
        return (result |> expectOk "suffix").Incomplete
    }

// ---------------------------------------------------------------------------------------------
// The algebra: retention maintains a monotonic watermark, the greatest effective instant of any
// purged transition, and a suffix is incomplete exactly when its start is not after it.
//
//   incomplete(from)  <=>  maxPurgedEffectiveAt >= from
//
// The timeline is built from positive deltas so it is strictly ascending by construction, and the
// oracle is the reference maximum over the purged prefix, not a restatement of the store's SQL.
// ---------------------------------------------------------------------------------------------

type private RetentionCase =
    { Deltas: byte list
      Purge: byte
      From: byte }

let private retentionProperty =
    testCase "a suffix is incomplete exactly when the purge watermark reaches its start" (fun () ->
        let config = Config.QuickThrowOnFailure.WithMaxTest(40).WithEndSize(10)

        Check.One(
            config,
            fun (case: RetentionCase) ->
                task {
                    do! reset ()
                    do! boot (keepCommandsFor hour)
                    let name = "e1"

                    let times =
                        // Construct at least one purgeable transition and a pinned
                        // survivor. Empty/singleton traces otherwise make this law
                        // pass without ever evaluating its watermark boundary.
                        (match case.Deltas with
                         | [] -> [ 0uy; 0uy ]
                         | [ only ] -> [ only; 0uy ]
                         | many -> many)
                        |> List.truncate 5
                        |> List.map (fun delta -> 1 + int delta % 30)
                        |> List.scan (+) 0
                        |> List.skip 1
                        |> List.map (float >> baseTime.AddMinutes)

                    let purged = 1 + int case.Purge % (times.Length - 1)

                    for index in 0 .. times.Length - 1 do
                        do! submit name $"k{index}" (if index < purged then Some baseTime else None)

                    for index, effectiveAt in List.indexed times do
                        do! commitAt name $"k{index}" effectiveAt (Epoch.ofUInt64 (uint64 index))

                    let! intact = suffixIncomplete name baseTime
                    Expect.isFalse intact "before retention, the full suffix is present"
                    let! reports = (newMaintenance ()).RunAsync(ClaimBatch.create 100, noCancellation)

                    let report =
                        reports
                        |> expectOk "maintenance"
                        |> List.find (fun report -> report.MachineId = machine)

                    Expect.equal report.PurgedCommands (int64 purged) "the generated prefix was actually purged"
                    let watermark = times[purged - 1]

                    let probes =
                        [ watermark.AddTicks -10L
                          watermark
                          watermark.AddTicks 10L
                          baseTime.AddMinutes(float (int case.From % 120)) ]

                    let mutable agrees = true

                    for probe in probes do
                        let! incomplete = suffixIncomplete name probe
                        agrees <- agrees && incomplete = (watermark >= probe)

                    return agrees
                }
        ))

// ---------------------------------------------------------------------------------------------
// Fixed cases the generator cannot always land on, kept as regressions because each one is the
// exact failure mode the epoch heuristic got wrong.
// ---------------------------------------------------------------------------------------------

let private regressions =
    testList
        "Postgres replay retention"
        [ testTask "a suffix starting after the watermark is complete" {
              // Epoch 1 (10h) is purged, epoch 2 (12h) survives. A suffix from 11h needs only the
              // survivor, so it must not be marked incomplete.
              do! reset ()
              do! boot (keepCommandsFor hour)
              let name = "gap"
              do! submit name "k1" (Some baseTime)
              do! submit name "k2" None
              do! commitAt name "k1" (baseTime.AddHours 10.) Epoch.initial
              do! commitAt name "k2" (baseTime.AddHours 12.) (Epoch.ofUInt64 1UL)

              let! _ = (newMaintenance ()).RunAsync(ClaimBatch.create 100, noCancellation)

              let! after = suffixIncomplete name (baseTime.AddHours 11.)
              Expect.isFalse after "the surviving 12h transition is all a suffix from 11h needs"

              let! before = suffixIncomplete name (baseTime.AddHours 9.)
              Expect.isTrue before "the purged 10h transition is part of a suffix from 9h"
          }

          testTask "a suffix starting exactly at the watermark is incomplete" {
              // The boundary is half-open the same way the suffix query is: effective_at >= from.
              do! reset ()
              do! boot (keepCommandsFor hour)
              let name = "edge"
              do! submit name "k1" (Some baseTime)
              do! submit name "k2" None
              do! commitAt name "k1" (baseTime.AddHours 10.) Epoch.initial
              do! commitAt name "k2" (baseTime.AddHours 12.) (Epoch.ofUInt64 1UL)

              let! _ = (newMaintenance ()).RunAsync(ClaimBatch.create 100, noCancellation)

              let! exactly = suffixIncomplete name (baseTime.AddHours 10.)
              Expect.isTrue exactly "the purged transition is effective exactly at the watermark"
          }

          testTask "a rejection does not make the snapshot's successful command purgeable" {
              // The newest command is a rejection with no transition, so purging the committed
              // predecessor empties the log. The watermark still says a suffix before it is lost.
              do! reset ()
              do! boot (keepCommandsFor hour)
              let name = "gone"
              do! submit name "k1" (Some baseTime)
              do! submit name "k2" None
              do! commitAt name "k1" (baseTime.AddHours 10.) Epoch.initial

              let! batch =
                  (newInbox ()).ClaimAsync(machine, ClaimBatch.create 50, LeaseDuration.create lease, noCancellation)

              let k2 =
                  batch
                  |> expectOk "claim"
                  |> List.tryFind (fun (c: LeasedCommand<Entity, TestEvent>) -> c.Work.IdempotencyKey = "k2")
                  |> Option.defaultWith (fun () -> failtest "the second command was not claimable")

              let! _ =
                  (newProcessor ())
                      .RejectAsync(k2.Work.CommandId, k2.Token, CommandFailure.Machine "refused", noCancellation)

              let! _ = (newMaintenance ()).RunAsync(ClaimBatch.create 100, noCancellation)

              Expect.equal
                  (scalar<int64> $"SELECT count(*) FROM fsm.transition WHERE entity_id = '{name}'")
                  1L
                  "the snapshot pins its producing command and transition"

              let! before = suffixIncomplete name (baseTime.AddHours 9.)
              Expect.isFalse before "no committed transition was removed"
          } ]

let tests =
    testList "Postgres replay retention properties" [ retentionProperty; regressions ]
