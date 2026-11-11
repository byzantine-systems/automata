module ByzantineSystems.Automata.Storage.Postgres.Tests.ReplayFinalizeProperties

open System
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open Expecto
open FsCheck
open TestContext

let private entity name : Entity = entityId name

let private lease = TimeSpan.FromMinutes 1.

let private at = DateTimeOffset(2026, 3, 1, 11, 0, 0, TimeSpan.Zero)

// ---------------------------------------------------------------------------------------------
// The correction finalize fence. A correction command, like any command, may only be finished by
// the exact token that currently holds its lease; once released back to ready, no token is
// authority. The commit itself is minimal but valid, so the one path that should finalize does.
// ---------------------------------------------------------------------------------------------

let private commit (name: string) : CorrectionCommit<Entity, TestState, TestEvent, TestAction> =
    let belief: CorrectedBelief<TestState> =
        { Asserted =
            { State = Active 1
              Status = Running
              Epoch = Epoch.ofUInt64 1UL }
          ValidFrom = at
          CommandId = CommandId.ofInt64 1L
          ChartVersion = version }

    { Transition = draft (entity name) Idle (Active 1) at
      ValidFrom = at
      Beliefs = belief, [] }

type private FinalizeCase = { Token: byte; Rescheduled: bool }

let private finalizeProperty =
    testCase "a correction finalizes only for the token that holds its lease" (fun () ->
        let config = Config.QuickThrowOnFailure.WithMaxTest(40).WithEndSize(10)

        Check.One(
            config,
            fun (case: FinalizeCase) ->
                task {
                    do! reset ()
                    let name = "e1"

                    let correction =
                        { submission (entity name) "c1" (Start 10) with
                            Kind = CommandKind.Correction(at, CorrectionPolicy.defaults) }

                    let! _ = (newInbox ()).Submit(correction, noCancellation)

                    let! claimed =
                        (newInbox ()).Claim(machine, ClaimBatch.create 1, LeaseDuration.create lease, noCancellation)

                    let held = claimed |> expectOk "claim" |> List.exactlyOne

                    if case.Rescheduled then
                        let! rescheduled =
                            (newInbox ())
                                .Reschedule(
                                    held.Work.CommandId,
                                    held.Token,
                                    Backoff.create (TimeSpan.FromMilliseconds 1.) (TimeSpan.FromMilliseconds 1.),
                                    noCancellation
                                )

                        Expect.equal (rescheduled |> expectOk "reschedule") LeaseUpdateOutcome.Updated "back to ready"

                    let presented =
                        match int case.Token % 4 with
                        | 0 -> held.Token
                        | 1 -> LeaseToken.ofInt64 0L
                        | 2 -> LeaseToken.ofInt64 (max 0L (LeaseToken.value held.Token - 1L))
                        | _ -> LeaseToken.ofInt64 (LeaseToken.value held.Token + 100L)

                    let! outcome =
                        (newReplayStore ())
                            .CommitCorrection(
                                held.Work.CommandId,
                                presented,
                                Epoch.initial,
                                commit name,
                                noCancellation
                            )

                    let result = outcome |> expectOk "finalize"
                    let shouldFinalize = int case.Token % 4 = 0 && not case.Rescheduled

                    return
                        match result, shouldFinalize with
                        | FinalizeOutcome.Finalized _, true -> true
                        | FinalizeOutcome.LeaseLost, false -> true
                        | _ -> false
                }
        ))

let tests = testList "Postgres replay finalize properties" [ finalizeProperty ]
