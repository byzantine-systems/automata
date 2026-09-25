module ByzantineSystems.Automata.Storage.Postgres.Tests.ReplayIntegrationTests

open System
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Runtime
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Postgres
open Expecto
open TestContext

/// Corrections through the inbox, end to end.
///
/// Every test lays down the same history: Start 1 at 10h, then Start 2 at 12h, so the entity
/// believes Active 1 from 10h and Active 3 from 12h. A missed Start 10 at 11h makes that Active 11
/// and Active 13.
let private entity name : Entity = entityId name

let private hour (h: int) =
    DateTimeOffset(2026, 3, 1, h, 0, 0, TimeSpan.Zero)

/// The total is capped at 100, which is how a replayed event can stop resolving.
let private cappedChart =
    statechart<TestState, TestEvent, TestAction, TestError> {
        root "root"

        classify (function
            | Idle -> stateId "idle"
            | Active _ -> stateId "active")

        state "idle" {
            on
                (fun _ event ->
                    match event with
                    | Start _ -> true
                    | Finish -> false)
                (fun _ event ->
                    match event with
                    | Start n -> [ Notify "started" ], Active n
                    | Finish -> [], Idle)
        }

        state "active" {
            on
                (fun state event ->
                    match state, event with
                    | Active total, Start n -> total + n <= 100
                    | _ -> false)
                (fun state event ->
                    match state, event with
                    | Active total, Start n -> [ Notify "added" ], Active(total + n)
                    | _ -> [], state)
        }
    }
    |> function
        | Ok c -> c
        | Error errors -> failwithf "the capped chart is invalid: %A" errors

let private build (catalog: ChartCatalog<TestState, TestEvent, TestAction, TestError> option) (current: int) =
    MachineCE.machine<Entity, TestState, TestEvent, TestAction, TestError> machine {
        chart cappedChart
        chartVersion current
        initialState Idle

        store (
            newMachineStore RetentionPolicy.keepEverything
            :> IMachineStore<Entity, TestState, TestEvent, TestAction, TestError>
        )

        chartCatalog (catalog |> Option.defaultValue ChartCatalog.empty)

        processor
            { ProcessorPolicy.defaults with
                PollingInterval = TimeSpan.FromMilliseconds 20. }
    }
    |> function
        | Ok built -> built
        | Error errors -> failtestf "the correction machine did not build: %A" errors

let private transitions name =
    rows
        "SELECT epoch FROM fsm.transition WHERE entity_id = @entity ORDER BY epoch"
        [ "entity", box name ]
        (fun reader -> reader.GetInt64 0)

let private drainUntil (built: Machine<Entity, TestState, TestEvent, TestAction, TestError>) (until: unit -> bool) =
    task {
        let deadline = DateTimeOffset.UtcNow.AddSeconds 20.
        let mutable settled = until ()

        while not settled && DateTimeOffset.UtcNow < deadline do
            let! _ = (Machine.processor built).PollAsync noCancellation
            settled <- until ()

        if not settled then
            failtest "the processor did not settle within the budget"
    }

let private accepted label outcome =
    match outcome with
    | Ok(Accepted id) -> id
    | other -> failtestf "%s: expected an acceptance, got %A" label other

let private at name key amount h =
    EventEnvelope.create key (Start amount) |> EventEnvelope.withReceivedAt (hour h)

/// A started machine holding the shared history for one entity.
let private seeded name =
    task {
        do! reset ()
        let built = build None 1
        let! _ = Machine.startAsync built (newRegistry ()) noCancellation
        let! _ = Machine.enqueue built (entity name) (at name "s1" 1 10) noCancellation
        let! _ = Machine.enqueue built (entity name) (at name "s2" 2 12) noCancellation
        do! drainUntil built (fun () -> (transitions name).Length = 2)
        return built
    }

let private correctAndSettle built name key amount h policy =
    task {
        let! submitted =
            Machine.correct built (entity name) (EventEnvelope.create key (Start amount)) (hour h) policy noCancellation

        let id = accepted "the correction" submitted

        do!
            drainUntil built (fun () ->
                (Machine.commandResult built id noCancellation).Result
                |> Result.map (function
                    | Some CommandResult.Pending
                    | None -> false
                    | Some _ -> true)
                |> Result.defaultValue false)

        let! result = Machine.commandResult built id noCancellation
        return result |> expectOk "the correction's result" |> Option.get
    }

let private believedAt name (validAt: DateTimeOffset) =
    task {
        let! belief = (newTemporalReader ()).ValidAt(machine, entity name, validAt, noCancellation)
        return belief |> expectOk "ValidAt" |> Option.map _.Snapshot.State
    }

let private liveState name =
    task {
        let! snapshot = (newReader ()).TryGetSnapshot(machine, entity name, noCancellation)
        return snapshot |> expectOk "snapshot" |> Option.map _.State
    }

let tests =
    testList
        "Postgres correction replay"
        [ testTask "a correction rewrites the timeline and keeps what was believed before it" {
              let! built = seeded "c"
              let knownBefore = DateTimeOffset(scalar<DateTime> "SELECT clock_timestamp()")

              match! correctAndSettle built "c" "fix" 10 11 CorrectionPolicy.defaults with
              | CommandResult.Committed committed ->
                  Expect.equal committed.Epoch (Epoch.ofUInt64 3UL) "the correction's own epoch"
              | other -> failtestf "expected the correction to commit, got %A" other

              let! at1130 = believedAt "c" (hour 11 + TimeSpan.FromMinutes 30.)
              let! at1230 = believedAt "c" (hour 12 + TimeSpan.FromMinutes 30.)
              Expect.equal at1130 (Some(Active 11)) "the missed event now holds from 11h"
              Expect.equal at1230 (Some(Active 13)) "and the later event was re-decided on top of it"

              let! before =
                  (newTemporalReader ())
                      .AsOf(machine, entity "c", hour 12 + TimeSpan.FromMinutes 30., knownBefore, noCancellation)

              Expect.equal
                  (before |> expectOk "AsOf" |> Option.map _.Snapshot.State)
                  (Some(Active 3))
                  "what was believed before the correction is still readable"

              let! live = liveState "c"
              Expect.equal live (Some(Active 13)) "the live state follows"
              do! Machine.stopAsync built noCancellation
          }

          testTask "an ordinary command after a correction commits" {
              // The live belief carries the correction's epoch. Were it attributed to the event
              // that produced it, the next command would expect an epoch already taken.
              let! built = seeded "after"
              let! _ = correctAndSettle built "after" "fix" 10 11 CorrectionPolicy.defaults
              let! _ = Machine.enqueue built (entity "after") (at "after" "s3" 1 13) noCancellation
              do! drainUntil built (fun () -> (transitions "after").Length = 4)

              let! live = liveState "after"
              Expect.equal live (Some(Active 14)) "the command applied on top of the corrected state"
              do! Machine.stopAsync built noCancellation
          }

          testTask "a correction waits behind a command in flight" {
              let! built = seeded "queued"
              let inbox = newInbox ()
              let! _ = Machine.enqueue built (entity "queued") (at "queued" "s3" 5 13) noCancellation
              let! claimed = inbox.Claim(machine, 1, TimeSpan.FromMinutes 1., noCancellation)
              let head = claimed |> expectOk "claim" |> List.exactlyOne

              let! submitted =
                  Machine.correct
                      built
                      (entity "queued")
                      (EventEnvelope.create "fix" (Start 10))
                      (hour 11)
                      CorrectionPolicy.defaults
                      noCancellation

              let _ = accepted "the correction" submitted

              match! (Machine.processor built).PollAsync noCancellation with
              | Ok report -> Expect.equal report.Summary.Claimed 0 "nothing is claimable behind the lease"
              | Error error -> failtestf "the poll failed: %A" error

              let! released =
                  inbox.Reschedule(
                      head.Work.CommandId,
                      head.Token,
                      Backoff.create (TimeSpan.FromMilliseconds 1.) (TimeSpan.FromMilliseconds 1.),
                      noCancellation
                  )

              released |> expectOk "reschedule" |> ignore
              do! drainUntil built (fun () -> (transitions "queued").Length = 4)

              // Start 5 at 13h ran first, and the correction replayed it: 1 + 10 + 2 + 5.
              let! live = liveState "queued"
              Expect.equal live (Some(Active 18)) "the correction saw the command ahead of it"
              do! Machine.stopAsync built noCancellation
          }

          testTask "a divergence dead-letters the correction and moves nothing" {
              // Start 99 at 11h makes 100, and Start 2 at 12h no longer fits under the cap.
              let! built = seeded "diverge"

              match! correctAndSettle built "diverge" "fix" 99 11 CorrectionPolicy.defaults with
              | CommandResult.DeadLettered(CommandFailure.Replay(ReplayError.Diverged(epoch, _))) ->
                  Expect.equal epoch (Epoch.ofUInt64 2UL) "the event that stopped resolving"
              | other -> failtestf "expected a divergence, got %A" other

              let! at1230 = believedAt "diverge" (hour 12 + TimeSpan.FromMinutes 30.)
              Expect.equal at1230 (Some(Active 3)) "the beliefs are untouched"
              Expect.equal (transitions "diverge").Length 2 "and no transition was written"
              do! Machine.stopAsync built noCancellation
          }

          testTask "truncating writes the timeline up to the divergence" {
              let! built = seeded "truncate"

              let policy =
                  { CorrectionPolicy.defaults with
                      OnDivergence = Divergence.Truncate }

              match! correctAndSettle built "truncate" "fix" 99 11 policy with
              | CommandResult.Committed _ -> ()
              | other -> failtestf "expected the truncated correction to commit, got %A" other

              let! at1230 = believedAt "truncate" (hour 12 + TimeSpan.FromMinutes 30.)
              let! live = liveState "truncate"
              Expect.equal at1230 (Some(Active 100)) "the diverged event no longer counts"
              Expect.equal live (Some(Active 100)) "nor does it in the live state"
              do! Machine.stopAsync built noCancellation
          }

          testTask "history the correction needs but retention removed is reported" {
              let! built = seeded "purged"
              arrange "DELETE FROM fsm.transition WHERE entity_id = 'purged' AND epoch = 1"

              match!
                  Machine.previewCorrection
                      built
                      (entity "purged")
                      (Start 10)
                      (hour 9)
                      CorrectionPolicy.defaults
                      noCancellation
              with
              | Error(CorrectionError.Replay ReplayError.HistoryPurged) -> ()
              | other -> failtestf "expected the purge to be reported, got %A" other

              do! Machine.stopAsync built noCancellation
          }

          testTask "a preview shows the correction and writes nothing" {
              let! built = seeded "preview"

              let! preview =
                  Machine.previewCorrection
                      built
                      (entity "preview")
                      (Start 10)
                      (hour 11)
                      CorrectionPolicy.defaults
                      noCancellation

              let plan = preview |> expectOk "preview"

              Expect.equal
                  (plan.Commit.Beliefs |> List.map _.Asserted.State)
                  [ Active 11; Active 13 ]
                  "the timeline it would write"

              let! at1130 = believedAt "preview" (hour 11 + TimeSpan.FromMinutes 30.)
              Expect.equal at1130 (Some(Active 1)) "nothing was written"
              Expect.equal (transitions "preview").Length 2 "not even a transition"
              do! Machine.stopAsync built noCancellation
          }

          testTask "a resubmitted correction corrects once" {
              let! built = seeded "twice"
              let! _ = correctAndSettle built "twice" "fix" 10 11 CorrectionPolicy.defaults

              let! again =
                  Machine.correct
                      built
                      (entity "twice")
                      (EventEnvelope.create "fix" (Start 10))
                      (hour 11)
                      CorrectionPolicy.defaults
                      noCancellation

              match again with
              | Ok(AlreadySubmitted _) -> ()
              | other -> failtestf "expected the duplicate to be recognised, got %A" other

              let! _ = (Machine.processor built).PollAsync noCancellation
              let! live = liveState "twice"
              Expect.equal live (Some(Active 13)) "applied once, not twice"
              Expect.equal (transitions "twice").Length 3 "one correction transition"
              do! Machine.stopAsync built noCancellation
          }

          testTask "a catalog chart that is not the registered one refuses the start" {
              // The fixture registered version 1 with a placeholder fingerprint, which no real
              // chart has.
              do! reset ()
              let built = build (Some(ChartCatalog.ofList [ 1, cappedChart ])) 2

              match! Machine.startAsync built (newRegistry ()) noCancellation with
              | Ok(Startup.Refused [ BootDefect.Misconfigured _ ]) -> ()
              | other -> failtestf "expected the start to be refused, got %A" other
          } ]
