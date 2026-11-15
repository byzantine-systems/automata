namespace ByzantineSystems.Automata.Storage

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage.ContractProperties
open Expecto
open FsCheck

/// <summary>Capability-gated correction equations, shared by timeline and snapshot-based implementations.</summary>
[<RequireQualifiedAccess>]
module CorrectionContract =
    type Store = IMachineStore<ContractEntityId, ContractState, ContractEvent, ContractAction, ContractError>

    type Environment =
        { Machine: MachineId
          Version: ChartVersion
          Fresh: unit -> Task<Store> }

    let private scenario (env: Environment) (first: byte) (later: byte list) (missed: byte) =
        task {
            let! store = env.Fresh()

            let capability =
                Store.tryCorrections store
                |> Option.defaultWith (fun () ->
                    failtest "the provider advertised corrections without their three operations")

            let ct = CancellationToken.None
            let inbox = store :> ContractInbox
            let processor = store :> ContractProcessor
            let reader = store :> ContractReader
            let entity: ContractEntityId = entityId "correction-law"

            let at hour =
                DateTimeOffset(2026, 1, 1, hour, 0, 0, TimeSpan.Zero)

            let laterValues = List.truncate 4 later |> List.map int
            let a, b, c = int first, List.sum laterValues, int missed
            let beforeEpoch = Epoch.ofUInt64 (uint64 (1 + laterValues.Length))
            let correctedEpoch = Epoch.next beforeEpoch

            let submit key kind event =
                task {
                    let! submitted =
                        inbox.SubmitAsync(
                            { MachineId = env.Machine
                              EntityId = entity
                              IdempotencyKey = key
                              ChartVersion = env.Version
                              Kind = kind
                              Event = event
                              VisibleAt = None
                              ReceivedAt = None
                              Audit = AuditContext.empty },
                            ct
                        )

                    match submitted with
                    | Ok(Accepted _) -> ()
                    | other -> failtestf "submission: %A" other

                    let! claimed =
                        inbox.ClaimAsync(
                            env.Machine,
                            ClaimBatch.create 1,
                            LeaseDuration.create (TimeSpan.FromMinutes 1.),
                            ct
                        )

                    return
                        claimed
                        |> function
                            | Ok [ held ] -> held
                            | other -> failtestf "claim: %A" other
                }

            let draft event fromState toState instant actions =
                { MachineId = env.Machine
                  EntityId = entity
                  Event = event
                  FromState = fromState
                  ToState = toState
                  EffectiveAt = instant
                  Status = InstanceStatus.Running
                  Actions = actions
                  HandledBy = stateId "root"
                  Exited = []
                  Entered = [] }

            let! one = submit "first" CommandKind.Event (Ping a)

            let! committed =
                processor.CommitAsync(
                    one.Work.CommandId,
                    one.Token,
                    Epoch.initial,
                    draft (Ping a) Idle (Active a) (at 10) [ Fired a ],
                    ct
                )

            Expect.equal committed (Ok(Finalized(Epoch.ofUInt64 1UL))) "first epoch"
            let laterIds = ResizeArray<CommandId>()
            let mutable total = a

            for index, amount in List.indexed laterValues do
                let! held = submit $"later-{index}" CommandKind.Event (Ping amount)

                let! committed =
                    processor.CommitAsync(
                        held.Work.CommandId,
                        held.Token,
                        Epoch.ofUInt64 (uint64 (index + 1)),
                        draft (Ping amount) (Active total) (Active(total + amount)) (at (12 + index)) [ Fired amount ],
                        ct
                    )

                Expect.equal committed (Ok(Finalized(Epoch.ofUInt64 (uint64 (index + 2))))) "suffix epoch"
                laterIds.Add held.Work.CommandId
                total <- total + amount

            let! prior = capability.Prior.BeforeAsync(env.Machine, entity, at 11, ct)

            Expect.equal
                (prior |> Result.map (Option.map (fun (s: Snapshot<ContractState>) -> s.State)))
                (Ok(Some(Active a)))
                "prior-state is strictly before the correction"

            let! suffix = capability.Log.SuffixAsync(env.Machine, entity, at 11, 10, ct)

            match suffix with
            | Ok suffix ->
                Expect.isFalse suffix.Incomplete "no retention occurred"

                Expect.equal (suffix.Transitions |> List.map _.CommandId) (List.ofSeq laterIds) "the exact suffix"
            | Error error -> failtestf "suffix: %A" error

            let! correction = submit "missed" (CommandKind.Correction(at 11, CorrectionPolicy.defaults)) (Ping c)

            let belief instant total =
                { Asserted =
                    { State = Active total
                      Status = InstanceStatus.Running
                      Epoch = correctedEpoch }
                  ValidFrom = instant
                  CommandId = correction.Work.CommandId
                  ChartVersion = env.Version }

            let correctionCommit =
                { Transition = draft (Ping c) (Active(a + b)) (Active(a + b + c)) (at 11) []
                  ValidFrom = at 11
                  Beliefs =
                    (belief (at 11) (a + c),
                     laterValues
                     |> List.scan (+) (a + c)
                     |> List.tail
                     |> List.mapi (fun i total -> belief (at (12 + i)) total)) }

            let! before = reader.TryGetSnapshotAsync(env.Machine, entity, ct)

            let! fenced =
                capability.Committer.CommitCorrectionAsync(
                    correction.Work.CommandId,
                    LeaseToken.ofInt64 0L,
                    beforeEpoch,
                    correctionCommit,
                    ct
                )

            Expect.equal fenced (Ok FinalizeOutcome.LeaseLost) "zero token has no authority"
            let! unchanged = reader.TryGetSnapshotAsync(env.Machine, entity, ct)
            Expect.equal unchanged before "a fenced correction writes nothing"

            let! conflict =
                capability.Committer.CommitCorrectionAsync(
                    correction.Work.CommandId,
                    correction.Token,
                    Epoch.initial,
                    correctionCommit,
                    ct
                )

            Expect.equal conflict (Ok(Conflict(Epoch.initial, beforeEpoch))) "epoch conflict"

            let! corrected =
                capability.Committer.CommitCorrectionAsync(
                    correction.Work.CommandId,
                    correction.Token,
                    beforeEpoch,
                    correctionCommit,
                    ct
                )

            Expect.equal corrected (Ok(Finalized correctedEpoch)) "correction advances the core epoch"

            let! repeated =
                capability.Committer.CommitCorrectionAsync(
                    correction.Work.CommandId,
                    correction.Token,
                    beforeEpoch,
                    correctionCommit,
                    ct
                )

            Expect.equal
                repeated
                (Ok(AlreadyFinalized(correctedEpoch, CommandStatus.Succeeded)))
                "ambiguous commits are safe to retry"

            let! snapshot = reader.TryGetSnapshotAsync(env.Machine, entity, ct)

            Expect.equal
                (snapshot
                 |> Result.map (Option.map (fun (s: Snapshot<ContractState>) -> s.State, s.Epoch)))
                (Ok(Some(Active(a + b + c), correctedEpoch)))
                "the final replayed state is the live snapshot"

            match Store.tryTemporal store with
            | Some temporal ->
                let expectedTotals = laterValues |> List.scan (+) (a + c)

                for index, expectedTotal in List.indexed expectedTotals do
                    let instant = if index = 0 then at 11 else at (11 + index)
                    let! belief = temporal.ValidAtAsync(env.Machine, entity, instant, ct)

                    Expect.equal
                        (belief
                         |> Result.map (
                             Option.map (fun (value: Belief<ContractEntityId, ContractState>) -> value.Snapshot.State)
                         ))
                        (Ok(Some(Active expectedTotal)))
                        "generated replacement agrees with the additive model"
            | None -> ()

            let! actions =
                (store :> ContractActionQueue)
                    .ClaimAsync(env.Machine, ClaimBatch.create 10, LeaseDuration.create (TimeSpan.FromMinutes 1.), ct)

            Expect.equal
                (actions |> Result.map List.length)
                (Ok(1 + laterValues.Length))
                "corrections never re-emit historical actions"

            let! next = submit "next" CommandKind.Event (Ping 1)

            let! nextCommit =
                processor.CommitAsync(
                    next.Work.CommandId,
                    next.Token,
                    correctedEpoch,
                    draft (Ping 1) Idle (Active(a + b + c + 1)) (at (13 + laterValues.Length)) [],
                    ct
                )

            Expect.equal
                nextCommit
                (Ok(Finalized(Epoch.next correctedEpoch)))
                "ordinary processing continues after correction"

            let! history = reader.HistoryAsync(env.Machine, entity, Page.create 10, ct)

            match history with
            | Ok history ->
                Expect.equal
                    (List.last history).Draft.FromState
                    (Active(a + b + c))
                    "the next ordinary origin is the corrected snapshot"
            | Error error -> failtestf "history: %A" error

            return true
        }

    let tests (env: Environment) =
        testCase "corrections share fencing, prior-state, suffix, and snapshot equations" (fun () ->
            Check.One(
                Config.QuickThrowOnFailure.WithMaxTest(12),
                fun (first: byte, later: byte list, missed: byte) ->
                    let values = List.take 4 (later @ [ 1uy; 2uy; 3uy; 4uy ])
                    // Every case exercises empty, singleton and longer suffixes, including shrinking.
                    [ 0..4 ]
                    |> List.forall (fun length ->
                        scenario env first (List.take length values) missed
                        |> fun pending -> pending.GetAwaiter().GetResult())
            ))
