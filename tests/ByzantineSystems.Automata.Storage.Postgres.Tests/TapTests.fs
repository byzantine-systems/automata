module ByzantineSystems.Automata.Storage.Postgres.Tests.TapTests

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Postgres
open ByzantineSystems.Automata.Storage.Postgres.Tests.TestContext
open Expecto

/// The TAP entry contract, member by member, over the real PostgreSQL stores. The
/// pre-cancellation cases never reach the database — that is the contract — so they need no
/// fixture state; only the durable-effect case reads anything back.
let private entity = entityId "tap-1"

let private one = CommandId.ofInt64 1L

let private commandToken: LeaseToken<CommandWork> = LeaseToken.ofInt64 1L

let private actionToken: LeaseToken<ActionWork> = LeaseToken.ofInt64 1L

let private batch = ClaimBatch.create 4

let private lease = LeaseDuration.create (TimeSpan.FromSeconds 30.)

let private backoff = Backoff.defaults

let private at = TestContext.startTime

let private actionItem: LeasedAction<Entity, TestAction> =
    { Work =
        { MachineId = TestContext.machine
          EntityId = entity
          CommandId = one
          Epoch = Epoch.initial
          Ordinal = 1
          Action = Notify "tap" }
      Token = actionToken
      DeliveryCount = 1
      ExpiresAt = at }

let private commit: CorrectionCommit<Entity, TestState, TestEvent, TestAction> =
    { Transition = TestContext.draft entity Idle (Active 1) at
      ValidFrom = at
      Beliefs =
        { Asserted =
            { State = Active 1
              Epoch = Epoch.ofUInt64 1UL
              Status = InstanceStatus.Running }
          ValidFrom = at
          CommandId = one
          ChartVersion = TestContext.version },
        [] }

let private identity: ChartIdentity =
    { MachineId = TestContext.machine
      Version = TestContext.version
      Fingerprint = ChartFingerprint.create TestContext.fingerprint }

let private audit: SupervisionRecord =
    { Supervisor = SupervisorName.create "tap"
      ChildId = SupervisedChildId.create "child"
      Kind = Started
      Strategy = OneForOne
      Reason = "tap"
      At = at }

/// Every public member of every capability this provider offers, as one labeled entry.
let private operations () : (string * (CancellationToken -> Task)) list =
    let encode, decode = EntityKey.forEntityId<TestEntity>
    let inbox = TestContext.newInbox ()
    let reader = TestContext.newReader ()
    let processor = TestContext.newProcessor ()
    let queue = TestContext.newActionQueue ()
    let temporal = TestContext.newTemporalReader ()
    let corrections = TestContext.newCorrections ()
    let replay = TestContext.newReplayStore ()
    let registry = TestContext.newRegistry ()
    let machine = TestContext.newMachineStore RetentionPolicy.keepEverything

    let concreteQueue =
        PostgresActionQueue<Entity, TestAction>(
            { Context = TestContext.context ()
              Queue = TestContext.actionQueue
              ActionCodec = Serialization.systemTextJson<TestAction> ()
              EntityIdEncode = encode
              EntityIdDecode = decode }
        )

    let directNotifications =
        PostgresWorkNotifications(TestContext.context (), TestContext.actionQueue, ListenerConnection.Off)
        :> IWorkNotifications

    let maintenance = TestContext.newMaintenance ()

    let supervision =
        PostgresSupervisionStore(TestContext.context ()) :> ISupervisionEventStore

    [ "inbox.submit", (fun ct -> inbox.SubmitAsync(TestContext.submission entity "tap-key" (Start 1), ct) :> Task)
      "inbox.claim", (fun ct -> inbox.ClaimAsync(TestContext.machine, batch, lease, ct) :> Task)
      "inbox.reschedule", (fun ct -> inbox.RescheduleAsync(one, commandToken, backoff, ct) :> Task)
      "inbox.extendLease", (fun ct -> inbox.ExtendLeaseAsync(one, commandToken, lease, ct) :> Task)
      "inbox.tryGet", (fun ct -> inbox.TryGetAsync(one, ct) :> Task)
      "inbox.tryFind", (fun ct -> inbox.TryFindAsync(TestContext.machine, entity, "tap-key", ct) :> Task)
      "reader.tryGetSnapshot", (fun ct -> reader.TryGetSnapshotAsync(TestContext.machine, entity, ct) :> Task)
      "reader.history", (fun ct -> reader.HistoryAsync(TestContext.machine, entity, Page.create 10, ct) :> Task)
      "processor.commit",
      (fun ct ->
          processor.CommitAsync(one, commandToken, Epoch.initial, TestContext.draft entity Idle (Active 1) at, ct)
          :> Task)
      "processor.reject", (fun ct -> processor.RejectAsync(one, commandToken, CommandFailure.Machine "tap", ct) :> Task)
      "processor.deadLetter",
      (fun ct -> processor.DeadLetterAsync(one, commandToken, CommandFailure.Machine "tap", ct) :> Task)
      "processor.tryGetResult", (fun ct -> processor.TryGetResultAsync(one, ct) :> Task)
      "queue.claim", (fun ct -> queue.ClaimAsync(TestContext.machine, batch, lease, ct) :> Task)
      "queue.complete", (fun ct -> queue.CompleteAsync(actionItem, ct) :> Task)
      "queue.reschedule", (fun ct -> queue.RescheduleAsync(actionItem, backoff, ct) :> Task)
      "queue.abandon", (fun ct -> queue.AbandonAsync(actionItem, Reason.create "tap", ct) :> Task)
      "queue.ensure", (fun ct -> concreteQueue.EnsureQueueAsync ct :> Task)
      "temporal.validAt", (fun ct -> temporal.ValidAtAsync(TestContext.machine, entity, at, ct) :> Task)
      "temporal.asOf", (fun ct -> temporal.AsOfAsync(TestContext.machine, entity, at, at, ct) :> Task)
      "corrections.correct", (fun ct -> corrections.CorrectAsync(TestContext.machine, entity, at, [], ct) :> Task)
      "replay.suffix", (fun ct -> replay.SuffixAsync(TestContext.machine, entity, at, 10, ct) :> Task)
      "replay.commitCorrection",
      (fun ct -> replay.CommitCorrectionAsync(one, commandToken, Epoch.initial, commit, ct) :> Task)
      "registry.register", (fun ct -> registry.RegisterAsync(identity, ct) :> Task)
      "registry.tryGet", (fun ct -> registry.TryGetAsync(TestContext.machine, TestContext.version, ct) :> Task)
      "boot.boot", (fun ct -> (machine :> IStoreBoot).BootAsync(TestContext.machine, ct) :> Task)
      "machine.ensureQueue", (fun ct -> machine.EnsureQueueAsync ct :> Task)
      "machine.notifications.listen",
      (fun ct ->
          (machine :> IWorkNotifications).ListenAsync(TestContext.machine, (fun () -> ()), (fun () -> ()), ct) :> Task)
      "notifications.listen",
      (fun ct -> directNotifications.ListenAsync(TestContext.machine, (fun () -> ()), (fun () -> ()), ct) :> Task)
      "maintenance.notifyPending", (fun ct -> maintenance.NotifyPendingAsync ct :> Task)
      "maintenance.run", (fun ct -> maintenance.RunAsync(batch, ct) :> Task)
      "maintenance.detectDrift", (fun ct -> maintenance.DetectDriftAsync ct :> Task)
      "maintenance.repairDrift", (fun ct -> maintenance.RepairDriftAsync ct :> Task)
      "maintenance.schedule",
      (fun ct -> maintenance.ScheduleAsync(TimeSpan.FromSeconds 1., TimeSpan.FromSeconds 2., batch, ct) :> Task)
      "maintenance.unschedule", (fun ct -> maintenance.UnscheduleAsync ct :> Task)
      "supervision.record", (fun ct -> supervision.RecordAsync(audit, ct) :> Task) ]

let private poisonInbox () : Inbox =
    let _, decode = EntityKey.forEntityId<TestEntity>

    PostgresCommandInbox<Entity, TestEvent>(
        { Context = TestContext.context ()
          EventCodec = Serialization.systemTextJson<TestEvent> ()
          EntityIdEncode =
            fun _ -> raise (ArgumentNullException("encoder", "a pre-canceled operation encoded its entity id"))
          EntityIdDecode = decode }
    )
    :> Inbox

let tests =
    testList
        "TAP entry semantics"
        [ for label, invoke in operations () do
              testCase $"%s{label} answers a canceled task on entry" (fun () ->
                  StorageTapContracts.canceledWithoutWork label invoke)

          testCase "pre-cancellation skips entity encoding" (fun () ->
              let inbox = poisonInbox ()

              StorageTapContracts.canceledWithoutWork "inbox.tryFind poison encoder" (fun ct ->
                  inbox.TryFindAsync(TestContext.machine, entity, "tap-key", ct) :> Task))

          testCase "an entity encoder ArgumentNullException faults the task" (fun () ->
              let inbox = poisonInbox ()

              StorageTapContracts.defectFaultsTask<ArgumentNullException> "inbox.tryFind poison encoder" (fun () ->
                  inbox.TryFindAsync(TestContext.machine, entity, "tap-key", CancellationToken.None) :> Task))

          testCase "a null submission is refused at the call site" (fun () ->
              let inbox = TestContext.newInbox ()

              StorageTapContracts.nullThrowsAtCallSite "inbox.submit" (fun () ->
                  inbox.SubmitAsync(Unchecked.defaultof<CommandSubmission<Entity, TestEvent>>, CancellationToken.None)
                  |> ignore))

          testCase "a null entity id is refused at the call site" (fun () ->
              let inbox = TestContext.newInbox ()

              StorageTapContracts.nullThrowsAtCallSite "inbox.tryFind entity" (fun () ->
                  inbox.TryFindAsync(TestContext.machine, Unchecked.defaultof<Entity>, "key", CancellationToken.None)
                  |> ignore))

          testCase "a null idempotency key is refused at the call site" (fun () ->
              let inbox = TestContext.newInbox ()

              StorageTapContracts.nullThrowsAtCallSite "inbox.tryFind key" (fun () ->
                  inbox.TryFindAsync(TestContext.machine, entity, null, CancellationToken.None)
                  |> ignore))

          testCase "a null reader entity id is refused at the call site" (fun () ->
              let reader = TestContext.newReader ()

              StorageTapContracts.nullThrowsAtCallSite "reader.tryGetSnapshot" (fun () ->
                  reader.TryGetSnapshotAsync(TestContext.machine, Unchecked.defaultof<Entity>, CancellationToken.None)
                  |> ignore))

          testCase "a null draft is refused at the call site" (fun () ->
              let processor = TestContext.newProcessor ()

              StorageTapContracts.nullThrowsAtCallSite "processor.commit" (fun () ->
                  processor.CommitAsync(
                      one,
                      commandToken,
                      Epoch.initial,
                      Unchecked.defaultof<TransitionDraft<Entity, TestState, TestEvent, TestAction>>,
                      CancellationToken.None
                  )
                  |> ignore))

          testCase "a null leased action is refused at the call site" (fun () ->
              let queue = TestContext.newActionQueue ()

              StorageTapContracts.nullThrowsAtCallSite "queue.complete" (fun () ->
                  queue.CompleteAsync(Unchecked.defaultof<LeasedAction<Entity, TestAction>>, CancellationToken.None)
                  |> ignore))

          testCase "a null temporal entity id is refused at the call site" (fun () ->
              let temporal = TestContext.newTemporalReader ()

              StorageTapContracts.nullThrowsAtCallSite "temporal.validAt" (fun () ->
                  temporal.ValidAtAsync(TestContext.machine, Unchecked.defaultof<Entity>, at, CancellationToken.None)
                  |> ignore))

          testCase "a null belief list is refused at the call site" (fun () ->
              let corrections = TestContext.newCorrections ()

              StorageTapContracts.nullThrowsAtCallSite "corrections.correct" (fun () ->
                  corrections.CorrectAsync(
                      TestContext.machine,
                      entity,
                      at,
                      Unchecked.defaultof<CorrectedBelief<TestState> list>,
                      CancellationToken.None
                  )
                  |> ignore))

          testCase "a null correction entity id is refused at the call site" (fun () ->
              let corrections = TestContext.newCorrections ()

              StorageTapContracts.nullThrowsAtCallSite "corrections.correct entity" (fun () ->
                  corrections.CorrectAsync(
                      TestContext.machine,
                      Unchecked.defaultof<Entity>,
                      at,
                      [],
                      CancellationToken.None
                  )
                  |> ignore))

          testCase "a null replay entity id is refused at the call site" (fun () ->
              let replay = TestContext.newReplayStore ()

              StorageTapContracts.nullThrowsAtCallSite "replay.suffix" (fun () ->
                  replay.SuffixAsync(TestContext.machine, Unchecked.defaultof<Entity>, at, 10, CancellationToken.None)
                  |> ignore))

          testCase "a null chart identity is refused at the call site" (fun () ->
              let registry = TestContext.newRegistry ()

              StorageTapContracts.nullThrowsAtCallSite "registry.register" (fun () ->
                  registry.RegisterAsync(Unchecked.defaultof<ChartIdentity>, CancellationToken.None)
                  |> ignore))

          testCase "a null machine id is refused at the call site" (fun () ->
              let store = TestContext.newMachineStore RetentionPolicy.keepEverything :> IStoreBoot

              StorageTapContracts.nullThrowsAtCallSite "boot.boot" (fun () ->
                  store.BootAsync(Unchecked.defaultof<MachineId>, CancellationToken.None)
                  |> ignore))

          testCase "a null notification callback is refused at the call site" (fun () ->
              let notifications =
                  TestContext.newMachineStore RetentionPolicy.keepEverything :> IWorkNotifications

              StorageTapContracts.nullThrowsAtCallSite "notifications.listen" (fun () ->
                  notifications.ListenAsync(
                      TestContext.machine,
                      Unchecked.defaultof<unit -> unit>,
                      (fun () -> ()),
                      CancellationToken.None
                  )
                  |> ignore))

          testCase "a null supervision record is refused at the call site" (fun () ->
              let supervision =
                  PostgresSupervisionStore(TestContext.context ()) :> ISupervisionEventStore

              StorageTapContracts.nullThrowsAtCallSite "supervision.record" (fun () ->
                  supervision.RecordAsync(Unchecked.defaultof<SupervisionRecord>, CancellationToken.None)
                  |> ignore))

          testTask "a pre-canceled submit records nothing" {
              do! TestContext.reset ()

              let inbox = TestContext.newInbox ()

              StorageTapContracts.canceledWithoutWork "inbox.submit" (fun ct ->
                  inbox.SubmitAsync(TestContext.submission entity "tap-never" (Start 1), ct) :> Task)

              let! outcome = inbox.TryFindAsync(TestContext.machine, entity, "tap-never", CancellationToken.None)

              Expect.equal outcome (Ok None) "nothing was recorded, because nothing was attempted"
          } ]
