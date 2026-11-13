module ByzantineSystems.Automata.Storage.Sqlite.Tests.TapTests

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Internal
open ByzantineSystems.Automata.Storage.Sqlite
open ByzantineSystems.Automata.Storage.Sqlite.Tests.TestContext
open Expecto

/// The TAP entry contract over the real SQLite stores. The pre-cancellation cases never open
/// the database — that is the contract — so one shared fixture serves them all.
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
        { MachineId = TestContext.testMachine
          EntityId = entity
          CommandId = one
          Epoch = Epoch.initial
          Ordinal = 1
          Action = Notify "tap" }
      Token = actionToken
      DeliveryCount = 1
      ExpiresAt = at }

let private identity: ChartIdentity =
    { MachineId = TestContext.testMachine
      Version = TestContext.version
      Fingerprint = ChartFingerprint.create TestContext.fingerprint }

/// Every public member of every capability this provider offers, as one labeled entry.
let private operations (fixture: TestContext.Fixture) : (string * (CancellationToken -> Task)) list =
    let inbox = TestContext.inboxOf fixture
    let reader = TestContext.readerOf fixture
    let processor = TestContext.processorOf fixture
    let queue = TestContext.actionsOf fixture
    let registry = TestContext.registryOf fixture
    let store = TestContext.storeOf fixture.Context

    [ "inbox.submit", (fun ct -> inbox.SubmitAsync(TestContext.submission entity "tap-key" (Start 1), ct) :> Task)
      "inbox.claim", (fun ct -> inbox.ClaimAsync(TestContext.testMachine, batch, lease, ct) :> Task)
      "inbox.reschedule", (fun ct -> inbox.RescheduleAsync(one, commandToken, backoff, ct) :> Task)
      "inbox.extendLease", (fun ct -> inbox.ExtendLeaseAsync(one, commandToken, lease, ct) :> Task)
      "inbox.tryGet", (fun ct -> inbox.TryGetAsync(one, ct) :> Task)
      "inbox.tryFind", (fun ct -> inbox.TryFindAsync(TestContext.testMachine, entity, "tap-key", ct) :> Task)
      "reader.tryGetSnapshot", (fun ct -> reader.TryGetSnapshotAsync(TestContext.testMachine, entity, ct) :> Task)
      "reader.history", (fun ct -> reader.HistoryAsync(TestContext.testMachine, entity, Page.create 10, ct) :> Task)
      "processor.commit",
      (fun ct ->
          processor.CommitAsync(one, commandToken, Epoch.initial, TestContext.draft entity Idle (Active 1) at, ct)
          :> Task)
      "processor.reject", (fun ct -> processor.RejectAsync(one, commandToken, CommandFailure.Machine "tap", ct) :> Task)
      "processor.deadLetter",
      (fun ct -> processor.DeadLetterAsync(one, commandToken, CommandFailure.Machine "tap", ct) :> Task)
      "processor.tryGetResult", (fun ct -> processor.TryGetResultAsync(one, ct) :> Task)
      "queue.claim", (fun ct -> queue.ClaimAsync(TestContext.testMachine, batch, lease, ct) :> Task)
      "queue.complete", (fun ct -> queue.CompleteAsync(actionItem, ct) :> Task)
      "queue.reschedule", (fun ct -> queue.RescheduleAsync(actionItem, backoff, ct) :> Task)
      "queue.abandon", (fun ct -> queue.AbandonAsync(actionItem, Reason.create "tap", ct) :> Task)
      "registry.register", (fun ct -> registry.RegisterAsync(identity, ct) :> Task)
      "registry.tryGet", (fun ct -> registry.TryGetAsync(TestContext.testMachine, TestContext.version, ct) :> Task)
      "boot.boot", (fun ct -> (store :> IStoreBoot).BootAsync(TestContext.testMachine, ct) :> Task) ]

let private poisonInbox (fixture: TestContext.Fixture) : Inbox =
    let _, decode = EntityKey.forEntityId<TestEntity>

    SqliteCommandInbox<Entity, TestEvent>(
        { Context = fixture.Context
          EventCodec = Serialization.systemTextJson<TestEvent> ()
          EntityIdEncode =
            fun _ -> raise (ArgumentNullException("encoder", "a pre-canceled operation encoded its entity id"))
          EntityIdDecode = decode }
    )
    :> Inbox

let tests =
    testList
        "TAP entry semantics"
        [ let fixture = TestContext.fixture "tap"

          for label, invoke in operations fixture do
              testCase $"%s{label} answers a canceled task on entry" (fun () ->
                  StorageTapContracts.canceledWithoutWork label invoke)

          testCase "pre-cancellation skips entity encoding" (fun () ->
              let inbox = poisonInbox fixture

              StorageTapContracts.canceledWithoutWork "inbox.tryFind poison encoder" (fun ct ->
                  inbox.TryFindAsync(TestContext.testMachine, entity, "tap-key", ct) :> Task))

          testCase "an entity encoder ArgumentNullException faults the task" (fun () ->
              let inbox = poisonInbox fixture

              StorageTapContracts.defectFaultsTask<ArgumentNullException> "inbox.tryFind poison encoder" (fun () ->
                  inbox.TryFindAsync(TestContext.testMachine, entity, "tap-key", CancellationToken.None) :> Task))

          testCase "a null submission is refused at the call site" (fun () ->
              let inbox = TestContext.inboxOf fixture

              StorageTapContracts.nullThrowsAtCallSite "inbox.submit" (fun () ->
                  inbox.SubmitAsync(Unchecked.defaultof<CommandSubmission<Entity, TestEvent>>, CancellationToken.None)
                  |> ignore))

          testCase "a null entity id is refused at the call site" (fun () ->
              let inbox = TestContext.inboxOf fixture

              StorageTapContracts.nullThrowsAtCallSite "inbox.tryFind entity" (fun () ->
                  inbox.TryFindAsync(
                      TestContext.testMachine,
                      Unchecked.defaultof<Entity>,
                      "key",
                      CancellationToken.None
                  )
                  |> ignore))

          testCase "a null idempotency key is refused at the call site" (fun () ->
              let inbox = TestContext.inboxOf fixture

              StorageTapContracts.nullThrowsAtCallSite "inbox.tryFind key" (fun () ->
                  inbox.TryFindAsync(TestContext.testMachine, entity, null, CancellationToken.None)
                  |> ignore))

          testCase "a null reader entity id is refused at the call site" (fun () ->
              let reader = TestContext.readerOf fixture

              StorageTapContracts.nullThrowsAtCallSite "reader.tryGetSnapshot" (fun () ->
                  reader.TryGetSnapshotAsync(
                      TestContext.testMachine,
                      Unchecked.defaultof<Entity>,
                      CancellationToken.None
                  )
                  |> ignore))

          testCase "a null draft is refused at the call site" (fun () ->
              let processor = TestContext.processorOf fixture

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
              let queue = TestContext.actionsOf fixture

              StorageTapContracts.nullThrowsAtCallSite "queue.complete" (fun () ->
                  queue.CompleteAsync(Unchecked.defaultof<LeasedAction<Entity, TestAction>>, CancellationToken.None)
                  |> ignore))

          testCase "a null chart identity is refused at the call site" (fun () ->
              let registry = TestContext.registryOf fixture

              StorageTapContracts.nullThrowsAtCallSite "registry.register" (fun () ->
                  registry.RegisterAsync(Unchecked.defaultof<ChartIdentity>, CancellationToken.None)
                  |> ignore))

          testCase "a null machine id is refused at the call site" (fun () ->
              let store = TestContext.storeOf fixture.Context :> IStoreBoot

              StorageTapContracts.nullThrowsAtCallSite "boot.boot" (fun () ->
                  store.BootAsync(Unchecked.defaultof<MachineId>, CancellationToken.None)
                  |> ignore))

          testTask "a pre-canceled submit records nothing" {
              let inbox = TestContext.inboxOf fixture

              StorageTapContracts.canceledWithoutWork "inbox.submit" (fun ct ->
                  inbox.SubmitAsync(TestContext.submission entity "tap-never" (Start 1), ct) :> Task)

              let! outcome = inbox.TryFindAsync(TestContext.testMachine, entity, "tap-never", CancellationToken.None)

              Expect.equal outcome (Ok None) "nothing was recorded, because nothing was attempted"
          } ]
