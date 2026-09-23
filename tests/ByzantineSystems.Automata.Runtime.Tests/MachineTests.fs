module ByzantineSystems.Automata.Runtime.Tests.MachineTests

open System
open System.Threading
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Runtime
open ByzantineSystems.Automata.Runtime.Tests.TestSupport
open Expecto

let private started (stub: StubStore<TestError>) =
    task {
        let built = buildMachine stub (newTime ()) id |> expectMachine
        let! _ = Machine.startAsync built (StubRegistry()) CancellationToken.None
        return built
    }

let configurationTests =
    testList
        "configuration"
        [ test "a machine without a chart version does not build" {
              let stub = StubStore<TestError>()

              let result =
                  machine<EntityId<TestEntity>, TestState, TestEvent, TestAction, TestError> testMachine {
                      chart testChart
                      initialState Idle
                      store (stub :> IMachineStore<_, _, _, _, _>)
                      actionQueue "test_actions"
                  }

              match result with
              | Error errors -> Expect.contains errors MissingChartVersion "the version has to be declared"
              | Ok _ -> failtest "a machine without a declared chart version should not build"
          }

          test "a machine without an action queue does not build" {
              let stub = StubStore<TestError>()

              let result =
                  machine<EntityId<TestEntity>, TestState, TestEvent, TestAction, TestError> testMachine {
                      chart testChart
                      chartVersion 1
                      initialState Idle
                      store (stub :> IMachineStore<_, _, _, _, _>)
                  }

              match result with
              | Error errors -> Expect.contains errors MissingActionQueue "the queue has to be named"
              | Ok _ -> failtest "a machine without an action queue should not build"
          }

          test "a queue name that would need quoting is refused" {
              // The name reaches dynamic SQL as an identifier rather than a parameter, so the
              // allowlist is narrower than PostgreSQL's own rules on purpose.
              let stub = StubStore<TestError>()

              let result =
                  machine<EntityId<TestEntity>, TestState, TestEvent, TestAction, TestError> testMachine {
                      chart testChart
                      chartVersion 1
                      initialState Idle
                      store (stub :> IMachineStore<_, _, _, _, _>)
                      actionQueue "Test Actions; DROP TABLE"
                  }

              match result with
              | Error errors ->
                  Expect.contains errors (InvalidActionQueueName "Test Actions; DROP TABLE") "refused by the allowlist"
              | Ok _ -> failtest "an unacceptable queue name should not build"
          }

          test "every defect is reported, not just the first" {
              let result =
                  machine<EntityId<TestEntity>, TestState, TestEvent, TestAction, TestError> testMachine {
                      chartVersion 1
                      initialState Idle
                  }

              match result with
              | Error errors ->
                  Expect.contains errors MissingChart "the chart"
                  Expect.contains errors MissingStore "the store"
                  Expect.contains errors MissingActionQueue "the queue"
              | Ok _ -> failtest "an empty machine should not build"
          }

          test "a declaration made twice is a defect" {
              let stub = StubStore<TestError>()

              let result =
                  machine<EntityId<TestEntity>, TestState, TestEvent, TestAction, TestError> testMachine {
                      chart testChart
                      chart testChart
                      chartVersion 1
                      initialState Idle
                      store (stub :> IMachineStore<_, _, _, _, _>)
                      actionQueue "test_actions"
                  }

              match result with
              | Error errors -> Expect.contains errors (DuplicateDeclaration MachineDeclaration.Chart) "declared twice"
              | Ok _ -> failtest "a duplicate declaration should not build"
          }

          test "an invalid processor policy is reported with its own defects" {
              let stub = StubStore<TestError>()

              let result =
                  machine<EntityId<TestEntity>, TestState, TestEvent, TestAction, TestError> testMachine {
                      chart testChart
                      chartVersion 1
                      initialState Idle
                      store (stub :> IMachineStore<_, _, _, _, _>)
                      actionQueue "test_actions"

                      processor
                          { ProcessorPolicy.defaults with
                              RenewAfter = TimeSpan.FromSeconds 60.
                              Lease = TimeSpan.FromSeconds 30. }
                  }

              match result with
              | Error [ InvalidProcessorPolicy defects ] ->
                  // Renewing after the lease has already lapsed protects nothing.
                  Expect.contains
                      defects
                      (RenewAfterNotBelowLease(TimeSpan.FromSeconds 60., TimeSpan.FromSeconds 30.))
                      "the renewal has to happen inside the lease"
              | other -> failtestf "expected one policy defect, got %A" other
          } ]

let lifecycleTests =
    testList
        "lifecycle"
        [ testTask "a machine that has not started refuses work" {
              let stub = StubStore<TestError>()
              let built = buildMachine stub (newTime ()) id |> expectMachine

              match!
                  Machine.enqueue built (entityId "E-1") (EventEnvelope.create "k1" (Start 1)) CancellationToken.None
              with
              | Error(MachineError.Rejected MachineRejection.NotStarted) -> ()
              | other -> failtestf "expected a not-started rejection, got %A" other
          }

          testTask "a stopped machine refuses work" {
              let stub = StubStore<TestError>()
              let! built = started stub
              do! Machine.stopAsync built CancellationToken.None

              match!
                  Machine.enqueue built (entityId "E-1") (EventEnvelope.create "k1" (Start 1)) CancellationToken.None
              with
              | Error(MachineError.Rejected _) -> ()
              | other -> failtestf "expected a lifecycle rejection, got %A" other
          }

          testTask "starting twice is harmless" {
              let stub = StubStore<TestError>()
              let! built = started stub
              let! _ = Machine.startAsync built (StubRegistry()) CancellationToken.None
              ()
          }

          testTask "a fingerprint mismatch is reported rather than raised" {
              // Whether a mismatch is fatal is the host's decision, so the store reports it and
              // the host decides. Making it fatal at boot belongs with the maintenance work.
              let stub = StubStore<TestError>()
              let built = buildMachine stub (newTime ()) id |> expectMachine
              let stored = Chart.fingerprint testChart

              match!
                  Machine.startAsync built (StubRegistry(ChartRegistration.Mismatched stored)) CancellationToken.None
              with
              | Ok(ChartRegistration.Mismatched _) -> ()
              | other -> failtestf "expected a reported mismatch, got %A" other
          } ]

let submissionTests =
    testList
        "submission"
        [ testTask "enqueue pins the machine's declared chart version" {
              let stub = StubStore<TestError>()
              let! built = started stub
              Expect.equal (ChartVersion.create 1) (Machine.chartVersion built) "the declared version"
          }

          testTask "enqueue returns the submission outcome" {
              let stub = StubStore<TestError>()
              stub.SubmitOutcome <- Ok(Accepted(CommandId.ofInt64 42L))
              let! built = started stub

              match!
                  Machine.enqueue built (entityId "E-1") (EventEnvelope.create "k1" (Start 1)) CancellationToken.None
              with
              | Ok(Accepted commandId) -> Expect.equal (CommandId.ofInt64 42L) commandId "the id the inbox assigned"
              | other -> failtestf "expected an acceptance, got %A" other
          }

          testTask "a repeated idempotency key is success, carrying the original id" {
              let stub = StubStore<TestError>()
              stub.SubmitOutcome <- Ok(AlreadySubmitted(CommandId.ofInt64 7L))
              let! built = started stub

              match!
                  Machine.enqueue built (entityId "E-1") (EventEnvelope.create "k1" (Start 1)) CancellationToken.None
              with
              | Ok(AlreadySubmitted commandId) -> Expect.equal (CommandId.ofInt64 7L) commandId "the original command"
              | other -> failtestf "expected an already-submitted outcome, got %A" other
          }

          testTask "commandResult reads what became of a command without waiting" {
              let stub = StubStore<TestError>()
              stub.ResultOutcome <- Ok(Some CommandResult.Pending)
              let! built = started stub

              match! Machine.commandResult built (CommandId.ofInt64 1L) CancellationToken.None with
              | Ok(Some CommandResult.Pending) -> ()
              | other -> failtestf "expected a pending result, got %A" other
          }

          testTask "send returns once the store reports a durable outcome" {
              let stub = StubStore<TestError>()
              stub.SubmitOutcome <- Ok(Accepted(CommandId.ofInt64 3L))
              stub.ResultOutcome <- Ok(Some(CommandResult.Rejected(CommandFailure.Machine "refused")))
              let! built = started stub

              match!
                  Machine.send built (entityId "E-1") (EventEnvelope.create "k1" (Start 1)) CancellationToken.None
              with
              | Ok(CommandResult.Rejected(CommandFailure.Machine reason)) ->
                  Expect.equal "refused" reason "the durable outcome, read back from the store"
              | other -> failtestf "expected a rejection, got %A" other
          }

          testTask "an envelope carries its audit context to the inbox" {
              let stub = StubStore<TestError>()
              let! built = started stub

              let envelope =
                  EventEnvelope.create "k1" (Start 1)
                  |> EventEnvelope.withCorrelation "trace-1"
                  |> EventEnvelope.withCausation "cause-1"

              Expect.equal (Some "trace-1") (EventEnvelope.correlationId envelope) "correlation survives"
              Expect.equal (Some "cause-1") (EventEnvelope.causationId envelope) "causation survives"

              match! Machine.enqueue built (entityId "E-1") envelope CancellationToken.None with
              | Ok _ -> ()
              | other -> failtestf "expected the submission to succeed, got %A" other
          } ]

let tests =
    testList "machine" [ configurationTests; lifecycleTests; submissionTests ]
