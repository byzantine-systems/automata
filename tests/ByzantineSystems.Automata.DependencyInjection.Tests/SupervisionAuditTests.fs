module ByzantineSystems.Automata.DependencyInjection.Tests.SupervisionAuditTests

open System
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Resilience
open ByzantineSystems.Automata.Runtime
open ByzantineSystems.Automata.Storage

open ByzantineSystems.Automata.DependencyInjection
open Expecto
open Microsoft.Extensions.DependencyInjection
open TestSupport

let private auditKinds (audit: InMemorySupervisionStore) = audit.Recorded() |> List.map _.Kind

let supervisionAuditTests =
    testList
        "supervision audit"
        [ testTask "an orderly stopped generation records Started then Stopped audits in order" {
              let store = TestStore()
              let audit = InMemorySupervisionStore()

              let machineReady =
                  TaskCompletionSource<TestMachine>(TaskCreationOptions.RunContinuationsAsynchronously)

              // Temporary: a generation that ends is stopped, never restarted, so the
              // supervisor settles and the host can flush the final audit records.
              let options =
                  { testOptions "audit-machine" store (fun m -> machineReady.TrySetResult m |> ignore) with
                      Supervisor =
                          { supervisorDefaults with
                              Restart = RestartKind.Temporary } }

              let provider =
                  ServiceCollection()
                      .AddSingleton<ISupervisionEventStore>(audit)
                      .AddAutomata(options)
                      .BuildServiceProvider()

              let hosted, background = resolveHostedService provider
              do! hosted.StartAsync(noCancellation)

              do! pollUntil "the Started audit record" (fun () -> auditKinds audit = [ SupervisionAuditKind.Started ])

              let! machine = machineReady.Task.WaitAsync(waitTimeout)

              // End the generation normally; the supervisor records Stopped and settles.
              do! Machine.stopAsync machine noCancellation

              do!
                  pollUntil "the Stopped audit record" (fun () ->
                      auditKinds audit = [ SupervisionAuditKind.Started; SupervisionAuditKind.Stopped ])

              let records = audit.Recorded()

              Expect.equal
                  (records |> List.map _.Kind)
                  [ SupervisionAuditKind.Started; SupervisionAuditKind.Stopped ]
                  "audit records are ordered Started then Stopped"

              Expect.isTrue
                  (records |> List.forall (fun r -> r.Supervisor = supervisorDefaults.Name))
                  "every record is attributed to the configured supervisor"

              Expect.isTrue
                  (records
                   |> List.forall (fun r -> r.ChildId = SupervisedChildId.create "audit-machine"))
                  "every record is attributed to the machine key"

              Expect.equal records[1].Reason "normal exit, not restarted" "the generation ended normally"

              // The host stops cleanly afterwards: no fault, no hang.
              do! hosted.StopAsync(noCancellation)
              Expect.isFalse background.ExecuteTask.IsFaulted "the hosted service did not fault"

              Expect.equal
                  background.ExecuteTask.Status
                  TaskStatus.RanToCompletion
                  "the execute task completed normally once the supervisor settled"
          }

          testTask "an audit persistence failure faults the hosted service ExecuteTask" {
              let store = TestStore()

              let storeError =
                  StoreError.Unavailable(InvalidOperationException "audit store down")

              let options = testOptions "audit-failure-machine" store ignore

              let provider =
                  ServiceCollection()
                      .AddSingleton<ISupervisionEventStore>(FailingAuditStore storeError)
                      .AddAutomata(options)
                      .BuildServiceProvider()

              let hosted, background = resolveHostedService provider

              // Start succeeds: the first audit write happens after the supervisor started.
              do! hosted.StartAsync(noCancellation)

              do! pollUntil "ExecuteTask to fault" (fun () -> background.ExecuteTask.IsCompleted)

              Expect.isTrue background.ExecuteTask.IsFaulted "the audit failure faulted the execute task"

              let fault = background.ExecuteTask.Exception.GetBaseException()

              match fault with
              | :? AutomataAuditException as auditError ->
                  Expect.equal auditError.Data0 storeError "the original store error is preserved"
              | other -> failtestf "expected AutomataAuditException, got %A" other

              // Cleanup: stopping a faulted service still completes.
              do! hosted.StopAsync(noCancellation)
          } ]
