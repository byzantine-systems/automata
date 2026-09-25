module ByzantineSystems.Automata.DependencyInjection.Tests.MaintenanceServiceTests

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.DependencyInjection
open Expecto
open Microsoft.Extensions.DependencyInjection
open TestSupport

/// <summary>
/// Maintenance that answers what the test scripted and counts what it was asked. Whether the
/// routines do what they say is the integration suite's business; this is about which of them
/// the service calls, and when.
/// </summary>
type private StubMaintenance(scheduling: Scheduling) =
    let mutable notifies = 0
    let mutable runs = 0
    let mutable detections = 0
    let mutable repairs = 0
    let mutable schedules = 0

    member val NotifyOutcome: Result<NotifyReport, StoreError> = Ok { Sent = 0; QueueUsage = 0.0 } with get, set
    member val Drift: BlockedDrift list = [] with get, set
    member val RunFault: exn option = None with get, set

    member _.Notifies = Volatile.Read &notifies
    member _.Runs = Volatile.Read &runs
    member _.Detections = Volatile.Read &detections
    member _.Repairs = Volatile.Read &repairs
    member _.Schedules = Volatile.Read &schedules

    interface IDatabaseMaintenance with
        member this.NotifyPending(_) =
            Interlocked.Increment &notifies |> ignore
            Task.FromResult this.NotifyOutcome

        member this.Run(_, _) =
            Interlocked.Increment &runs |> ignore

            match this.RunFault with
            | Some fault -> Task.FromException<Result<MaintenanceReport list, StoreError>> fault
            | None -> Task.FromResult(Ok [])

        member this.DetectDrift(_) =
            Interlocked.Increment &detections |> ignore
            Task.FromResult(Ok this.Drift)

        member _.RepairDrift(_) =
            Interlocked.Increment &repairs |> ignore
            Task.FromResult(Ok [])

        member _.Schedule(_, _, _, _) =
            Interlocked.Increment &schedules |> ignore
            Task.FromResult(Ok scheduling)

        member _.Unschedule(_) = Task.FromResult(Ok 0)

let private fast (stub: StubMaintenance) (scheduler: MaintenanceScheduler) : MaintenanceOptions =
    { MaintenanceOptions.defaults (fun _ -> stub :> IDatabaseMaintenance) with
        Scheduler = scheduler
        NotifyEvery = TimeSpan.FromMilliseconds 10.
        RunEvery = TimeSpan.FromMilliseconds 10. }

/// Starts the service, lets the test watch it, and stops it again.
let private running (options: MaintenanceOptions) (watch: Microsoft.Extensions.Hosting.BackgroundService -> Task) =
    task {
        let provider =
            ServiceCollection().AddAutomataMaintenance(options).BuildServiceProvider()

        let hosted, service = resolveHostedService provider
        do! hosted.StartAsync noCancellation
        do! watch service
        do! hosted.StopAsync noCancellation
    }

let private drifted =
    { CommandId = CommandId.ofInt64 1L
      MachineId = machineId "di-tests"
      EntityId = "E-1"
      Sequence = 2L
      Blocked = true
      Expected = false }

let tests =
    testList
        "maintenance service"
        [ testTask "in process, it announces and runs passes on their own clocks" {
              let stub = StubMaintenance(Scheduling.Unavailable)

              do!
                  running (fast stub MaintenanceScheduler.InProcess) (fun _ ->
                      task {
                          do! pollUntil "several notification ticks" (fun () -> stub.Notifies >= 3)
                          do! pollUntil "several passes" (fun () -> stub.Runs >= 3)
                      })

              Expect.equal stub.Schedules 0 "nothing asked the database to schedule anything"
          }

          testTask "without pg_cron, a request to schedule in the database falls back to in process" {
              // Maintenance is not optional; only who runs it is.
              let stub = StubMaintenance(Scheduling.Unavailable)

              do!
                  running (fast stub MaintenanceScheduler.InDatabase) (fun _ ->
                      pollUntil "passes run here instead" (fun () -> stub.Runs >= 2))

              Expect.equal stub.Schedules 1 "it asked once"
          }

          testTask "when the database runs maintenance, this only watches for drift" {
              // pg_cron's output lands in the server log, which is not where an application looks.
              let stub = StubMaintenance(Scheduling.Scheduled)

              do!
                  running (fast stub MaintenanceScheduler.InDatabase) (fun _ ->
                      pollUntil "drift is still checked here" (fun () -> stub.Detections >= 2))

              Expect.equal stub.Runs 0 "the database runs the passes"
              Expect.equal stub.Notifies 0 "and the announcements"
          }

          testTask "a failing tick is logged and the next one still runs" {
              let stub = StubMaintenance(Scheduling.Unavailable)
              stub.NotifyOutcome <- Error(StoreError.Unavailable(InvalidOperationException "scripted"))

              do!
                  running (fast stub MaintenanceScheduler.InProcess) (fun service ->
                      task {
                          do! pollUntil "ticks keep coming" (fun () -> stub.Notifies >= 3)
                          Expect.isFalse service.ExecuteTask.IsFaulted "a missed tick delays, it does not fault"
                      })
          }

          testTask "drift is reported and not repaired unless asked" {
              let stub = StubMaintenance(Scheduling.Unavailable)
              stub.Drift <- [ drifted ]

              do!
                  running (fast stub MaintenanceScheduler.InProcess) (fun _ ->
                      pollUntil "drift was checked" (fun () -> stub.Detections >= 2))

              Expect.equal stub.Repairs 0 "repair hides the bug that caused the drift"
          }

          testTask "drift is repaired when the host opts in" {
              let stub = StubMaintenance(Scheduling.Unavailable)
              stub.Drift <- [ drifted ]

              let options =
                  { fast stub MaintenanceScheduler.InProcess with
                      RepairDrift = true }

              do! running options (fun _ -> pollUntil "a repair ran" (fun () -> stub.Repairs >= 1))
          }

          testTask "a loop that faults stops the other and faults the service" {
              // The failure Task.WhenAll alone would hide: one loop dead while the other runs on.
              let stub = StubMaintenance(Scheduling.Unavailable)
              stub.RunFault <- Some(InvalidOperationException "not understood")

              do!
                  running (fast stub MaintenanceScheduler.InProcess) (fun service ->
                      task {
                          do! pollUntil "the service faulted" (fun () -> service.ExecuteTask.IsFaulted)
                          let notifiesAfterFault = stub.Notifies
                          do! Task.Delay 100
                          Expect.equal stub.Notifies notifiesAfterFault "the notification loop stopped with it"

                          Expect.isTrue
                              (service.ExecuteTask.Exception.InnerExceptions
                               |> Seq.exists (fun error -> error.Message = "not understood"))
                              "and the fault is the one that happened"
                      })
          }

          test "an interval that is not positive is refused at registration" {
              let stub = StubMaintenance(Scheduling.Unavailable)

              let options =
                  { fast stub MaintenanceScheduler.InProcess with
                      NotifyEvery = TimeSpan.Zero }

              Expect.throwsT<ArgumentException>
                  (fun () -> ServiceCollection().AddAutomataMaintenance(options) |> ignore)
                  "a zero interval is a loop with no pause"
          } ]
