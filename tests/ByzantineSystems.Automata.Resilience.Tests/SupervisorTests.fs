module ByzantineSystems.Automata.Resilience.Tests.SupervisorTests

open System
open System.Diagnostics
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Resilience
open Expecto

type FakeTime(start: DateTimeOffset) =
    inherit TimeProvider()
    let mutable current = start

    override _.GetUtcNow() : DateTimeOffset = current

    member _.Advance(span: TimeSpan) = current <- current.Add span

let testClock = FakeTime(DateTimeOffset(2026, 9, 19, 12, 0, 0, TimeSpan.Zero))
let ct = CancellationToken.None

/// A controllable child: it runs until stopped, crashed, or completed by the test.
type FakeChild(id: string, onStopped: string -> unit) =
    let completion =
        TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

    let mutable stopCount = 0

    interface ISupervisedChild with
        member _.Completion = completion.Task

        member _.StopAsync(_: CancellationToken) =
            task {
                stopCount <- stopCount + 1
                onStopped id
                completion.TrySetResult() |> ignore
            }

    member _.Crash(message: string) =
        completion.TrySetException(InvalidOperationException message) |> ignore

    member _.Complete() = completion.TrySetResult() |> ignore
    member _.StopCount = stopCount

/// What the n-th started instance of a child does the moment it starts.
type ChildScript =
    | Run
    | CrashNow of string
    | ExitNormally
    | HangForever

type HangingChild(id: string, onStopped: string -> unit) =
    let completion =
        TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

    interface ISupervisedChild with
        member _.Completion = completion.Task
        member _.StopAsync(_: CancellationToken) = task { onStopped id }

type GatedStopChild(id: string, stopRelease: Task, onStopStarted: string -> unit, onStopFinished: string -> unit) =
    let completion =
        TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

    interface ISupervisedChild with
        member _.Completion = completion.Task

        member _.StopAsync(_: CancellationToken) =
            task {
                onStopStarted id
                do! stopRelease
                onStopFinished id
                completion.TrySetResult() |> ignore
            }

    member _.Crash(message: string) =
        completion.TrySetException(InvalidOperationException message) |> ignore

/// Builds a child spec with a per-instance script and records every started instance.
let mkChildSpec
    (id: string)
    (script: ChildScript list)
    (restart: RestartKind)
    (stopLog: ResizeArray<string>)
    (startedInstances: ResizeArray<FakeChild>)
    : ChildSpec =
    { Id = id
      Start =
        fun _ ->
            task {
                let index = startedInstances.Count

                let child =
                    FakeChild(id, fun stoppedId -> lock stopLog (fun _ -> stopLog.Add stoppedId))

                lock startedInstances (fun _ -> startedInstances.Add(child))

                let behavior = if index < script.Length then script[index] else Run

                match behavior with
                | Run
                | HangForever -> ()
                | CrashNow message -> child.Crash message
                | ExitNormally -> child.Complete()

                return child :> ISupervisedChild
            }
      Restart = restart
      RestartDelay = TimeSpan.Zero
      Shutdown = TimeSpan.FromMilliseconds 100.
      StartupRetry = None }

let mkHangingChildSpec (id: string) (stopLog: ResizeArray<string>) : ChildSpec =
    { Id = id
      Start =
        fun _ ->
            Task.FromResult(
                HangingChild(id, fun stoppedId -> lock stopLog (fun _ -> stopLog.Add stoppedId)) :> ISupervisedChild
            )
      Restart = Permanent
      RestartDelay = TimeSpan.Zero
      Shutdown = TimeSpan.FromMilliseconds 50.
      StartupRetry = None }

let waitFor (span: TimeSpan) (probe: unit -> bool) : bool =
    let sw = Stopwatch.StartNew()

    let rec loop () =
        if probe () then
            true
        elif sw.Elapsed > span then
            false
        else
            Thread.Sleep 20
            loop ()

    loop ()

let startedEvents (supervisor: ISupervisor) =
    supervisor.Events()
    |> List.filter (fun event -> event.Kind = Started)
    |> List.length

let restartedEvents (supervisor: ISupervisor) =
    supervisor.Events()
    |> List.filter (fun event -> event.Kind = Restarted)
    |> List.map (fun event -> event.ChildId)

let escalatedEvents (supervisor: ISupervisor) =
    supervisor.Events()
    |> List.filter (fun event -> event.Kind = Escalated)
    |> List.length

let spec (strategy: RestartStrategy) (intensity: int) (period: TimeSpan) (children: ChildSpec list) : SupervisorSpec =
    { Strategy = strategy
      Intensity = intensity
      Period = period
      Children = children }

let validationTests =
    testList
        "Supervisor.validate"
        [ test "a sound spec passes" {
              let child = mkChildSpec "only" [ Run ] Permanent (ResizeArray()) (ResizeArray())
              Expect.isOk (Supervisor.validate (spec OneForOne 5 (TimeSpan.FromSeconds 10.) [ child ])) "valid spec"
          }

          test "duplicate ids and weak intensity are rejected" {
              let child = mkChildSpec "dup" [ Run ] Permanent (ResizeArray()) (ResizeArray())

              match Supervisor.validate (spec OneForOne 0 TimeSpan.Zero [ child; child ]) with
              | Error errors ->
                  Expect.contains errors (DuplicateChildId "dup") "duplicate child ids"
                  Expect.contains errors (IntensityBelowOne 0) "intensity below one"
                  Expect.contains errors (PeriodNotPositive TimeSpan.Zero) "period not positive"
              | Ok _ -> failtest "expected defects"
          }

          test "negative delays are rejected per child" {
              let child =
                  { (mkChildSpec "delayed" [ Run ] Permanent (ResizeArray()) (ResizeArray())) with
                      RestartDelay = TimeSpan.FromSeconds -1.
                      Shutdown = TimeSpan.FromSeconds -1. }

              match Supervisor.validate (spec OneForOne 1 (TimeSpan.FromSeconds 1.) [ child ]) with
              | Error errors ->
                  Expect.contains
                      errors
                      (RestartDelayNegative("delayed", TimeSpan.FromSeconds -1.))
                      "negative restart delay"

                  Expect.contains
                      errors
                      (ShutdownNegative("delayed", TimeSpan.FromSeconds -1.))
                      "negative shutdown budget"
              | Ok _ -> failtest "expected defects"
          }

          test "Polly 8.8 startup timeout bounds are validated" {
              let timeout = TimeSpan.FromMilliseconds 1.

              let child =
                  { (mkChildSpec "quick" [ Run ] Permanent (ResizeArray()) (ResizeArray())) with
                      StartupRetry =
                          Some
                              { MaxAttempts = 1
                                BaseDelay = TimeSpan.Zero
                                MaxDelay = TimeSpan.Zero
                                UseJitter = false
                                StartupTimeout = timeout } }

              match Supervisor.validate (spec OneForOne 1 (TimeSpan.FromSeconds 1.) [ child ]) with
              | Error errors ->
                  Expect.contains
                      errors
                      (StartupTimeoutOutOfRange("quick", timeout))
                      "startup timeout matches Polly's lower bound"
              | Ok _ -> failtest "expected a startup timeout defect"
          } ]

let restartTests =
    testList
        "restart semantics"
        [ testTask "a permanent child that crashes is restarted" {
              let started = ResizeArray<FakeChild>()

              let child =
                  mkChildSpec "worker" [ CrashNow "boom" ] Permanent (ResizeArray()) started

              let! (supervisor: ISupervisor) =
                  Supervisor.start (spec OneForOne 5 (TimeSpan.FromMinutes 1.) [ child ]) ct testClock

              let restarted =
                  waitFor (TimeSpan.FromSeconds 5.) (fun () -> restartedEvents supervisor |> List.length >= 1)

              Expect.isTrue restarted "the child was restarted"
              Expect.equal 2 started.Count "a second instance started"

              do! supervisor.StopAsync ct
          }

          testTask "exceeding restart intensity escalates and faults the supervisor" {
              let started = ResizeArray<FakeChild>()

              let child =
                  mkChildSpec "fragile" [ CrashNow "one"; CrashNow "two" ] Permanent (ResizeArray()) started

              let! (supervisor: ISupervisor) =
                  Supervisor.start (spec OneForOne 1 (TimeSpan.FromMinutes 10.) [ child ]) ct testClock

              let! escalated =
                  task {
                      try
                          let! _ = supervisor.Completion
                          return None
                      with SupervisorEscalated(childId, reason) ->
                          return Some(childId, reason)
                  }

              match escalated with
              | Some(childId, reason) ->
                  Expect.equal "fragile" childId "the failing child is named"
                  Expect.stringContains reason "intensity" "the reason mentions intensity"
                  Expect.equal 1 (escalatedEvents supervisor) "one escalation recorded"
              | None -> failtest "the supervisor should have faulted"

              Expect.equal 2 started.Count "one restart was allowed before escalation"
          }

          testTask "the intensity window expires and allows new restarts" {
              let started = ResizeArray<FakeChild>()
              let clock = FakeTime(DateTimeOffset(2026, 9, 19, 12, 0, 0, TimeSpan.Zero))
              // The first instance crashes; the second runs until the test crashes it.
              let child =
                  { (mkChildSpec "cycler" [ CrashNow "one" ] Permanent (ResizeArray()) started) with
                      RestartDelay = TimeSpan.FromSeconds 1. }

              let! (supervisor: ISupervisor) =
                  Supervisor.start (spec OneForOne 1 (TimeSpan.FromSeconds 10.) [ child ]) ct clock

              // After the first restart, advance past the window before crashing again.
              let secondInstanceReady =
                  waitFor (TimeSpan.FromSeconds 5.) (fun () -> started.Count >= 2)

              Expect.isTrue secondInstanceReady "the restarted instance exists"

              clock.Advance(TimeSpan.FromSeconds 20.)
              (started[1]).Crash "two"

              let restartedTwice =
                  waitFor (TimeSpan.FromSeconds 5.) (fun () -> restartedEvents supervisor |> List.length >= 2)

              Expect.isTrue restartedTwice "the expired window allowed the second restart"
              Expect.equal 0 (escalatedEvents supervisor) "no escalation"

              do! supervisor.StopAsync ct
          }

          testTask "a temporary child is never restarted" {
              let started = ResizeArray<FakeChild>()

              let child =
                  mkChildSpec "ephemeral" [ CrashNow "boom" ] Temporary (ResizeArray()) started

              let! (supervisor: ISupervisor) =
                  Supervisor.start (spec OneForOne 5 (TimeSpan.FromMinutes 1.) [ child ]) ct testClock

              let stopped =
                  waitFor (TimeSpan.FromSeconds 5.) (fun _ ->
                      supervisor.Events()
                      |> List.exists (fun event -> event.Kind = Stopped && event.ChildId = "ephemeral"))

              Expect.isTrue stopped "the child stopped permanently"
              Expect.equal 1 started.Count "no second instance"

              do! supervisor.StopAsync ct
          }

          testTask "a transient child restarts only on abnormal exits" {
              let normalStarted = ResizeArray<FakeChild>()

              let normalChild =
                  mkChildSpec "politely" [ ExitNormally ] Transient (ResizeArray()) normalStarted

              let! (normalSupervisor: ISupervisor) =
                  Supervisor.start (spec OneForOne 5 (TimeSpan.FromMinutes 1.) [ normalChild ]) ct testClock

              let normalStopped =
                  waitFor (TimeSpan.FromSeconds 5.) (fun _ ->
                      normalSupervisor.Events()
                      |> List.exists (fun event -> event.Kind = Stopped && event.ChildId = "politely"))

              Expect.isTrue normalStopped "a normal exit is not restarted"
              Expect.equal 1 normalStarted.Count "exactly one instance"

              do! normalSupervisor.StopAsync ct

              let abnormalStarted = ResizeArray<FakeChild>()

              let abnormalChild =
                  mkChildSpec "rudely" [ CrashNow "kaboom" ] Transient (ResizeArray()) abnormalStarted

              let! (abnormalSupervisor: ISupervisor) =
                  Supervisor.start (spec OneForOne 5 (TimeSpan.FromMinutes 1.) [ abnormalChild ]) ct testClock

              let restarted =
                  waitFor (TimeSpan.FromSeconds 5.) (fun () -> restartedEvents abnormalSupervisor |> List.length >= 1)

              Expect.isTrue restarted "an abnormal exit is restarted"

              do! abnormalSupervisor.StopAsync ct
          } ]

let strategyTests =
    testList
        "restart strategies"
        [ testTask "one-for-all restarts every sibling" {
              let stopLog = ResizeArray<string>()
              let aStarted = ResizeArray<FakeChild>()
              let bStarted = ResizeArray<FakeChild>()
              let a = mkChildSpec "alpha" [ CrashNow "boom" ] Permanent stopLog aStarted
              let b = mkChildSpec "beta" [ Run ] Permanent stopLog bStarted

              let! (supervisor: ISupervisor) =
                  Supervisor.start (spec OneForAll 5 (TimeSpan.FromMinutes 1.) [ a; b ]) ct testClock

              let bothRestarted =
                  waitFor (TimeSpan.FromSeconds 5.) (fun () ->
                      (restartedEvents supervisor |> List.length) >= 2
                      && aStarted.Count >= 2
                      && bStarted.Count >= 2)

              Expect.isTrue bothRestarted "both children were restarted"
              Expect.contains stopLog "beta" "the healthy sibling was stopped first"

              do! supervisor.StopAsync ct
          }

          testTask "group restart waits for the terminate barrier and starts in declaration order" {
              let timeline = ResizeArray<string>()

              let record event =
                  lock timeline (fun () -> timeline.Add event)

              let snapshot () =
                  lock timeline (fun () -> List.ofSeq timeline)

              let completedGate =
                  TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

              completedGate.TrySetResult(()) |> ignore

              let alphaStop = completedGate

              let betaStop =
                  TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

              let gammaStop =
                  TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

              let alphaRestart =
                  TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

              let betaRestart =
                  TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

              let gammaRestart =
                  TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

              let alphaStarted = ResizeArray<GatedStopChild>()
              let betaStarted = ResizeArray<GatedStopChild>()
              let gammaStarted = ResizeArray<GatedStopChild>()

              let childSpec
                  id
                  (stopGate: TaskCompletionSource<unit>)
                  (restartGate: TaskCompletionSource<unit>)
                  (started: ResizeArray<GatedStopChild>)
                  =
                  { Id = id
                    Start =
                      fun _ ->
                          task {
                              let isRestart = lock started (fun () -> started.Count > 0)

                              if isRestart then
                                  record $"restart-began-{id}"
                                  do! restartGate.Task

                              let child =
                                  GatedStopChild(
                                      id,
                                      stopGate.Task,
                                      (fun childId -> record $"stop-began-{childId}"),
                                      (fun childId -> record $"stop-finished-{childId}")
                                  )

                              lock started (fun () -> started.Add child)
                              return child :> ISupervisedChild
                          }
                    Restart = Permanent
                    RestartDelay = TimeSpan.Zero
                    Shutdown = TimeSpan.FromSeconds 2.
                    StartupRetry = None }

              let alpha = childSpec "alpha" alphaStop alphaRestart alphaStarted
              let beta = childSpec "beta" betaStop betaRestart betaStarted
              let gamma = childSpec "gamma" gammaStop gammaRestart gammaStarted

              let! (supervisor: ISupervisor) =
                  Supervisor.start (spec OneForAll 10 (TimeSpan.FromMinutes 1.) [ alpha; beta; gamma ]) ct testClock

              alphaStarted[0].Crash "boom"

              Expect.isTrue
                  (waitFor (TimeSpan.FromSeconds 2.) (fun () -> snapshot () |> List.contains "stop-began-gamma"))
                  "reverse-order termination began with gamma"

              Expect.isFalse
                  (snapshot () |> List.exists (fun event -> event.StartsWith "restart-began-"))
                  "no restart began while gamma was stopping"

              gammaStop.TrySetResult(()) |> ignore

              Expect.isTrue
                  (waitFor (TimeSpan.FromSeconds 2.) (fun () -> snapshot () |> List.contains "stop-began-beta"))
                  "beta was stopped after gamma"

              Expect.isFalse
                  (snapshot () |> List.exists (fun event -> event.StartsWith "restart-began-"))
                  "no restart began while beta was stopping"

              betaStop.TrySetResult(()) |> ignore

              Expect.isTrue
                  (waitFor (TimeSpan.FromSeconds 2.) (fun () -> snapshot () |> List.contains "restart-began-alpha"))
                  "alpha restarted after every stop finished"

              Expect.isFalse
                  (waitFor (TimeSpan.FromMilliseconds 200.) (fun () ->
                      snapshot () |> List.contains "restart-began-beta"))
                  "beta did not start before alpha finished starting"

              alphaRestart.TrySetResult(()) |> ignore

              Expect.isTrue
                  (waitFor (TimeSpan.FromSeconds 2.) (fun () -> snapshot () |> List.contains "restart-began-beta"))
                  "beta started after alpha"

              Expect.isFalse
                  (snapshot () |> List.contains "restart-began-gamma")
                  "gamma did not start before beta finished starting"

              betaRestart.TrySetResult(()) |> ignore

              Expect.isTrue
                  (waitFor (TimeSpan.FromSeconds 2.) (fun () -> snapshot () |> List.contains "restart-began-gamma"))
                  "gamma started after beta"

              gammaRestart.TrySetResult(()) |> ignore

              Expect.isTrue
                  (waitFor (TimeSpan.FromSeconds 2.) (fun () -> restartedEvents supervisor |> List.length = 3))
                  "all affected children restarted"

              let restartOrder =
                  snapshot () |> List.filter (fun event -> event.StartsWith "restart-began-")

              Expect.equal
                  [ "restart-began-alpha"; "restart-began-beta"; "restart-began-gamma" ]
                  restartOrder
                  "group restarts followed declaration order"

              do! supervisor.StopAsync ct
          }

          testTask "rest-for-one restarts only younger siblings" {
              let stopLog = ResizeArray<string>()
              let aStarted = ResizeArray<FakeChild>()
              let bStarted = ResizeArray<FakeChild>()
              let cStarted = ResizeArray<FakeChild>()
              let a = mkChildSpec "alpha" [ Run ] Permanent stopLog aStarted
              let b = mkChildSpec "beta" [ CrashNow "boom" ] Permanent stopLog bStarted
              let c = mkChildSpec "gamma" [ Run ] Permanent stopLog cStarted

              let! (supervisor: ISupervisor) =
                  Supervisor.start (spec RestForOne 5 (TimeSpan.FromMinutes 1.) [ a; b; c ]) ct testClock

              let youngerRestarted =
                  waitFor (TimeSpan.FromSeconds 5.) (fun () -> bStarted.Count >= 2 && cStarted.Count >= 2)

              Expect.isTrue youngerRestarted "beta and gamma restarted"

              let restarted = restartedEvents supervisor
              Expect.contains restarted "beta" "the failed child restarted"
              Expect.contains restarted "gamma" "the younger sibling restarted"
              Expect.isFalse (List.contains "alpha" restarted) "the older sibling was left alone"
              Expect.equal 1 aStarted.Count "alpha never restarted"
              Expect.isFalse (stopLog.Contains "alpha") "alpha was never stopped"

              do! supervisor.StopAsync ct
          } ]

let lifecycleTests =
    testList
        "lifecycle"
        [ testTask "startup retry recovers a flaky start" {
              let attempts = ref 0
              let started = ResizeArray<FakeChild>()

              let child =
                  { (mkChildSpec "flaky" [ Run ] Permanent (ResizeArray()) started) with
                      Start =
                          fun _ ->
                              task {
                                  incr attempts

                                  if !attempts < 3 then
                                      raise (InvalidOperationException "flaky start")

                                  let child = FakeChild("flaky", ignore)
                                  lock started (fun _ -> started.Add(child))
                                  return child :> ISupervisedChild
                              }
                      StartupRetry =
                          Some
                              { MaxAttempts = 3
                                BaseDelay = TimeSpan.FromMilliseconds 1.
                                MaxDelay = TimeSpan.FromMilliseconds 5.
                                UseJitter = false
                                StartupTimeout = TimeSpan.FromSeconds 5. } }

              let! (supervisor: ISupervisor) =
                  Supervisor.start (spec OneForOne 1 (TimeSpan.FromMinutes 1.) [ child ]) ct testClock

              Expect.equal 3 !attempts "the third startup attempt succeeded"
              Expect.equal 1 (startedEvents supervisor) "one Started event"

              do! supervisor.StopAsync ct
          }

          testTask "an unstartable child faults the supervisor start" {
              let child =
                  { (mkChildSpec "brick" [ Run ] Permanent (ResizeArray()) (ResizeArray())) with
                      Start = fun _ -> Task.FromException<ISupervisedChild>(InvalidOperationException "bricked") }

              let! outcome =
                  task {
                      try
                          let! _ = Supervisor.start (spec OneForOne 1 (TimeSpan.FromMinutes 1.) [ child ]) ct testClock
                          return None
                      with SupervisorEscalated(childId, reason) ->
                          return Some(childId, reason)
                  }

              match outcome with
              | Some(childId, reason) ->
                  Expect.equal "brick" childId "the unstartable child is named"
                  Expect.stringContains reason "start failed" "the reason mentions the failed start"
              | None -> failtest "the supervisor start should have faulted"
          }

          testTask "shutdown stops children in reverse order within budgets" {
              let stopLog = ResizeArray<string>()
              let a = mkChildSpec "alpha" [ Run ] Permanent stopLog (ResizeArray())
              let b = mkChildSpec "beta" [ Run ] Permanent stopLog (ResizeArray())
              let c = mkHangingChildSpec "hanging" stopLog

              let! (supervisor: ISupervisor) =
                  Supervisor.start (spec OneForOne 5 (TimeSpan.FromMinutes 1.) [ a; b; c ]) ct testClock

              let sw = Stopwatch.StartNew()
              do! supervisor.StopAsync ct
              sw.Stop()

              Expect.equal [ "hanging"; "beta"; "alpha" ] (List.ofSeq stopLog) "reverse start order"
              Expect.isTrue (sw.Elapsed < TimeSpan.FromSeconds 4.) "a hanging child cannot stall shutdown forever"
              Expect.isTrue supervisor.Completion.IsCompleted "the supervisor settled"
          }

          testTask "stopping an empty supervisor completes immediately" {
              let! (supervisor: ISupervisor) =
                  Supervisor.start (spec OneForOne 1 (TimeSpan.FromSeconds 1.) []) ct testClock

              do! supervisor.StopAsync ct
              Expect.isTrue supervisor.Completion.IsCompleted "nothing to wait for"
          }

          testTask "cancellation before initial startup cannot hang" {
              let cts = new CancellationTokenSource()
              cts.Cancel()

              let child = mkChildSpec "never" [ Run ] Permanent (ResizeArray()) (ResizeArray())

              let! canceled =
                  task {
                      try
                          let! _ =
                              (Supervisor.start
                                  (spec OneForOne 1 (TimeSpan.FromSeconds 1.) [ child ])
                                  cts.Token
                                  testClock)
                                  .WaitAsync(TimeSpan.FromSeconds 2.)

                          return false
                      with :? OperationCanceledException ->
                          return true
                  }

              Expect.isTrue canceled "start completes as canceled"
          }

          testTask "parent cancellation stops a running child" {
              let cts = new CancellationTokenSource()
              let started = ResizeArray<FakeChild>()
              let child = mkChildSpec "worker" [ Run ] Permanent (ResizeArray()) started

              let! (supervisor: ISupervisor) =
                  Supervisor.start (spec OneForOne 1 (TimeSpan.FromSeconds 1.) [ child ]) cts.Token testClock

              cts.Cancel()
              do! supervisor.Completion.WaitAsync(TimeSpan.FromSeconds 2.)
              Expect.equal 1 started[0].StopCount "the child received an orderly stop request"
          }

          testTask "a child finishing startup during cancellation is stopped" {
              let cts = new CancellationTokenSource()

              let startGate =
                  TaskCompletionSource<ISupervisedChild>(TaskCreationOptions.RunContinuationsAsynchronously)

              let child = FakeChild("late", ignore)

              let childSpec =
                  { Id = "late"
                    Start = fun _ -> startGate.Task
                    Restart = Permanent
                    RestartDelay = TimeSpan.Zero
                    Shutdown = TimeSpan.FromMilliseconds 100.
                    StartupRetry = None }

              let starting =
                  Supervisor.start (spec OneForOne 1 (TimeSpan.FromSeconds 1.) [ childSpec ]) cts.Token testClock

              cts.Cancel()
              startGate.TrySetResult(child) |> ignore

              let! canceled =
                  task {
                      try
                          let! _ = starting.WaitAsync(TimeSpan.FromSeconds 2.)
                          return false
                      with :? OperationCanceledException ->
                          return true
                  }

              let stopped = waitFor (TimeSpan.FromSeconds 2.) (fun () -> child.StopCount = 1)
              Expect.isTrue canceled "the initial start reports cancellation"
              Expect.isTrue stopped "the late child is not leaked"
          } ]

let tests =
    testList "supervisor" [ validationTests; restartTests; strategyTests; lifecycleTests ]
