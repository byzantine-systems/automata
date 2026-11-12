module ByzantineSystems.Automata.Runtime.Tests.LoopTests

open System
open System.Diagnostics
open System.Reflection
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Runtime
open ByzantineSystems.Automata.Runtime.Tests.TestSupport
open ByzantineSystems.Automata.Storage
open Expecto

let runRecurringStress () =
    let iterations = 100_000
    let mutable count = 0
    let mutable early = 0L
    let mutable late = 0L

    let step () =
        count <- count + 1

        if count = 10_000 then
            early <- GC.GetTotalMemory(true)

        if count = iterations then
            late <- GC.GetTotalMemory(true)
            Task.FromResult false
        else
            Task.FromResult true

    Recurring.repeat step CancellationToken.None |> _.GetAwaiter().GetResult()

    if count <> iterations then
        invalidOp $"expected {iterations} steps, observed {count}"

    let retained = late - early

    if retained > 4_000_000L then
        invalidOp $"the loop retained {retained} bytes across 90,000 synchronous steps"

let private committedTransition ordinal : CommittedTransition<EntityId<TestEntity>, TestState, TestEvent, TestAction> =
    { Draft =
        { MachineId = testMachine
          EntityId = entityId "E-1"
          Event = Start ordinal
          Actions = []
          FromState = Idle
          ToState = Active ordinal
          HandledBy = stateId "idle"
          Exited = []
          Entered = []
          Status = InstanceStatus.Running
          EffectiveAt = startTime }
      Epoch = Epoch.ofUInt64 (uint64 ordinal)
      CommandId = CommandId.ofInt64 (int64 ordinal)
      ChartVersion = ChartVersion.create 1
      CommittedAt = startTime }

let tests =
    testList
        "long-running loops"
        [ testCase "synchronous recurring steps use bounded stack" (fun () ->
              let iterations = 256
              let mutable count = 0
              let mutable shallowest = System.Int32.MaxValue
              let mutable deepest = 0

              let step () =
                  count <- count + 1
                  let depth = StackTrace(false).FrameCount
                  shallowest <- min shallowest depth
                  deepest <- max deepest depth
                  Task.FromResult(count < iterations)

              Recurring.repeat step CancellationToken.None |> _.GetAwaiter().GetResult()

              Expect.equal count iterations "every requested step ran"
              Expect.isLessThanOrEqual (deepest - shallowest) 16 "stack depth stayed independent of iteration count")

          testCase "100k synchronous recurring steps retain bounded memory" (fun () ->
              let start = ProcessStartInfo("dotnet")
              start.ArgumentList.Add(Assembly.GetExecutingAssembly().Location)
              start.ArgumentList.Add("--recurring-stress")
              start.UseShellExecute <- false
              start.RedirectStandardError <- true

              use child = Process.Start start

              if not (child.WaitForExit 30_000) then
                  child.Kill(true)
                  failtest "the recurring stress process did not finish in 30 seconds"

              Expect.equal child.ExitCode 0 (child.StandardError.ReadToEnd()))

          Property.property "processor totals are the fold of every full batch" (fun (FsCheck.PositiveInt seed) ->
              let count = (seed % 25 + 1) * 4
              let store = StubStore<TestError>()

              let machine =
                  buildMachine store (newTime ()) (fun policy ->
                      { policy with
                          Batch = 4
                          Concurrency = 1 })
                  |> expectMachine

              for command in 1..count do
                  store.Offer(leasedCommand (int64 command) $"E-{command}" (Start command) 0)

              use cancellation = new CancellationTokenSource()
              let running = (Machine.processor machine).RunAsync cancellation.Token

              let finished =
                  SpinWait.SpinUntil((fun () -> store.Commits.Count = count), TimeSpan.FromSeconds 5.)

              cancellation.Cancel()

              let stopped = running.WaitAsync(TimeSpan.FromSeconds 5.).GetAwaiter().GetResult()

              finished
              && match stopped with
                 | ProcessorStop.Drained totals ->
                     totals.Claimed = count
                     && totals.Committed = count
                     && totals.Rejected = 0
                     && totals.Rescheduled = 0
                     && totals.DeadLettered = 0
                     && totals.LeaseLost = 0
                 | ProcessorStop.Escalated _ -> false)

          Property.property "dispatcher totals are the fold of every full batch" (fun (FsCheck.PositiveInt seed) ->
              let count = (seed % 25 + 1) * 4
              let store = StubStore<TestError>()

              let machine =
                  buildMachine store (newTime ()) (fun policy ->
                      { policy with
                          Batch = 4
                          Concurrency = 1 })
                  |> expectMachine

              for ordinal in 1..count do
                  store.OfferAction(leasedAction (int64 ordinal) (int64 ordinal) ordinal 1)

              use cancellation = new CancellationTokenSource()
              let mutable handled = 0

              let handler: ActionHandler<_, _, TestError> =
                  fun _ _ ->
                      handled <- handled + 1

                      if handled = count then
                          cancellation.Cancel()

                      Task.FromResult(Ok())

              let report =
                  (Machine.dispatcher machine handler).RunAsync(cancellation.Token).WaitAsync(TimeSpan.FromSeconds 5.)
                  |> _.GetAwaiter().GetResult()

              report.Claimed = count
              && report.Delivered = count
              && report.Rescheduled = 0
              && report.Abandoned = 0
              && report.LeaseLost = 0)

          testCase "observer completion drains every accepted transition" (fun () ->
              let count = 1_000
              let observed = ResizeArray()

              let observer =
                  ObserverDispatcher<_, _, _, _>(
                      (fun transition _ ->
                          observed.Add(CommandId.value transition.CommandId)
                          Task.FromResult(())),
                      count
                  )

              for ordinal in 1..count do
                  observer.TryPublish(committedTransition ordinal)

              observer.Complete()

              observer.RunAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds 5.).GetAwaiter().GetResult()

              Expect.sequenceEqual
                  observed
                  (seq { 1L .. int64 count })
                  "accepted transitions were observed once and in channel order") ]
