module ByzantineSystems.Automata.Examples.Supervision.Program

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Resilience
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging

/// A supervised child that crashes after a short delay, to show restart behaviour.
/// Its <see cref="T:ByzantineSystems.Automata.Resilience.ISupervisedChild.Completion" />
/// faults on the crash and completes normally when asked to stop.
type FlakyChild(name: string, crashAfter: TimeSpan) =

    let completion =
        TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

    let stopping = new CancellationTokenSource()
    let mutable loopTask = Task.CompletedTask

    member _.Start() : unit =
        loopTask <-
            task {
                try
                    do! Task.Delay(crashAfter, stopping.Token)

                    completion.TrySetException(InvalidOperationException $"{name} crashed")
                    |> ignore
                with
                | :? OperationCanceledException -> completion.TrySetResult() |> ignore
                | error -> completion.TrySetException(error) |> ignore
            }

        loopTask |> ignore

    interface ISupervisedChild with

        member _.Completion: Task = completion.Task

        member _.StopAsync(ct: CancellationToken) : Task =
            task {
                stopping.Cancel()
                do! loopTask.WaitAsync(ct)
            }

let private flakySpec =
    { Id = "flaky-worker"
      Start =
        fun (_ct: CancellationToken) ->
            task {
                let child = FlakyChild("flaky-worker", TimeSpan.FromMilliseconds 60.)
                child.Start()
                return child :> ISupervisedChild
            }
      Restart = RestartKind.Transient
      RestartDelay = TimeSpan.Zero
      Shutdown = TimeSpan.FromSeconds 2.
      StartupRetry = None }

let private spec =
    { Strategy = RestartStrategy.OneForOne
      Intensity = 3
      Period = TimeSpan.FromMinutes 1.
      Children = [ flakySpec ] }

let private run (logger: ILogger) : Task =
    task {
        let! supervisor = Supervisor.start spec CancellationToken.None TimeProvider.System

        logger.LogInformation(
            "Supervisor started; child crash interval is {CrashInterval}",
            TimeSpan.FromMilliseconds 60.
        )

        try
            do! supervisor.Completion.WaitAsync(TimeSpan.FromSeconds 10.)
        with :? SupervisorEscalated as escalated ->
            logger.LogWarning(
                "Supervisor escalated child {ChildId}: {EscalationReason}",
                escalated.childId,
                escalated.reason
            )

        logger.LogInformation("Supervision events in occurrence order")

        for event in supervisor.Events() do
            logger.LogInformation(
                "Supervision event {EventKind} for child {ChildId}: {EventReason}",
                event.Kind,
                event.ChildId,
                event.Reason
            )

        do! supervisor.StopAsync CancellationToken.None
    }

[<EntryPoint>]
let main args =
    let builder = Host.CreateApplicationBuilder(args)
    use host = builder.Build()

    let logger =
        host.Services
            .GetRequiredService<ILoggerFactory>()
            .CreateLogger("ByzantineSystems.Automata.Examples.Supervision")

    try
        (run logger).GetAwaiter().GetResult()
        0
    with ex ->
        logger.LogError(ex, "Supervision example failed")
        1
