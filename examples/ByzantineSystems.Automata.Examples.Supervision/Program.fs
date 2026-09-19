module ByzantineSystems.Automata.Examples.Supervision.Program

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Resilience

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

let private run () : Task =
    task {
        let! supervisor = Supervisor.start spec CancellationToken.None TimeProvider.System
        printfn "supervisor started; the child crashes every 60ms"

        try
            do! supervisor.Completion.WaitAsync(TimeSpan.FromSeconds 10.)
        with :? SupervisorEscalated as escalated ->
            printfn "escalated: %s (%s)" escalated.childId escalated.reason

        printfn ""
        printfn "supervision events (occurrence order):"

        for event in supervisor.Events() do
            printfn "  %-10A %-20s %s" event.Kind event.ChildId event.Reason

        do! supervisor.StopAsync CancellationToken.None
    }

[<EntryPoint>]
let main _ =
    try
        (run ()).GetAwaiter().GetResult()
        0
    with ex ->
        eprintfn "error: %s" ex.Message
        1
