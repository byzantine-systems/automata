module ByzantineSystems.Automata.Storage.Sqlite.Tests.CrossProcessTests

open System
open System.Diagnostics
open System.IO
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Sqlite.Tests.TestContext
open Expecto

/// The argument that turns this test binary into a claim worker.
[<Literal>]
let WorkerFlag = "--claim-worker"

let private entities = [ for index in 1..5 -> $"e{index}" ]
let private perEntity = 6

/// How long a worker holds each command, so the two workers' claims overlap in time.
let private workTime = TimeSpan.FromMilliseconds 40.0

let private openCommands (db: string) =
    scalar db "SELECT count(*) FROM fsm_command WHERE status IN ('ready', 'leased');"

/// <summary>
/// The child's side: wait for the go signal, then claim and finish commands until none are open,
/// printing one line per command this process finished. The WriteGate is per process, so two of
/// these meet only at SQLite's own lock, which is exactly what this test is about.
/// </summary>
let runWorker (db: string) (go: string) : int =
    let fixture =
        { Db = db
          Clock = ManualClock startTime
          Context = contextFor db }

    let inbox = inboxOf fixture
    let processor = processorOf fixture

    Console.Out.WriteLine "ready"
    Console.Out.Flush()

    while not (File.Exists go) do
        Threading.Thread.Sleep 5

    let rec loop () =
        task {
            match! inbox.Claim(testMachine, 3, TimeSpan.FromSeconds 30.0, noCancellation) with
            | Error error -> return failwithf "the worker's claim failed: %A" error
            | Ok [] when openCommands db = "0" -> return ()
            | Ok [] ->
                do! Task.Delay 5
                return! loop ()
            | Ok batch ->
                for held in batch do
                    do! Task.Delay workTime

                    match!
                        processor.Reject(
                            held.Work.CommandId,
                            held.Token,
                            CommandFailure.Domain(Refused "done"),
                            noCancellation
                        )
                    with
                    | Ok(Finalized _) ->
                        Console.Out.WriteLine $"{CommandId.value held.Work.CommandId} {LeaseToken.value held.Token}"
                    | other -> failwithf "the worker's finalize answered %A" other

                Console.Out.Flush()
                return! loop ()
        }

    (loop ()).GetAwaiter().GetResult()
    0

/// Starts this test binary again as a worker. The binary is run through the dotnet host, which
/// works whether the suite itself was started by dotnet run, dotnet test or the apphost.
let private spawn (db: string) (go: string) : Process =
    let host =
        match Environment.GetEnvironmentVariable "DOTNET_HOST_PATH" with
        | null
        | "" -> "dotnet"
        | path -> path

    let info = ProcessStartInfo(host)
    info.ArgumentList.Add(typeof<ManualClock>.Assembly.Location)
    info.ArgumentList.Add WorkerFlag
    info.ArgumentList.Add db
    info.ArgumentList.Add go
    info.RedirectStandardOutput <- true
    info.RedirectStandardError <- true
    info.UseShellExecute <- false
    Process.Start info

let tests =
    testList
        "two processes"
        [ testTask "claims from two processes never share a command, and every entity stays ordered" {
              let db = fixture "cross-process"
              let inbox = inboxOf db

              for name in entities do
                  for index in 1..perEntity do
                      let! _ = submit inbox (entityId name) $"{name}-{index}" (Start index)
                      ()

              // Created only once both workers are waiting; its existence is the start signal.
              let go = freshPath "go"
              let first = spawn db.Db go
              let second = spawn db.Db go

              // Both are running and waiting before either may claim, so their claims overlap.
              for worker in [ first; second ] do
                  let! ready = worker.StandardOutput.ReadLineAsync()
                  Expect.equal ready "ready" "the worker started"

              File.WriteAllText(go, "")

              let finish (worker: Process) =
                  task {
                      let! output = worker.StandardOutput.ReadToEndAsync()
                      let! errors = worker.StandardError.ReadToEndAsync()
                      do! worker.WaitForExitAsync()
                      Expect.equal worker.ExitCode 0 $"the worker exited cleanly: {errors}"

                      return
                          output.Split('\n', StringSplitOptions.RemoveEmptyEntries)
                          |> Array.map (fun line ->
                              match line.Trim().Split ' ' with
                              | [| commandId; token |] -> int64 commandId, int64 token
                              | _ -> failtestf "an unreadable worker line: %s" line)
                          |> List.ofArray
                  }

              let! firstDone = finish first
              let! secondDone = finish second
              first.Dispose()
              second.Dispose()

              let finished = firstDone @ secondDone |> List.map fst

              Expect.equal
                  (List.sort finished)
                  [ 1L .. int64 (List.length entities * perEntity) ]
                  "every command was finished exactly once, by exactly one process"

              Expect.isNonEmpty firstDone "the first process did some of the work"
              Expect.isNonEmpty secondDone "and so did the second"

              // Each process's claims drew tokens from the same counter inside the same file lock,
              // so no token was ever handed to both.
              let tokensOf (finishedBy: (int64 * int64) list) =
                  finishedBy |> List.map snd |> Set.ofList

              Expect.isEmpty
                  (Set.intersect (tokensOf firstDone) (tokensOf secondDone))
                  "no lease token was issued to both processes"

              // Finishing order within an entity is the order the errors were recorded in, and it
              // must be submission order whichever process did the work.
              for name in entities do
                  Expect.equal
                      (column
                          db.Db
                          "SELECT c.seq FROM fsm_command c JOIN fsm_command_error e ON e.command_id = c.command_id
                           WHERE c.entity_id = @entity ORDER BY e.recorded_at, c.seq;"
                          [ "@entity", box name ])
                      [ for index in 1..perEntity -> string index ]
                      $"{name} finished in submission order"
          } ]
