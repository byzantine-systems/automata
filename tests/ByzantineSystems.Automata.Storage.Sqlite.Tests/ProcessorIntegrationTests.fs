module ByzantineSystems.Automata.Storage.Sqlite.Tests.ProcessorIntegrationTests

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Runtime
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Sqlite
open ByzantineSystems.Automata.Storage.Sqlite.Tests.TestContext
open Expecto

type private Built = Machine<Entity, TestState, TestEvent, TestAction, TestError>

let private entity name : Entity = entityId name

let private integrationChart =
    statechart<TestState, TestEvent, TestAction, TestError> {
        root "root"

        classify (function
            | Idle -> stateId "idle"
            | Active _ -> stateId "active")

        state "idle" {
            on
                (fun _ event ->
                    match event with
                    | Start _ -> true
                    | Finish -> false)
                (fun _ event ->
                    match event with
                    | Start n -> [ Notify "started" ], Active n
                    | Finish -> [], Idle)
        }

        state "active" {
            on
                (fun _ event ->
                    match event with
                    | Start _ -> true
                    | Finish -> false)
                (fun state event ->
                    match state, event with
                    // Each Start adds to the running total, so the committed state records the
                    // order the commands were applied in, not only the last one.
                    | Active total, Start n -> [ Notify "added" ], Active(total + n)
                    | _ -> [], state)
        }
    }
    |> function
        | Ok chart -> chart
        | Error errors -> failwithf "the integration chart is invalid: %A" errors

let private build (machineStore: Store) : Built =
    MachineCE.machine<Entity, TestState, TestEvent, TestAction, TestError> testMachine {
        chart integrationChart
        chartVersion 1
        initialState Idle
        store (machineStore :> IMachineStore<Entity, TestState, TestEvent, TestAction, TestError>)

        processor
            { ProcessorPolicy.defaults with
                Batch = 16
                PollingInterval = TimeSpan.FromMilliseconds 20.
                Lease = TimeSpan.FromSeconds 10.
                RenewAfter = TimeSpan.FromSeconds 5. }
    }
    |> function
        | Ok built -> built
        | Error errors -> failtestf "the integration machine did not build: %A" errors

/// A migrated file with nothing declared: starting the machine registers its chart.
let private started () =
    task {
        let db = migrated "processor"
        let context = contextFor db
        let store = storeOf context
        let built = build store
        let! _ = Machine.startAsync built (SqliteChartRegistry({ Context = context })) noCancellation
        return db, store, built
    }

let private drainUntil (processors: CommandProcessor<Entity, TestState, TestEvent, TestAction, TestError> list) until =
    task {
        let deadline = DateTimeOffset.UtcNow.AddSeconds 20.
        let mutable settled = until ()

        while not settled && DateTimeOffset.UtcNow < deadline do
            let! _ =
                processors
                |> List.map (fun p -> p.PollAsync CancellationToken.None)
                |> Task.WhenAll

            settled <- until ()

        if not settled then
            failtest "the processors did not settle within the budget"
    }

let private epochs (db: string) (name: string) =
    column db "SELECT epoch FROM fsm_transition WHERE entity_id = @entity ORDER BY epoch;" [ "@entity", box name ]

let private stateOf (store: Store) name =
    task {
        let! snapshot =
            (store :> IStateReader<Entity, TestState, TestEvent, TestAction>)
                .TryGetSnapshot(testMachine, entity name, noCancellation)

        return snapshot |> expectOk "snapshot" |> Option.map _.State
    }

let private enqueue (built: Built) name key event =
    task {
        match! Machine.enqueue built (entity name) (EventEnvelope.create key event) noCancellation with
        | Ok outcome -> return outcome
        | Error error -> return failtestf "enqueue %s failed: %A" key error
    }

let tests =
    testList
        "command processor"
        [ testTask "a machine boots, and a submitted command is claimed, resolved and committed" {
              let! db, store, built = started ()

              let! submitted = enqueue built "e1" "k1" (Start 5)
              let commandId = commandIdOf submitted

              do! drainUntil [ Machine.processor built ] (fun () -> (epochs db "e1").Length = 1)

              let! state = stateOf store "e1"
              Expect.equal state (Some(Active 5)) "the chart's decision was committed"

              match! Machine.commandResult built commandId noCancellation with
              | Ok(Some(CommandResult.Committed committed)) ->
                  Expect.equal committed.Epoch (Epoch.ofUInt64 1UL) "the first epoch"
                  Expect.equal committed.CommandId commandId "the command it came from"
              | other -> failtestf "expected a committed result, got %A" other

              Expect.equal
                  (scalar db "SELECT count(*) FROM fsm_action;")
                  "1"
                  "the chart's action was queued by the commit itself"

              do! Machine.stopAsync built noCancellation
          }

          testTask "commands for one entity commit in order under two processors" {
              let! db, storeA, machineA = started ()
              let machineB = build (storeOf (contextFor db))
              let! _ = Machine.startAsync machineB (SqliteChartRegistry({ Context = contextFor db })) noCancellation

              for index in 1..10 do
                  let! _ = enqueue machineA "ordered" $"k{index}" (Start index)
                  ()

              do!
                  drainUntil [ Machine.processor machineA; Machine.processor machineB ] (fun () ->
                      (epochs db "ordered").Length = 10)

              Expect.equal (epochs db "ordered") [ for epoch in 1..10 -> string epoch ] "gapless and in order"

              // 1 + 2 + ... + 10. A lost or doubled command changes the sum; the gapless epochs
              // rule out a reorder.
              let! state = stateOf storeA "ordered"
              Expect.equal state (Some(Active 55)) "every command applied exactly once"

              do! Machine.stopAsync machineA noCancellation
              do! Machine.stopAsync machineB noCancellation
          }

          testTask "different entities are claimed in one poll" {
              let! _, _, built = started ()

              for index in 1..8 do
                  let! _ = enqueue built $"e{index}" $"k{index}" (Start 1)
                  ()

              match! (Machine.processor built).PollAsync noCancellation with
              | Ok report -> Expect.equal report.Summary.Claimed 8 "one claim took all eight heads"
              | Error error -> failtestf "the poll failed: %A" error

              do! Machine.stopAsync built noCancellation
          }

          testTask "an unhandled event is dead-lettered and releases its entity" {
              let! db, store, built = started ()

              let! poison = enqueue built "stuck" "poison" Finish
              let! _ = enqueue built "stuck" "after" (Start 2)

              do! drainUntil [ Machine.processor built ] (fun () -> (epochs db "stuck").Length = 1)

              match! Machine.commandResult built (commandIdOf poison) noCancellation with
              | Ok(Some(CommandResult.DeadLettered(CommandFailure.Machine _))) -> ()
              | other -> failtestf "expected a machine-reason dead letter, got %A" other

              let! state = stateOf store "stuck"
              Expect.equal state (Some(Active 2)) "the command behind it ran"

              do! Machine.stopAsync built noCancellation
          }

          // This store cannot replay the past, so the runtime finds no replay capability and
          // dead-letters a correction. What must hold is that the entity is not wedged by it.
          testTask "a correction is dead-lettered with its reason and releases its entity" {
              let! db, store, built = started ()

              let! _ = enqueue built "corrected" "k1" (Start 1)
              do! drainUntil [ Machine.processor built ] (fun () -> (epochs db "corrected").Length = 1)

              let! correction =
                  Machine.correct
                      built
                      (entity "corrected")
                      (EventEnvelope.create "fix" (Start 10))
                      (DateTimeOffset.UtcNow.AddDays -1.0)
                      CorrectionPolicy.defaults
                      noCancellation

              let correctionId = correction |> expectOk "correct" |> commandIdOf
              let! _ = enqueue built "corrected" "k2" (Start 2)

              do! drainUntil [ Machine.processor built ] (fun () -> (epochs db "corrected").Length = 2)

              match! Machine.commandResult built correctionId noCancellation with
              | Ok(Some(CommandResult.DeadLettered(CommandFailure.Machine reason))) ->
                  Expect.stringContains reason "correction" "the reason says what the store cannot do"
              | other -> failtestf "expected the correction to be dead-lettered, got %A" other

              let! state = stateOf store "corrected"
              Expect.equal state (Some(Active 3)) "the command after the correction still ran"

              do! Machine.stopAsync built noCancellation
          } ]
