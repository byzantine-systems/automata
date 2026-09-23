module ByzantineSystems.Automata.Storage.Postgres.Tests.ProcessorIntegrationTests

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Resilience
open ByzantineSystems.Automata.Runtime
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Postgres
open Expecto
open TestContext

/// <summary>
/// What the command processor claims is only true of a real database, so it is proven here.
///
/// One limit is worth stating rather than implying. Two processors in this process, even on
/// independent data sources, prove exclusion across connections and not across operating-system
/// processes. The cross-process proof needs a spawned second process and belongs with the
/// benchmark harness; nothing below should be read as covering it.
/// </summary>
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
                    // Each Start adds to the running total, so the committed state is a record
                    // of the order the commands were applied in rather than only the last one.
                    | Active total, Start n -> [ Notify "added" ], Active(total + n)
                    | _ -> [], state)
        }
    }
    |> function
        | Ok c -> c
        | Error errors -> failwithf "the integration chart is invalid: %A" errors

/// A store on its own data source, so two of these share nothing but the database.
let private ownStore () =
    let encode, decode = EntityKey.forEntityId<TestEntity>
    let source = DataSource.create connectionString

    PostgresMachineStore<Entity, TestState, TestEvent, TestAction, TestError>(
        { Context = PostgresContext.create source TransientPolicy.defaults ignore
          ActionQueue = actionQueue
          StateCodec = Serialization.systemTextJson<TestState> ()
          EventCodec = Serialization.systemTextJson<TestEvent> ()
          ActionCodec = Serialization.systemTextJson<TestAction> ()
          ErrorCodec = Serialization.systemTextJson<TestError> ()
          EntityIdEncode = encode
          EntityIdDecode = decode }
    )

let private buildMachine
    (machineStore: PostgresMachineStore<Entity, TestState, TestEvent, TestAction, TestError>)
    (time: TimeProvider)
    =
    // Qualified because this suite binds `machine` to its own machine id, which shadows the
    // computation expression of the same name.
    MachineCE.machine<Entity, TestState, TestEvent, TestAction, TestError> machine {
        chart integrationChart
        chartVersion 1
        initialState Idle
        store (machineStore :> IMachineStore<Entity, TestState, TestEvent, TestAction, TestError>)
        actionQueue TestContext.actionQueue

        processor
            { ProcessorPolicy.defaults with
                Batch = 16
                PollingInterval = TimeSpan.FromMilliseconds 20.
                Lease = TimeSpan.FromSeconds 10.
                RenewAfter = TimeSpan.FromSeconds 5. }

        timeProvider time
    }
    |> function
        | Ok built -> built
        | Error errors -> failtestf "the integration machine did not build: %A" errors

/// Drains until the predicate holds or the budget runs out, which is what a worker loop does.
let private drainUntil
    (processors: CommandProcessor<Entity, TestState, TestEvent, TestAction, TestError> list)
    (until: unit -> bool)
    =
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

let private stateOf (store: PostgresMachineStore<Entity, TestState, TestEvent, TestAction, TestError>) name =
    task {
        let! snapshot = (store :> IStateReader<Entity, TestState>).TryGetSnapshot(machine, entity name, noCancellation)

        return snapshot |> expectOk "snapshot"
    }

let private transitionEpochs name =
    rows
        "SELECT epoch, event FROM fsm.transition WHERE entity_id = @entity ORDER BY epoch"
        [ "entity", box name ]
        (fun reader -> reader.GetInt64 0, reader.GetString 1)

let tests =
    testList
        "Postgres command processor"
        [ testTask "a submitted command is claimed, resolved and committed" {
              do! reset ()
              let store = ownStore ()
              let built = buildMachine store TimeProvider.System
              let! _ = Machine.startAsync built (newRegistry ()) noCancellation

              let! submitted = Machine.enqueue built (entity "e1") (EventEnvelope.create "k1" (Start 5)) noCancellation

              let commandId =
                  match submitted with
                  | Ok(Accepted id) -> id
                  | other -> failtestf "expected an acceptance, got %A" other

              let processor = Machine.processor built
              do! drainUntil [ processor ] (fun () -> (transitionEpochs "e1").Length = 1)

              let! snapshot = stateOf store "e1"
              Expect.equal (Some(Active 5)) (snapshot |> Option.map _.State) "the chart's decision was committed"

              let! result = Machine.commandResult built commandId noCancellation

              match result with
              | Ok(Some(CommandResult.Committed committed)) ->
                  Expect.equal (Epoch.ofUInt64 1UL) committed.Epoch "the first epoch"
                  Expect.equal commandId committed.CommandId "the command it came from"
              | other -> failtestf "expected a committed result, got %A" other

              do! Machine.stopAsync built noCancellation
          }

          testTask "commands for one entity commit in submission order under two processors" {
              // The head invariant is what makes this true: at most one command per entity is
              // claimable, so a second processor cannot take the next one until the first is
              // finished. Neither processor knows the other exists.
              do! reset ()
              let storeA = ownStore ()
              let storeB = ownStore ()
              let machineA = buildMachine storeA TimeProvider.System
              let machineB = buildMachine storeB TimeProvider.System
              let! _ = Machine.startAsync machineA (newRegistry ()) noCancellation
              let! _ = Machine.startAsync machineB (newRegistry ()) noCancellation

              // Ten commands for one entity, each adding its own amount.
              for index in 1..10 do
                  let! _ =
                      Machine.enqueue
                          machineA
                          (entity "ordered")
                          (EventEnvelope.create $"k{index}" (Start index))
                          noCancellation

                  ()

              let processors = [ Machine.processor machineA; Machine.processor machineB ]
              do! drainUntil processors (fun () -> (transitionEpochs "ordered").Length = 10)

              let epochs = transitionEpochs "ordered" |> List.map fst

              Expect.equal [ 1L .. 10L ] epochs "the epoch is gapless and in order"

              let! snapshot = stateOf storeA "ordered"

              // 1 + 2 + ... + 10. Any reordering still sums to 55, but a lost or doubled command
              // does not, and the gapless epoch above is what rules out a reorder.
              Expect.equal (Some(Active 55)) (snapshot |> Option.map _.State) "every command applied exactly once"

              do! Machine.stopAsync machineA noCancellation
              do! Machine.stopAsync machineB noCancellation
          }

          testTask "different entities are processed concurrently" {
              do! reset ()
              let store = ownStore ()
              let built = buildMachine store TimeProvider.System
              let! _ = Machine.startAsync built (newRegistry ()) noCancellation

              for index in 1..8 do
                  let! _ =
                      Machine.enqueue
                          built
                          (entity $"e{index}")
                          (EventEnvelope.create $"k{index}" (Start 1))
                          noCancellation

                  ()

              let processor = Machine.processor built

              // One poll claims one command per entity, so a batch of eight distinct entities is
              // eight commands. That is the whole point of claiming by entity head.
              match! processor.PollAsync noCancellation with
              | Ok report -> Expect.equal 8 report.Summary.Claimed "a single claim took all eight entities"
              | Error error -> failtestf "the poll failed: %A" error

              do! Machine.stopAsync built noCancellation
          }

          testTask "a repeated idempotency key produces one command and one transition" {
              do! reset ()
              let store = ownStore ()
              let built = buildMachine store TimeProvider.System
              let! _ = Machine.startAsync built (newRegistry ()) noCancellation

              let envelope = EventEnvelope.create "same-key" (Start 3)
              let! first = Machine.enqueue built (entity "dup") envelope noCancellation
              let! second = Machine.enqueue built (entity "dup") envelope noCancellation

              match first, second with
              | Ok(Accepted a), Ok(AlreadySubmitted b) -> Expect.equal a b "the second submission found the first"
              | other -> failtestf "expected an acceptance then a duplicate, got %A" other

              do! drainUntil [ Machine.processor built ] (fun () -> (transitionEpochs "dup").Length >= 1)

              Expect.equal 1 (transitionEpochs "dup").Length "one transition, not two"

              do! Machine.stopAsync built noCancellation
          }

          testTask "a skewed clock cannot reorder committed history" {
              // Durable order is the database's, not the application's. A host whose clock runs
              // an hour behind still appends after everything already committed.
              do! reset ()
              let store = ownStore ()
              let skewed = FakeClock(DateTimeOffset.UtcNow.AddHours -1.)
              let built = buildMachine store skewed
              let! _ = Machine.startAsync built (newRegistry ()) noCancellation

              for index in 1..3 do
                  let! _ =
                      Machine.enqueue
                          built
                          (entity "skew")
                          (EventEnvelope.create $"k{index}" (Start index))
                          noCancellation

                  ()

              do! drainUntil [ Machine.processor built ] (fun () -> (transitionEpochs "skew").Length = 3)

              let committedOrder =
                  rows
                      "SELECT epoch FROM fsm.transition WHERE entity_id = @entity ORDER BY committed_at, epoch"
                      [ "entity", box "skew" ]
                      (fun reader -> reader.GetInt64 0)

              Expect.equal [ 1L; 2L; 3L ] committedOrder "committed_at agrees with the epoch the database assigned"

              do! Machine.stopAsync built noCancellation
          }

          testTask "an unhandled event is dead-lettered and releases its entity" {
              do! reset ()
              let store = ownStore ()
              let built = buildMachine store TimeProvider.System
              let! _ = Machine.startAsync built (newRegistry ()) noCancellation

              // Finish has no rule anywhere in this chart, so it can never be handled.
              let! poison = Machine.enqueue built (entity "stuck") (EventEnvelope.create "poison" Finish) noCancellation
              let! _ = Machine.enqueue built (entity "stuck") (EventEnvelope.create "after" (Start 2)) noCancellation

              let poisonId =
                  match poison with
                  | Ok(Accepted id) -> id
                  | other -> failtestf "expected an acceptance, got %A" other

              do! drainUntil [ Machine.processor built ] (fun () -> (transitionEpochs "stuck").Length = 1)

              match! Machine.commandResult built poisonId noCancellation with
              | Ok(Some(CommandResult.DeadLettered(CommandFailure.Machine _))) -> ()
              | other -> failtestf "expected a machine-reason dead letter, got %A" other

              // The command behind it ran, which is the property that matters: a command nobody
              // can process must not wedge its entity forever.
              let! snapshot = stateOf store "stuck"
              Expect.equal (Some(Active 2)) (snapshot |> Option.map _.State) "the next command was released"

              do! Machine.stopAsync built noCancellation
          }

          testTask "a committed transition leaves its actions in the queue" {
              do! reset ()
              let store = ownStore ()
              let built = buildMachine store TimeProvider.System
              let! _ = Machine.startAsync built (newRegistry ()) noCancellation

              let! _ = Machine.enqueue built (entity "effects") (EventEnvelope.create "k1" (Start 1)) noCancellation
              do! drainUntil [ Machine.processor built ] (fun () -> (transitionEpochs "effects").Length = 1)

              let depth = scalar<int64> $"SELECT count(*) FROM pgmq.q_{actionQueue}"
              Expect.equal 1L depth "the chart's action was queued by the commit itself"

              do! Machine.stopAsync built noCancellation
          } ]
