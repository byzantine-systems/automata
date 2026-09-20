module ByzantineSystems.Automata.Storage.Postgres.Tests.TestContext

open System
open System.Threading
open System.Threading.Tasks
open Npgsql
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Postgres
open Expecto

type TestEntity = class end

type TestState =
    | Idle
    | Active of int
    | Done

type TestEvent =
    | Start of int
    | Finish

type TestAction = Log of string

type Entity = EntityId<TestEntity>
type PgStore = PostgresStore<Entity, TestState, TestEvent, TestAction>

let startTime = DateTimeOffset(2026, 9, 19, 12, 0, 0, TimeSpan.Zero)

/// Deterministic clock; the store takes all timestamps from it.
type FakeTime(start: DateTimeOffset) =
    inherit TimeProvider()
    let mutable current = start

    override _.GetUtcNow() : DateTimeOffset = current
    member _.Advance(span: TimeSpan) = current <- current.Add span

let machine = machineId "pg-tests"
let noCancellation = CancellationToken.None

let private statePath (state: TestState) : string[] =
    match state with
    | Idle -> [| "idle" |]
    | Active _ -> [| "active" |]
    | Done -> [| "done" |]

let connectionString =
    match Environment.GetEnvironmentVariable "AUTOMATA_TEST_DB" with
    | null -> ""
    | value -> value

let private dataSource = NpgsqlDataSource.Create(connectionString)

/// Drops the schema and journal and re-applies migrations, for a clean slate.
let migrate () : unit =
    use conn = dataSource.OpenConnection()

    use cmd =
        new NpgsqlCommand("DROP SCHEMA IF EXISTS fsm CASCADE; DROP TABLE IF EXISTS public.schemaversions;", conn)

    cmd.ExecuteNonQuery() |> ignore

    Migrator.migrate connectionString

let private migrateOnce = lazy (migrate ())

/// Truncates every table and restarts identities, for per-test isolation.
let reset () : Task =
    task {
        migrateOnce.Force()

        use! conn = dataSource.OpenConnectionAsync(noCancellation).AsTask()

        use cmd =
            new NpgsqlCommand(
                """TRUNCATE fsm.transition, fsm.outbox, fsm.retry_queue, fsm.dead_letter, fsm.supervision_event,
                             fsm.machine_chart_version, fsm.instance, fsm.machine
                   RESTART IDENTITY CASCADE""",
                conn
            )

        do! (cmd.ExecuteNonQueryAsync(noCancellation) :> Task)
    }

let newStore (time: FakeTime) : PgStore =
    let options =
        { DataSource = dataSource
          StateCodec = Serialization.systemTextJson<TestState> ()
          EventCodec = Serialization.systemTextJson<TestEvent> ()
          ActionCodec = Serialization.systemTextJson<TestAction> ()
          ActionListCodec = Serialization.systemTextJsonList<TestAction> ()
          EntityIdEncode = EntityId.value
          EntityIdDecode = EntityId.create
          StatePath = statePath
          TimeProvider = time }

    PostgresStore(options)

let newTime () = FakeTime(startTime)

let mapTask (mapping: 'T -> 'U) (source: Task<'T>) : Task<'U> =
    task {
        let! value = source
        return mapping value
    }

let expectOk label result =
    match result with
    | Ok value -> value
    | Error error -> failtestf "%s: expected Ok, got %A" label error

let expectOkUnit label result =
    match result with
    | Ok() -> ()
    | Error error -> failtestf "%s: expected Ok, got %A" label error

let mkTransition
    (entity: Entity)
    (key: string)
    (epoch: Epoch)
    (fromState: TestState)
    (toState: TestState)
    (actions: TestAction list)
    (status: InstanceStatus)
    (occurredAt: DateTimeOffset)
    : Transition<Entity, TestState, TestEvent, TestAction> =
    { MachineId = machine
      EntityId = entity
      IdempotencyKey = key
      OccurredAt = occurredAt
      Epoch = epoch
      Event = Start 1
      Actions = actions
      FromState = fromState
      ToState = toState
      Status = status
      HandledBy = stateId "idle"
      Exited = []
      Entered = [] }

let mkRequest (entity: Entity) (key: string) (nextAttemptAt: DateTimeOffset) : RetryRequest<Entity, TestEvent> =
    { MachineId = machine
      EntityId = entity
      IdempotencyKey = key
      Event = Finish
      NextAttemptAt = nextAttemptAt
      LastError = Some "transient failure" }

/// Runs a raw SQL statement synchronously, returning the failure for constraint tests.
let exec (sql: string) : Result<unit, exn> =
    try
        use conn = dataSource.OpenConnection()
        use cmd = new NpgsqlCommand(sql, conn)
        cmd.ExecuteNonQuery() |> ignore
        Ok()
    with ex ->
        Error ex

/// Returns every nullable column in the fsm schema (should be empty).
let nullableColumns () : (string * string) list =
    use conn = dataSource.OpenConnection()

    use cmd =
        new NpgsqlCommand(
            "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'fsm' AND is_nullable = 'YES'",
            conn
        )

    use reader = cmd.ExecuteReader()

    let rec go acc =
        if reader.Read() then
            go ((reader.GetString 0, reader.GetString 1) :: acc)
        else
            List.rev acc

    go []
