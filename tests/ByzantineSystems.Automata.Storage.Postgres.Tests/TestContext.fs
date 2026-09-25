module ByzantineSystems.Automata.Storage.Postgres.Tests.TestContext

open System
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Npgsql
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Resilience
open ByzantineSystems.Automata.Storage.Postgres
open Expecto

type TestEntity = class end

type TestEvent =
    | Start of int
    | Finish

type TestState =
    | Idle
    | Active of int

type TestAction = Notify of string

type TestError = Refused of reason: string

type Entity = EntityId<TestEntity>
type Inbox = ICommandInbox<Entity, TestEvent>
type Processor = ICommandProcessorStore<Entity, TestState, TestEvent, TestAction, TestError>
type Reader = IStateReader<Entity, TestState, TestEvent, TestAction>

let machine = machineId "pg-tests"
let noCancellation = CancellationToken.None
let version = ChartVersion.create 1

/// A syntactically valid fingerprint for the test machine's version 1.
///
/// The registry suite computes real ones from real charts. Everything else only needs the version
/// to exist, because the foreign key now insists that a command pins a version some chart
/// declared, so registering one is part of a usable empty database.
let fingerprint = String.replicate 64 "a"

/// The queue a commit enqueues its actions into, shared by every test in this suite.
let actionQueue = "automata_test_actions"

let connectionString =
    match Environment.GetEnvironmentVariable "AUTOMATA_TEST_DB" with
    | null -> ""
    | value -> value

let configured = not (String.IsNullOrWhiteSpace connectionString)

/// Built lazily: constructing a data source against an empty connection string throws, and the
/// unit-level suites in this project must run with no database configured at all.
let private dataSourceHolder = lazy (DataSource.create connectionString)

let dataSource () : NpgsqlDataSource = dataSourceHolder.Force()

/// The data source paired with the resilience every call through it runs under. Tests share one,
/// so the circuit breaker sees the whole suite's traffic exactly as an application's would.
let private contextHolder =
    lazy (PostgresContext.create (dataSource ()) TransientPolicy.defaults ignore)

let context () : PostgresContext = contextHolder.Force()

/// Drops the schema and journal and re-applies migrations, for a clean slate.
let migrate () : unit =
    use conn = (dataSource ()).OpenConnection()

    use cmd =
        new NpgsqlCommand("DROP SCHEMA IF EXISTS fsm CASCADE; DROP TABLE IF EXISTS public.schemaversions;", conn)

    cmd.ExecuteNonQuery() |> ignore

    match Migrator.migrate Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance connectionString with
    | Ok _ -> ()
    | Error(MigrationError.Failed(script, error)) ->
        failwithf "the fixture could not migrate at %s: %s" script error.Message

    // The action queue is the application's to create, not the migration's: its name is
    // configuration, and one machine's queue is not another's. An application calls this once at
    // startup; here the fixture stands in for that startup.
    use ensure = new NpgsqlCommand("SELECT fsm.ensure_action_queue(@queue);", conn)
    ensure.Parameters.AddWithValue("queue", actionQueue) |> ignore
    ensure.ExecuteNonQuery() |> ignore

let private migrateOnce = lazy (migrate ())

/// Truncates every table for per-test isolation, then re-declares the test machine's chart
/// version.
///
/// Identities restart so that command ids are predictable within a test, but fsm.lease_token is
/// deliberately left alone: tokens should keep climbing across tests, and a test that passes only
/// because the token counter was reset is a test that is not exercising the fence.
///
/// fsm.command and fsm.machine_chart_version are truncated in one statement because they are
/// related by a foreign key, and PostgreSQL refuses to truncate one without the other unless
/// asked to CASCADE. Listing both says which tables are being emptied; CASCADE would not.
///
/// The registration is a plain insert rather than a call to the registry. A fixture that fails
/// should point at the fixture.
let reset () : Task =
    task {
        migrateOnce.Force()

        use! conn = (dataSource ()).OpenConnectionAsync(noCancellation).AsTask()

        use cmd =
            new NpgsqlCommand(
                // action_dead_letter references fsm.command, and TRUNCATE refuses to leave a
                // foreign key dangling, so it is truncated in the same statement rather than
                // separately.
                "TRUNCATE fsm.command, fsm.command_error, fsm.transition, fsm.machine_chart_version,
                          fsm.supervision_event, fsm.instance_state, fsm.instance_state_history,
                          fsm.action_dead_letter, fsm.machine_maintenance, fsm.belief_cold_state,
                          fsm.command_correction
                          RESTART IDENTITY;
                 -- A materialized view cannot be truncated; refreshed over the emptied history,
                 -- it is empty too.
                 REFRESH MATERIALIZED VIEW fsm.belief_cold;
                 INSERT INTO fsm.machine_chart_version (machine_id, version, fingerprint)
                 VALUES (@machine_id, @version, @fingerprint);
                 -- The queue survives the truncate because it is pgmq's table, not ours.
                 -- Emptying it keeps one test's undelivered actions out of the next one.
                 SELECT pgmq.purge_queue(@queue);
                 -- The archive too, or one test's delivered actions are the next one's purge.
                 DELETE FROM pgmq.a_automata_test_actions;",
                conn
            )

        cmd.Parameters.AddWithValue("machine_id", MachineId.value machine) |> ignore
        cmd.Parameters.AddWithValue("queue", actionQueue) |> ignore
        cmd.Parameters.AddWithValue("version", ChartVersion.value version) |> ignore
        cmd.Parameters.AddWithValue("fingerprint", fingerprint) |> ignore

        do! (cmd.ExecuteNonQueryAsync(noCancellation) :> Task)
    }

let newInbox () : Inbox =
    let encode, decode = EntityKey.forEntityId<TestEntity>

    PostgresCommandInbox<Entity, TestEvent>(
        { Context = context ()
          EventCodec = Serialization.systemTextJson<TestEvent> ()
          EntityIdEncode = encode
          EntityIdDecode = decode }
    )
    :> Inbox

let private processorStore () =
    let encode, decode = EntityKey.forEntityId<TestEntity>

    PostgresCommandProcessorStore<Entity, TestState, TestEvent, TestAction, TestError>(
        { Context = context ()
          ActionQueue = actionQueue
          StateCodec = Serialization.systemTextJson<TestState> ()
          EventCodec = Serialization.systemTextJson<TestEvent> ()
          ActionCodec = Serialization.systemTextJson<TestAction list> ()
          ErrorCodec = Serialization.systemTextJson<TestError> ()
          EntityIdEncode = encode
          EntityIdDecode = decode }
    )

let newProcessor () : Processor = processorStore () :> Processor

let newReader () : Reader = processorStore () :> Reader

/// A minimal committed transition, for tests that care about what finalize does to the inbox and
/// the log rather than about the chart that produced it.
let draft (entity: Entity) (fromState: TestState) (toState: TestState) (effectiveAt: DateTimeOffset) =
    { MachineId = machine
      EntityId = entity
      Event = Finish
      Actions = [ Notify "done" ]
      FromState = fromState
      ToState = toState
      HandledBy = stateId "root"
      Exited = []
      Entered = []
      Status = Running
      EffectiveAt = effectiveAt }

type Actions = IActionQueue<Entity, TestAction>

let private actionQueueStore () =
    let encode, decode = EntityKey.forEntityId<TestEntity>

    PostgresActionQueue<Entity, TestAction>(
        { Context = context ()
          Queue = actionQueue
          ActionCodec = Serialization.systemTextJson<TestAction> ()
          EntityIdEncode = encode
          EntityIdDecode = decode }
    )

let newActionQueue () : Actions = actionQueueStore () :> Actions

type Temporal = ITemporalReader<Entity, TestState>
type Corrections = ICorrectionStore<Entity, TestState>

let private temporalStore () =
    let encode, decode = EntityKey.forEntityId<TestEntity>

    PostgresTemporalStore<Entity, TestState>(
        { Context = context ()
          StateCodec = Serialization.systemTextJson<TestState> ()
          EntityIdEncode = encode
          EntityIdDecode = decode }
    )

let newTemporalReader () : Temporal = temporalStore () :> Temporal

let newCorrections () : Corrections = temporalStore () :> Corrections

let newRegistry () : IChartRegistry =
    PostgresChartRegistry({ Context = context () }) :> IChartRegistry

let submission (entity: Entity) (key: string) (event: TestEvent) : CommandSubmission<Entity, TestEvent> =
    { MachineId = machine
      EntityId = entity
      IdempotencyKey = key
      ChartVersion = version
      Kind = CommandKind.Event
      Event = event
      VisibleAt = None
      ReceivedAt = None
      Audit = AuditContext.empty }

let startTime = DateTimeOffset(2026, 9, 19, 12, 0, 0, TimeSpan.Zero)

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

let commandIdOf (outcome: SubmissionOutcome) : CommandId =
    match outcome with
    | Accepted commandId -> commandId
    | AlreadySubmitted commandId -> commandId

/// Runs a raw statement, returning the failure so a constraint test can inspect it.
let exec (sql: string) : Result<unit, exn> =
    try
        use conn = (dataSource ()).OpenConnection()
        use cmd = new NpgsqlCommand(sql, conn)
        cmd.ExecuteNonQuery() |> ignore
        Ok()
    with ex ->
        Error ex

/// Reads one scalar, for assertions about rows the contract deliberately does not expose.
let scalar<'T> (sql: string) : 'T =
    use conn = (dataSource ()).OpenConnection()
    use cmd = new NpgsqlCommand(sql, conn)
    cmd.ExecuteScalar() :?> 'T

/// Every nullable column of a table in the fsm schema. Must stay empty: absence in this schema is always a
/// value, never NULL, so that a CHECK constraint cannot pass by being unknown.
let nullableColumns () : (string * string) list =
    use conn = (dataSource ()).OpenConnection()

    use cmd =
        new NpgsqlCommand(
            // Base tables only. A view cannot carry NOT NULL, so every view column reports
            // nullable whatever the tables under it say; the rule is about what is stored.
            "SELECT c.table_name, c.column_name FROM information_schema.columns c JOIN information_schema.tables t ON t.table_schema = c.table_schema AND t.table_name = c.table_name WHERE c.table_schema = 'fsm' AND t.table_type = 'BASE TABLE' AND c.is_nullable = 'YES' ORDER BY c.table_name, c.column_name",
            conn
        )

    use reader = cmd.ExecuteReader()

    let rec go acc =
        if reader.Read() then
            go ((reader.GetString 0, reader.GetString 1) :: acc)
        else
            List.rev acc

    go []

/// Rows reported by the shipped drift view. The view is read rather than a re-statement of it,
/// so the test cannot pass against a query the library does not ship.
let blockedDriftRows () : string list =
    use conn = (dataSource ()).OpenConnection()

    use cmd =
        new NpgsqlCommand(
            "SELECT command_id, machine_id, entity_id, seq, blocked, expected_blocked FROM fsm.blocked_drift ORDER BY machine_id, entity_id, seq",
            conn
        )

    use reader = cmd.ExecuteReader()

    let rec go acc =
        if reader.Read() then
            let description =
                $"command {reader.GetInt64 0} of {reader.GetString 2}: blocked={reader.GetBoolean 4}, expected={reader.GetBoolean 5}"

            go (description :: acc)
        else
            List.rev acc

    go []

/// Runs a shipped statement with named parameters, projecting each row.
///
/// Lets a test execute the file the library actually embeds rather than a restatement of it, the
/// way blockedDriftRows does, without having to rewrite the statement to fit the test.
let rows (sql: string) (parameters: (string * obj) list) (project: NpgsqlDataReader -> 'T) : 'T list =
    use conn = (dataSource ()).OpenConnection()
    use cmd = new NpgsqlCommand(sql, conn)

    for name, value in parameters do
        cmd.Parameters.AddWithValue(name, value) |> ignore

    use reader = cmd.ExecuteReader()

    let rec go acc =
        if reader.Read() then
            go (project (reader :?> NpgsqlDataReader) :: acc)
        else
            List.rev acc

    go []

/// The column names a query actually returns, for asserting the reader's contract.
let columnsOf (sql: string) (parameters: (string * obj) list) : string list =
    use conn = (dataSource ()).OpenConnection()
    use cmd = new NpgsqlCommand(sql, conn)

    for name, value in parameters do
        cmd.Parameters.AddWithValue(name, value) |> ignore

    use reader = cmd.ExecuteReader()
    [ for ordinal in 0 .. reader.FieldCount - 1 -> reader.GetName ordinal ]

/// <summary>
/// A clock fixed at a chosen instant, for proving that the application's clock decides nothing
/// about durable order. Every timestamp that matters is stamped by the database.
/// </summary>
type FakeClock(at: DateTimeOffset) =
    inherit TimeProvider()
    override _.GetUtcNow() = at

/// Lays a belief down through the ordinary forward path, which is the only writer that exists
/// outside a correction. The fixture calls the routine rather than the store, so a reader test
/// fails on the reader rather than on whatever wrote the row.
let believe (name: string) (at: DateTimeOffset) (state: TestState) (epoch: int64) : unit =
    let encoded =
        match (Serialization.systemTextJson<TestState> ()).Encode state with
        | Ok json -> json
        | Error error -> failtestf "the fixture could not encode a state: %A" error

    exec
        $"SELECT fsm.close_and_open('{MachineId.value machine}', '{name}', '{at:o}'::timestamptz, '{encoded}'::jsonb, 'running', {epoch}, 1, 1)"
    |> expectOkUnit "close_and_open"

/// The whole store, as an application builds it, with the retention the test asks for.
let newMachineStore (retention: RetentionPolicy) =
    let encode, decode = EntityKey.forEntityId<TestEntity>

    PostgresMachineStore<Entity, TestState, TestEvent, TestAction, TestError>(
        { Context = context ()
          ActionQueue = actionQueue
          StateCodec = Serialization.systemTextJson<TestState> ()
          EventCodec = Serialization.systemTextJson<TestEvent> ()
          ActionCodec = Serialization.systemTextJson<TestAction> ()
          ErrorCodec = Serialization.systemTextJson<TestError> ()
          EntityIdEncode = encode
          EntityIdDecode = decode
          Retention = retention
          ReapAfter = TimeSpan.FromMinutes 5.
          Listener = ListenerConnection.SameDataSource }
    )

let newMaintenance () : IDatabaseMaintenance =
    PostgresMaintenance(context ()) :> IDatabaseMaintenance

/// Boots the store, which is what registers the machine for maintenance.
let boot (retention: RetentionPolicy) : Task<unit> =
    task {
        let store = newMachineStore retention :> IStoreBoot
        let! report = store.Boot(machine, noCancellation)

        match report with
        | Ok BootReport.Ready -> ()
        | other -> failtestf "the store should have booted, but answered %A" other
    }

/// Runs a statement that must succeed, for arranging rows the contract does not let a test write.
let arrange (sql: string) : unit =
    match exec sql with
    | Ok() -> ()
    | Error error -> failtestf "arranging failed: %s" error.Message

/// The JSON plan of a statement, planned but not run.
let explain (sql: string) : string =
    use conn = (dataSource ()).OpenConnection()
    use cmd = new NpgsqlCommand($"EXPLAIN (FORMAT JSON) {sql}", conn)
    cmd.ExecuteScalar() |> string

/// Every (node type, relation) pair in a JSON plan, walked rather than string-matched, so a
/// sequential scan of a small registry table is not mistaken for one of the inbox.
let scans (plan: string) : (string * string) list =
    let rec walk (node: JsonElement) =
        [ let kind = node.GetProperty("Node Type").GetString()

          match node.TryGetProperty "Relation Name" with
          | true, relation -> kind, relation.GetString()
          | _ -> ()

          match node.TryGetProperty "Plans" with
          | true, children ->
              for child in children.EnumerateArray() do
                  yield! walk child
          | _ -> () ]

    use document = JsonDocument.Parse plan
    walk (document.RootElement[0].GetProperty "Plan")
