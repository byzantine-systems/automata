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

type TestEvent =
    | Start of int
    | Finish

type Entity = EntityId<TestEntity>
type Inbox = ICommandInbox<Entity, TestEvent>

let machine = machineId "pg-tests"
let noCancellation = CancellationToken.None
let version = ChartVersion.create 1

/// A syntactically valid fingerprint for the test machine's version 1.
///
/// The registry suite computes real ones from real charts. Everything else only needs the version
/// to exist, because the foreign key now insists that a command pins a version some chart
/// declared, so registering one is part of a usable empty database.
let fingerprint = String.replicate 64 "a"

let connectionString =
    match Environment.GetEnvironmentVariable "AUTOMATA_TEST_DB" with
    | null -> ""
    | value -> value

let configured = not (String.IsNullOrWhiteSpace connectionString)

/// Built lazily: constructing a data source against an empty connection string throws, and the
/// unit-level suites in this project must run with no database configured at all.
let private dataSourceHolder = lazy (DataSource.create connectionString)

let dataSource () : NpgsqlDataSource = dataSourceHolder.Force()

/// Drops the schema and journal and re-applies migrations, for a clean slate.
let migrate () : unit =
    use conn = (dataSource ()).OpenConnection()

    use cmd =
        new NpgsqlCommand("DROP SCHEMA IF EXISTS fsm CASCADE; DROP TABLE IF EXISTS public.schemaversions;", conn)

    cmd.ExecuteNonQuery() |> ignore

    Migrator.migrate connectionString

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
                "TRUNCATE fsm.command, fsm.machine_chart_version, fsm.supervision_event RESTART IDENTITY;
                 INSERT INTO fsm.machine_chart_version (machine_id, version, fingerprint)
                 VALUES (@machine_id, @version, @fingerprint);",
                conn
            )

        cmd.Parameters.AddWithValue("machine_id", MachineId.value machine) |> ignore
        cmd.Parameters.AddWithValue("version", ChartVersion.value version) |> ignore
        cmd.Parameters.AddWithValue("fingerprint", fingerprint) |> ignore

        do! (cmd.ExecuteNonQueryAsync(noCancellation) :> Task)
    }

let newInbox () : Inbox =
    let encode, decode = EntityKey.forEntityId<TestEntity>

    PostgresCommandInbox<Entity, TestEvent>(
        { DataSource = dataSource ()
          EventCodec = Serialization.systemTextJson<TestEvent> ()
          EntityIdEncode = encode
          EntityIdDecode = decode }
    )
    :> Inbox

let newRegistry () : IChartRegistry =
    PostgresChartRegistry({ DataSource = dataSource () }) :> IChartRegistry

let submission (entity: Entity) (key: string) (event: TestEvent) : CommandSubmission<Entity, TestEvent> =
    { MachineId = machine
      EntityId = entity
      IdempotencyKey = key
      ChartVersion = version
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

/// Every nullable column in the fsm schema. Must stay empty: absence in this schema is always a
/// value, never NULL, so that a CHECK constraint cannot pass by being unknown.
let nullableColumns () : (string * string) list =
    use conn = (dataSource ()).OpenConnection()

    use cmd =
        new NpgsqlCommand(
            "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'fsm' AND is_nullable = 'YES' ORDER BY table_name, column_name",
            conn
        )

    use reader = cmd.ExecuteReader()

    let rec go acc =
        if reader.Read() then
            go ((reader.GetString 0, reader.GetString 1) :: acc)
        else
            List.rev acc

    go []

/// Rows reported by the shipped drift-detection query. The real file is used rather than a
/// re-statement of it, so the test cannot pass against a query the library does not ship.
let blockedDriftRows () : string list =
    use conn = (dataSource ()).OpenConnection()
    use cmd = new NpgsqlCommand(SqlResources.get "command" "blocked_drift", conn)
    use reader = cmd.ExecuteReader()

    let rec go acc =
        if reader.Read() then
            let description =
                $"command {reader.GetInt64 0} of {reader.GetString 2}: blocked={reader.GetBoolean 4}, expected={reader.GetBoolean 5}"

            go (description :: acc)
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
