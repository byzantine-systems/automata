module ByzantineSystems.Automata.Storage.Sqlite.Tests.TestContext

open System
open System.IO
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Resilience
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Sqlite
open Expecto
open Microsoft.Data.Sqlite
open Microsoft.Extensions.Logging.Abstractions

let machine = "sqlite-tests"

/// A syntactically valid fingerprint for the test machine's version 1, which the foreign key on
/// every command requires to exist.
let fingerprint = String.replicate 64 "a"

/// One directory per test run, removed when the run ends. Every database file a test creates
/// lives in it, together with the -wal and -shm files WAL keeps beside each one.
let directory =
    Path.Combine(Path.GetTempPath(), $"bs-automata-sqlite-tests-{Guid.NewGuid():N}")

/// The path of a new database file, which does not exist until something opens it.
let freshPath (name: string) : string =
    Directory.CreateDirectory directory |> ignore
    Path.Combine(directory, $"{name}-{Guid.NewGuid():N}.db")

/// Migrates a new file and answers its connection string. A fixture that fails should say so.
let migrated (name: string) : string =
    let connectionString = DataSource.connectionString (freshPath name)

    match Migrator.migrate NullLogger.Instance connectionString with
    | Ok _ -> connectionString
    | Error(MigrationError.Failed(script, error)) ->
        failwith $"the fixture could not migrate at {script}: {error.Message}"
    | Error(MigrationError.Unsupported reason) -> failwith $"the fixture could not migrate: {reason}"

/// The context a store would be given for a connection string, on the default policy.
let contextFor (connectionString: string) : SqliteContext =
    SqliteContext.create connectionString TransientPolicy.defaults ignore TimeProvider.System

let private bind (parameters: (string * obj) list) (cmd: SqliteCommand) =
    for name, value in parameters do
        cmd.Parameters.AddWithValue(name, value) |> ignore

/// Runs a statement for its effect, on a fresh connection.
let exec (connectionString: string) (sql: string) (parameters: (string * obj) list) : unit =
    use conn = new SqliteConnection(connectionString)
    conn.Open()
    use cmd = conn.CreateCommand()
    cmd.CommandText <- sql
    bind parameters cmd
    cmd.ExecuteNonQuery() |> ignore

/// Reads the first column of every row, as text.
let column (connectionString: string) (sql: string) (parameters: (string * obj) list) : string list =
    use conn = new SqliteConnection(connectionString)
    conn.Open()
    use cmd = conn.CreateCommand()
    cmd.CommandText <- sql
    bind parameters cmd
    use reader = cmd.ExecuteReader()

    [ while reader.Read() do
          if reader.IsDBNull 0 then
              "NULL"
          else
              string (reader.GetValue 0) ]

/// Reads a single value, as text.
let scalar (connectionString: string) (sql: string) : string =
    match column connectionString sql [] with
    | [ value ] -> value
    | other -> failwith $"expected one row from {sql}, got {List.length other}"

/// The detail lines of a statement's query plan. Parameters stay unbound, as they are when a
/// store prepares the statement, so the planner cannot fold them into the plan.
let plan (connectionString: string) (sql: string) : string list =
    use conn = new SqliteConnection(connectionString)
    conn.Open()
    use cmd = conn.CreateCommand()
    cmd.CommandText <- "EXPLAIN QUERY PLAN " + sql
    use reader = cmd.ExecuteReader()

    [ while reader.Read() do
          reader.GetString(reader.GetOrdinal "detail") ]

/// The database the schema suites share, migrated once.
let private sharedHolder = lazy (migrated "shared")

let shared () : string = sharedHolder.Force()

/// Empties every data table, children first because foreign keys are enforced, and declares the
/// test machine's chart version again.
///
/// fsm_counter is deliberately left alone. Tokens should keep climbing across tests, as the
/// PostgreSQL fixture leaves fsm.lease_token alone: a test that passes only because the counter
/// was reset is not exercising the fence.
let reset () : unit =
    exec
        (shared ())
        "DELETE FROM fsm_action_dead_letter;
         DELETE FROM fsm_action;
         DELETE FROM fsm_command_error;
         DELETE FROM fsm_command_correction;
         DELETE FROM fsm_entity_snapshot;
         DELETE FROM fsm_transition;
         DELETE FROM fsm_command;
         DELETE FROM fsm_machine_chart_version;
         INSERT INTO fsm_machine_chart_version (machine_id, version, fingerprint, registered_at)
             VALUES (@machine, 1, @fingerprint, 0);"
        [ "@machine", box machine; "@fingerprint", box fingerprint ]

/// Inserts a command row directly, with defaults for every column a test does not name. The
/// schema suites test what the schema refuses, so they write rows no store ever would.
let insertCommand (connectionString: string) (overrides: (string * obj) list) : unit =
    let defaults: (string * obj) list =
        [ "machine_id", box machine
          "entity_id", box "entity-1"
          "seq", box 1L
          "idempotency_key", box "key-1"
          "chart_version", box 1L
          "event", box "{}"
          "kind", box "event"
          "status", box "ready"
          "blocked", box 0L
          "visible_at", box 0L
          "lease_token", box 0L
          "received_at", box 0L ]

    let values =
        defaults
        |> List.map (fun (name, value) ->
            match List.tryFind (fun (overridden, _) -> overridden = name) overrides with
            | Some(_, value) -> name, value
            | None -> name, value)

    let names = values |> List.map fst
    let columns = String.Join(", ", names)
    let placeholders = String.Join(", ", names |> List.map (fun name -> "@" + name))

    exec
        connectionString
        $"INSERT INTO fsm_command ({columns}) VALUES ({placeholders});"
        (values |> List.map (fun (name, value) -> "@" + name, value))

/// The command id of the only command with the given key.
let commandId (connectionString: string) (key: string) : int64 =
    column connectionString "SELECT command_id FROM fsm_command WHERE idempotency_key = @key;" [ "@key", box key ]
    |> List.exactlyOne
    |> int64

/// Runs <paramref name="action" /> and answers the extended result code of the SQLite error it
/// raised, failing the test when it raised none.
let extendedError (action: unit -> unit) : int =
    try
        action ()
        failwith "expected SQLite to refuse the statement"
    with :? SqliteException as error ->
        error.SqliteExtendedErrorCode

/// Extended result codes the schema suites expect.
[<RequireQualifiedAccess>]
module Code =
    let constraintCheck = 275
    let constraintForeignKey = 787
    let constraintTrigger = 1811
    let constraintUnique = 2067
    let readOnly = 8

// ---------------------------------------------------------------------------------------------
// The store suites.
//
// Each test gets its own migrated file rather than a shared one emptied between tests. A file is
// cheap, the tests then run in parallel without stepping on each other, and every store in a test
// shares a clock the test moves by hand, so a lease expires or a backoff elapses without sleeping.
// ---------------------------------------------------------------------------------------------

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
type Actions = IActionQueue<Entity, TestAction>
type Claimed = LeasedCommand<Entity, TestEvent>
type ClaimedAction = LeasedAction<Entity, TestAction>
type Store = SqliteMachineStore<Entity, TestState, TestEvent, TestAction, TestError>

let testMachine: MachineId = machineId machine
let noCancellation = CancellationToken.None
let version = ChartVersion.create 1
let startTime = DateTimeOffset(2026, 9, 19, 12, 0, 0, TimeSpan.Zero)

/// A clock that stands still until the test moves it.
type ManualClock(start: DateTimeOffset) =
    inherit TimeProvider()
    let mutable now = start

    member _.Advance(span: TimeSpan) = now <- now.Add span
    override _.GetUtcNow() = now

/// One migrated file with the test machine's version 1 declared, and the context every store in a
/// test is built from.
type Fixture =
    { Db: string
      Clock: ManualClock
      Context: SqliteContext }

let fixture (name: string) : Fixture =
    let db = migrated name

    exec
        db
        "INSERT INTO fsm_machine_chart_version (machine_id, version, fingerprint, registered_at)
         VALUES (@machine, 1, @fingerprint, 0);"
        [ "@machine", box machine; "@fingerprint", box fingerprint ]

    let clock = ManualClock startTime

    { Db = db
      Clock = clock
      Context = SqliteContext.create db TransientPolicy.defaults ignore clock }

let private entityKey = EntityKey.forEntityId<TestEntity>

let inboxOf (fixture: Fixture) : Inbox =
    let encode, decode = entityKey

    SqliteCommandInbox<Entity, TestEvent>(
        { Context = fixture.Context
          EventCodec = Serialization.systemTextJson<TestEvent> ()
          EntityIdEncode = encode
          EntityIdDecode = decode }
    )
    :> Inbox

let private processorStoreOf (fixture: Fixture) =
    let encode, decode = entityKey

    SqliteCommandProcessorStore<Entity, TestState, TestEvent, TestAction, TestError>(
        { Context = fixture.Context
          StateCodec = Serialization.systemTextJson<TestState> ()
          EventCodec = Serialization.systemTextJson<TestEvent> ()
          ActionCodec = Serialization.systemTextJson<TestAction list> ()
          ErrorCodec = Serialization.systemTextJson<TestError> ()
          EntityIdEncode = encode
          EntityIdDecode = decode }
    )

let processorOf (fixture: Fixture) : Processor = processorStoreOf fixture :> Processor

let readerOf (fixture: Fixture) : Reader = processorStoreOf fixture :> Reader

let actionsOf (fixture: Fixture) : Actions =
    let encode, decode = entityKey

    SqliteActionQueue<Entity, TestAction>(
        { Context = fixture.Context
          ActionCodec = Serialization.systemTextJson<TestAction> ()
          EntityIdEncode = encode
          EntityIdDecode = decode }
    )
    :> Actions

let registryOf (fixture: Fixture) : IChartRegistry =
    SqliteChartRegistry({ Context = fixture.Context }) :> IChartRegistry

let storeOf (context: SqliteContext) : Store =
    MachineStoreOptions.forEntityId<TestEntity, TestState, TestEvent, TestAction, TestError> context
    |> SqliteMachineStore

let submission (entity: Entity) (key: string) (event: TestEvent) : CommandSubmission<Entity, TestEvent> =
    { MachineId = testMachine
      EntityId = entity
      IdempotencyKey = key
      ChartVersion = version
      Kind = CommandKind.Event
      Event = event
      VisibleAt = None
      ReceivedAt = None
      Audit = AuditContext.empty }

/// A minimal committed transition, for tests about what finalize does rather than which chart
/// produced it.
let draft (entity: Entity) (fromState: TestState) (toState: TestState) (effectiveAt: DateTimeOffset) =
    { MachineId = testMachine
      EntityId = entity
      Event = Finish
      Actions = [ Notify "done" ]
      FromState = fromState
      ToState = toState
      HandledBy = stateId "root"
      Exited = [ stateId "idle" ]
      Entered = [ stateId "active" ]
      Status = Running
      EffectiveAt = effectiveAt }

let expectOk label result =
    match result with
    | Ok value -> value
    | Error error -> failtestf "%s: expected Ok, got %A" label error

let commandIdOf (outcome: SubmissionOutcome) : CommandId =
    match outcome with
    | Accepted commandId -> commandId
    | AlreadySubmitted commandId -> commandId

/// Submits one command and answers its id.
let submit (inbox: Inbox) (entity: Entity) (key: string) (event: TestEvent) : Task<CommandId> =
    task {
        let! outcome = inbox.Submit(submission entity key event, noCancellation)
        return outcome |> expectOk $"submit {key}" |> commandIdOf
    }

/// Claims up to a batch of commands with a lease the test does not expect to lapse.
let claim (inbox: Inbox) (batch: int) : Task<Claimed list> =
    task {
        let! claimed = inbox.Claim(testMachine, batch, TimeSpan.FromSeconds 30.0, noCancellation)
        return claimed |> expectOk "claim"
    }

/// Claims every claimable command and answers the one submitted under a key. A claim is per
/// machine rather than per entity, so any other claimable head comes back too.
let claimKey (inbox: Inbox) (key: string) : Task<Claimed> =
    task {
        let! claimed = claim inbox 50

        return
            claimed
            |> List.tryFind (fun held -> held.Work.IdempotencyKey = key)
            |> Option.defaultWith (fun () -> failtestf "the command %s was not claimable" key)
    }

/// Submits one command and claims it, which is where most finalize tests start.
let leased (inbox: Inbox) (entity: Entity) (key: string) : Task<Claimed> =
    task {
        let! _ = submit inbox entity key (Start 1)
        return! claimKey inbox key
    }

/// Releases pooled connections, which hold the files open, and removes the run's directory.
let cleanup () : unit =
    SqliteConnection.ClearAllPools()

    try
        if Directory.Exists directory then
            Directory.Delete(directory, true)
    with :? IOException ->
        ()
