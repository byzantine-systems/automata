module ByzantineSystems.Automata.Storage.Sqlite.Tests.DbTests

open System
open System.IO
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage.Internal
open ByzantineSystems.Automata.Storage.Sqlite
open ByzantineSystems.Automata.Storage.Sqlite.Tests.TestContext
open Microsoft.Data.Sqlite
open Expecto

/// A file with one counter row and no store schema. The boundary helpers need a database, not
/// this store's tables, and a table of their own keeps them independent of the migrations.
let private scratch (name: string) : string =
    let connectionString = DataSource.connectionString (freshPath name)

    exec
        connectionString
        "PRAGMA journal_mode = WAL;
         CREATE TABLE counter (n INTEGER NOT NULL) STRICT;
         INSERT INTO counter (n) VALUES (0);"
        []

    connectionString

/// A clock that moves one second every time it is read, so a test can count the reads.
type private SteppingClock(start: DateTimeOffset) =
    inherit TimeProvider()
    let mutable reads = 0

    member _.Reads = reads

    override _.GetUtcNow() =
        let now = start.AddSeconds(float reads)
        reads <- reads + 1
        now

let private increment (conn: SqliteConnection) (transaction: SqliteTransaction) (ct: CancellationToken) =
    task {
        use cmd = conn.CreateCommand()
        cmd.Transaction <- transaction
        cmd.CommandText <- "UPDATE counter SET n = n + 1;"
        let! _ = cmd.ExecuteNonQueryAsync(ct)
        return ()
    }

let private instantTests =
    testList
        "Instant"
        [ testCase "round-trips to the microsecond"
          <| fun _ ->
              let value =
                  DateTimeOffset(2026, 9, 25, 10, 0, 0, TimeSpan.Zero).AddTicks(1_234_560L)

              Expect.equal
                  (Instant.toDateTimeOffset (Instant.ofDateTimeOffset value))
                  value
                  "an exact microsecond survives"

          testCase "drops sub-microsecond ticks rather than rounding them"
          <| fun _ ->
              let value =
                  DateTimeOffset(2026, 9, 25, 10, 0, 0, TimeSpan.Zero).AddTicks(1_234_567L)

              let back = Instant.toDateTimeOffset (Instant.ofDateTimeOffset value)
              Expect.equal back (value.AddTicks(-7L)) "the remainder is truncated"

          testCase "never moves an instant before the epoch later"
          <| fun _ ->
              let value = DateTimeOffset.UnixEpoch.AddTicks(-15L)
              let back = Instant.toDateTimeOffset (Instant.ofDateTimeOffset value)
              Expect.isLessThanOrEqual back value "flooring, not truncation toward zero"
              Expect.equal back (DateTimeOffset.UnixEpoch.AddTicks(-20L)) "floored to the microsecond below"

          testCase "the sentinels are the ends of DateTimeOffset"
          <| fun _ ->
              Expect.equal
                  (Instant.toDateTimeOffset Instant.negativeInfinity)
                  DateTimeOffset.MinValue
                  "the unset instant is MinValue"

              Expect.equal
                  (Instant.toDateTimeOffset Instant.infinity)
                  (DateTimeOffset.MaxValue.AddTicks(-9L))
                  "the open bound is MaxValue, to the microsecond"

          testCase "an open bound reads as None"
          <| fun _ ->
              Expect.isNone (Instant.openEnded Instant.infinity) "infinity is open"
              Expect.isSome (Instant.openEnded 0L) "the epoch is an instant"

          testCase "adding saturates at the open bound"
          <| fun _ ->
              Expect.equal (Instant.add Instant.infinity (TimeSpan.FromDays 1.)) Instant.infinity "no overflow"
              Expect.equal (Instant.add 0L (TimeSpan.FromSeconds 1.)) 1_000_000L "a second is a million microseconds" ]

let private classificationTests =
    testList
        "classification"
        [ testCase "a second writer is refused with a transient BUSY"
          <| fun _ ->
              let connectionString = scratch "busy"

              use holder = new SqliteConnection(connectionString)
              holder.Open()
              use held = holder.BeginTransaction(deferred = false)

              // A one-second busy timeout, so the test waits one second rather than three.
              let impatient =
                  SqliteConnectionStringBuilder(connectionString, DefaultTimeout = 1).ToString()

              use contender = new SqliteConnection(impatient)
              contender.Open()

              let error =
                  Expect.throwsC (fun () -> contender.BeginTransaction(deferred = false) |> ignore) (fun error ->
                      error :?> SqliteException)

              Expect.equal error.SqliteErrorCode Db.SqliteBusy "the writer lock is held"
              Expect.isTrue (Db.isTransient error) "contention is worth retrying"
              Expect.isTrue (Db.isUnavailable error) "and is the store not serving"
              held.Rollback()

          testCase "a write on the read path is a defect, not an outage"
          <| fun _ ->
              let context = contextFor (scratch "readonly")

              let code =
                  extendedError (fun () -> exec context.ReadConnectionString "UPDATE counter SET n = 1;" [])

              Expect.equal code Code.readOnly "a read-only connection refuses the write"

              let error = SqliteException("readonly", Code.readOnly)
              Expect.isFalse (Db.isTransient error) "not retried"
              Expect.isFalse (Db.isUnavailable error) "and left to propagate"

          testCase "an in-memory database is recognised in every spelling"
          <| fun _ ->
              Expect.isTrue (DataSource.isInMemory "Data Source=:memory:") ":memory:"
              Expect.isTrue (DataSource.isInMemory "Data Source=shared;Mode=Memory") "Mode=Memory"
              Expect.isTrue (DataSource.isInMemory "Data Source=") "an empty data source is a temporary database"
              Expect.isFalse (DataSource.isInMemory (DataSource.connectionString "store.db")) "a file" ]

/// A unit that bumps the counter inside the session's transaction, then answers with
/// <paramref name="answer" />. Built from the driver directly, because the counter table is this
/// suite's own and has no embedded statement.
let private bumpThen (answer: Result<'T, StoreError>) : Op<WriteSession, 'T> =
    Op.ofDriver (fun session ct ->
        task {
            do! increment session.Connection session.Transaction ct
            return answer
        })

let private transactionTests =
    testList
        "Sql.write"
        [ testTask "commits on Ok" {
              let context = contextFor (scratch "commit")
              let! outcome = Sql.write context (bumpThen (Ok())) CancellationToken.None
              Expect.equal outcome (Ok()) "the work succeeded"
              Expect.equal (scalar context.ConnectionString "SELECT n FROM counter;") "1" "and its write is durable"
          }

          testTask "rolls back on Error" {
              let context = contextFor (scratch "rollback")

              let! outcome = Sql.write context (bumpThen (Error(StoreError.NotFound "anything"))) CancellationToken.None

              Expect.isError outcome "the work refused"
              Expect.equal (scalar context.ConnectionString "SELECT n FROM counter;") "0" "and left nothing behind"
          }

          testTask "an outcome that is a refusal but not a failure still commits" {
              let context = contextFor (scratch "refusal")
              let! outcome = Sql.write context (bumpThen (Ok "lease_lost")) CancellationToken.None
              Expect.equal outcome (Ok "lease_lost") "the outcome is reported"
              Expect.equal (scalar context.ConnectionString "SELECT n FROM counter;") "1" "and what it wrote stays"
          }

          testTask "reads the clock once, before the work" {
              let start = DateTimeOffset(2026, 9, 25, 10, 0, 0, TimeSpan.Zero)
              let clock = SteppingClock(start)

              let context =
                  { contextFor (scratch "clock") with
                      Clock = clock }

              let! outcome = Sql.write context Sql.now CancellationToken.None

              Expect.equal outcome (Ok(Instant.ofDateTimeOffset start)) "the work sees the first reading"
              Expect.equal clock.Reads 1 "and there is only one"
          }

          testTask "a transient failure runs the whole unit again, with a new now" {
              let clock = SteppingClock(DateTimeOffset(2026, 9, 25, 10, 0, 0, TimeSpan.Zero))

              let context =
                  { contextFor (scratch "retry") with
                      Clock = clock }

              let mutable attempts = 0

              // The driver's contention error, raised by the first attempt only, after its write:
              // what a retry must not keep is that write.
              let flaky =
                  Op.ofDriver (fun (session: WriteSession) ct ->
                      task {
                          attempts <- attempts + 1
                          do! increment session.Connection session.Transaction ct

                          if attempts = 1 then
                              raise (SqliteException("busy", Db.SqliteBusy))

                          return Ok session.Now
                      })

              let! outcome = Sql.write context flaky CancellationToken.None

              Expect.equal attempts 2 "the unit ran again"
              Expect.equal clock.Reads 2 "in a new transaction, which read the clock again"
              Expect.isOk outcome "and the second attempt committed"
              Expect.equal (scalar context.ConnectionString "SELECT n FROM counter;") "1" "only its write survived"
          }

          testTask "writeOnce makes one attempt and reports the outage" {
              let context = contextFor (scratch "once")
              let mutable attempts = 0

              let busy =
                  Op.ofDriver (fun (_: WriteSession) _ ->
                      attempts <- attempts + 1
                      raise (SqliteException("busy", Db.SqliteBusy)))

              let! outcome = Sql.writeOnce context busy CancellationToken.None

              Expect.equal attempts 1 "never retried"

              match outcome with
              | Error(StoreError.Unavailable _) -> ()
              | other -> failtestf "contention should read as unavailable, got %A" other
          }

          testTask "a failure the store does not recognise is Unexpected, and rolls back" {
              let context = contextFor (scratch "unexpected")

              let broken =
                  Op.ofDriver (fun (session: WriteSession) ct ->
                      task {
                          do! increment session.Connection session.Transaction ct
                          return raise (InvalidOperationException "a defect")
                      })

              let! outcome = Sql.write context broken CancellationToken.None

              match outcome with
              | Error(StoreError.Unexpected(:? InvalidOperationException)) -> ()
              | other -> failtestf "a defect should read as unexpected, got %A" other

              Expect.equal (scalar context.ConnectionString "SELECT n FROM counter;") "0" "nothing it wrote survives"
          } ]

let private primitiveTests =
    let journal = Statement.load "system" "journal"
    let byVersion = Statement.load "chart" "by_version"

    let fingerprintOf (version: int64) =
        Sql.one
            "ChartFingerprint"
            byVersion
            [ "@machine_id", Param.Text machine; "@version", Param.Integer version ]
            (fun reader -> Row.string reader "fingerprint")

    testList
        "Sql primitives"
        [ testTask "one refuses a statement that returns no row" {
              let context = contextFor (migrated "one-none")
              let! outcome = Sql.write context (fingerprintOf 99L) CancellationToken.None

              match outcome with
              | Error(StoreError.Serialization(_, error)) ->
                  Expect.stringContains error.Message "chart/by_version returned no row" "names the statement"
              | other -> failtestf "expected a decode failure, got %A" other
          }

          testTask "one refuses a statement that returns more than one row" {
              let context = contextFor (migrated "one-many")

              let! outcome =
                  Sql.write
                      context
                      (Sql.one "Script" journal [] (fun reader -> Row.string reader "script_name"))
                      CancellationToken.None

              match outcome with
              | Error(StoreError.Serialization(_, error)) ->
                  Expect.stringContains error.Message "system/journal returned more than one row" "names the statement"
              | other -> failtestf "expected a decode failure, got %A" other
          }

          testTask "rows reads every row, in order" {
              let context = contextFor (migrated "rows")

              let! outcome =
                  Sql.write
                      context
                      (Sql.rows journal [] (fun reader -> Row.string reader "script_name"))
                      CancellationToken.None

              match outcome with
              | Ok scripts -> Expect.equal scripts (List.sort scripts) "in the statement's order"
              | Error error -> failtestf "expected the journal, got %A" error
          }

          testTask "a null text is refused as the defect it is, not sent as NULL" {
              let context = contextFor (migrated "null-text")

              let lookup =
                  Sql.tryOne
                      "ChartFingerprint"
                      byVersion
                      [ "@machine_id", Param.Text null; "@version", Param.Integer 1L ]
                      (fun reader -> Row.string reader "fingerprint")

              let! outcome = Sql.write context lookup CancellationToken.None

              match outcome with
              | Error(StoreError.Unexpected(:? ArgumentNullException)) -> ()
              | other -> failtestf "expected the null to be refused, got %A" other
          }

          testTask "a read sees one snapshot, even when a writer commits between its statements" {
              let connectionString = scratch "snapshot"
              let context = contextFor connectionString

              let counterNow =
                  Sql.select (fun query ct ->
                      task {
                          use cmd = query.Connection.CreateCommand()
                          cmd.CommandText <- "SELECT n FROM counter;"

                          match query.Transaction with
                          | Some transaction -> cmd.Transaction <- (transaction :?> SqliteTransaction)
                          | None -> ()

                          let! value = cmd.ExecuteScalarAsync ct
                          return unbox<int64> value
                      })

              let writeBetween =
                  Sql.select (fun _ _ ->
                      exec connectionString "UPDATE counter SET n = n + 1;" []
                      Task.FromResult())

              let! outcome =
                  Sql.read
                      context
                      (sqliteRead {
                          let! before = counterNow
                          do! writeBetween
                          let! after = counterNow
                          return before, after
                      })
                      CancellationToken.None

              Expect.equal outcome (Ok(0L, 0L)) "the second statement saw the first one's snapshot"
              Expect.equal (scalar connectionString "SELECT n FROM counter;") "1" "while the write did commit"
          } ]

let private gateTests =
    testList
        "WriteGate"
        [ testCase "two spellings of one file share a gate"
          <| fun _ ->
              let relative = "Data Source=gate-probe.db"

              let absolute =
                  DataSource.connectionString (Path.Combine(Environment.CurrentDirectory, "gate-probe.db"))

              Expect.equal (WriteGate.keyOf relative) (WriteGate.keyOf absolute) "one key"

              Expect.isTrue
                  (Object.ReferenceEquals(
                      WriteGate.forConnectionString relative,
                      WriteGate.forConnectionString absolute
                  ))
                  "one semaphore"

              Expect.isTrue
                  (Object.ReferenceEquals(
                      WriteGate.forConnectionString absolute,
                      WriteGate.forConnectionString (DataSource.readOnly absolute)
                  ))
                  "the mode does not split the gate"

          testTask "concurrent writers queue instead of contending" {
              // Fifty writers, each holding the lock for 50 ms, against a one-second busy timeout:
              // two and a half seconds of work that no single SQLite wait can cover. Measured
              // without the gate, forty of the fifty fail with SQLITE_BUSY. With it, they queue
              // on the gate, where the wait has no timeout, and none reaches SQLite's lock held.
              let impatient =
                  SqliteConnectionStringBuilder(scratch "gate", DefaultTimeout = 1).ToString()

              let context = contextFor impatient
              let writers = 50

              let slowBump =
                  Op.ofDriver (fun (session: WriteSession) ct ->
                      task {
                          do! increment session.Connection session.Transaction ct
                          do! Task.Delay(50, ct)
                          return Ok()
                      })

              let write () =
                  Sql.write context slowBump CancellationToken.None

              let! outcomes =
                  Array.init writers (fun _ -> Task.Run<Result<unit, StoreError>>(write))
                  |> Task.WhenAll

              Expect.all outcomes (fun outcome -> outcome = Ok()) "every writer committed"
              Expect.equal (scalar context.ConnectionString "SELECT n FROM counter;") (string writers) "none was lost"
          } ]

[<Tests>]
let tests =
    testList
        "Db"
        [ instantTests
          classificationTests
          transactionTests
          primitiveTests
          gateTests ]
