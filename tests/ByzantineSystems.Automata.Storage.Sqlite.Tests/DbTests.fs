module ByzantineSystems.Automata.Storage.Sqlite.Tests.DbTests

open System
open System.IO
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
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

let private transactionTests =
    testList
        "writeTransaction"
        [ testTask "commits on Ok" {
              let context = contextFor (scratch "commit")

              let! outcome =
                  Db.writeTransaction
                      context.ConnectionString
                      context.Clock
                      (fun conn transaction _ ct ->
                          task {
                              do! increment conn transaction ct
                              return Ok()
                          })
                      CancellationToken.None

              Expect.equal outcome (Ok()) "the work succeeded"
              Expect.equal (scalar context.ConnectionString "SELECT n FROM counter;") "1" "and its write is durable"
          }

          testTask "rolls back on Error" {
              let context = contextFor (scratch "rollback")

              let! outcome =
                  Db.writeTransaction
                      context.ConnectionString
                      context.Clock
                      (fun conn transaction _ ct ->
                          task {
                              do! increment conn transaction ct
                              return Error(StoreError.NotFound "anything")
                          })
                      CancellationToken.None

              Expect.isError outcome "the work refused"
              Expect.equal (scalar context.ConnectionString "SELECT n FROM counter;") "0" "and left nothing behind"
          }

          testTask "reads the clock once, before the work" {
              let start = DateTimeOffset(2026, 9, 25, 10, 0, 0, TimeSpan.Zero)
              let clock = SteppingClock(start)

              let context =
                  { contextFor (scratch "clock") with
                      Clock = clock }

              let! outcome =
                  Db.writeTransaction
                      context.ConnectionString
                      context.Clock
                      (fun _ _ now _ -> Task.FromResult(Ok now))
                      CancellationToken.None

              Expect.equal outcome (Ok(Instant.ofDateTimeOffset start)) "the work sees the first reading"
              Expect.equal clock.Reads 1 "and there is only one"
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

              let write () =
                  Db.writeTransaction
                      context.ConnectionString
                      context.Clock
                      (fun conn transaction _ ct ->
                          task {
                              do! increment conn transaction ct
                              do! Task.Delay(50, ct)
                              return Ok()
                          })
                      CancellationToken.None

              let! outcomes =
                  Array.init writers (fun _ -> Task.Run<Result<unit, StoreError>>(write))
                  |> Task.WhenAll

              Expect.all outcomes (fun outcome -> outcome = Ok()) "every writer committed"
              Expect.equal (scalar context.ConnectionString "SELECT n FROM counter;") (string writers) "none was lost"
          } ]

[<Tests>]
let tests =
    testList "Db" [ instantTests; classificationTests; transactionTests; gateTests ]
