namespace ByzantineSystems.Automata.Storage.Sqlite

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Resilience
open Microsoft.Data.Sqlite
open Polly
open Polly.CircuitBreaker
open Polly.Timeout

/// <summary>
/// Instants as the schema stores them: whole microseconds since the Unix epoch, in an
/// <c>INTEGER</c> column.
///
/// Not ISO-8601 text. Text compares correctly only while every value has exactly the same
/// width, and SQLite's own clock stops at milliseconds, where PostgreSQL keeps microseconds. An
/// integer orders exactly, keeps the same precision as the PostgreSQL store, and makes
/// <c>visible_at &lt;= @now</c> an index range rather than a string comparison.
///
/// The schema's rule that absence always has a value holds here too, and the two sentinels are
/// the ends of what <see cref="T:System.DateTimeOffset" /> can represent. They therefore
/// round-trip through the conversions below without a special case, and only
/// <see cref="M:ByzantineSystems.Automata.Storage.Sqlite.Instant.openEnded(System.Int64)" />
/// has to know about them.
/// </summary>
[<RequireQualifiedAccess>]
module internal Instant =

    let private ticksPerMicrosecond = TimeSpan.TicksPerMillisecond / 1000L

    let private epochTicks = DateTimeOffset.UnixEpoch.UtcTicks

    /// <summary>
    /// An instant, floored to the microsecond, so a value never moves later than the one it
    /// came from. Floored rather than divided: integer division truncates toward zero, which
    /// before the epoch would round an instant up.
    /// </summary>
    let ofDateTimeOffset (value: DateTimeOffset) : int64 =
        let ticks = value.UtcTicks - epochTicks

        if ticks >= 0L then
            ticks / ticksPerMicrosecond
        else
            (ticks - (ticksPerMicrosecond - 1L)) / ticksPerMicrosecond

    let toDateTimeOffset (micros: int64) : DateTimeOffset =
        DateTimeOffset(epochTicks + micros * ticksPerMicrosecond, TimeSpan.Zero)

    /// <summary>The open upper bound: <c>DateTimeOffset.MaxValue</c>, to the microsecond.</summary>
    let infinity: int64 = ofDateTimeOffset DateTimeOffset.MaxValue

    /// <summary>The unset instant: <c>DateTimeOffset.MinValue</c>.</summary>
    let negativeInfinity: int64 = ofDateTimeOffset DateTimeOffset.MinValue

    /// <summary>Reads an upper bound as an instant, or <c>None</c> when it is open.</summary>
    let openEnded (micros: int64) : DateTimeOffset option =
        if micros >= infinity then
            None
        else
            Some(toDateTimeOffset micros)

    /// <summary>Adds a duration, to the microsecond, saturating at the open bound.</summary>
    let add (micros: int64) (span: TimeSpan) : int64 =
        let delta = span.Ticks / ticksPerMicrosecond

        if delta > 0L && micros > infinity - delta then
            infinity
        else
            micros + delta

/// <summary>
/// One asynchronous write lock per database file, shared by everything in this process.
///
/// SQLite admits one writer per file. Without this, writers in the same process meet at
/// SQLite's own lock, where the driver waits synchronously: each waiter blocks a thread-pool
/// thread for up to the busy timeout, cannot be cancelled while it waits, and is not served in
/// any particular order. Retrying a writer that lost that wait adds another waiter rather than
/// capacity, so a burst of contention turns into thread-pool starvation long before SQLite has
/// any difficulty.
///
/// Queueing here first changes where the wait happens rather than how much of it there is,
/// since SQLite would serialise these writers anyway. The wait is asynchronous, roughly in
/// arrival order, and honours cancellation, and <c>SQLITE_BUSY</c> is left to mean what the
/// busy timeout and the resilience pipeline are for: another process holding the file.
///
/// Keyed by the file's full path rather than held by a context, so two contexts built for the
/// same file still share one gate. Reads do not take it: WAL lets them run beside the writer.
/// </summary>
[<RequireQualifiedAccess>]
module internal WriteGate =

    let private gates =
        System.Collections.Concurrent.ConcurrentDictionary<string, SemaphoreSlim>(StringComparer.Ordinal)

    /// <summary>
    /// The file a connection string names, as the key its gate is filed under. Relative paths
    /// resolve against the current directory, as SQLite resolves them.
    /// </summary>
    let keyOf (connectionString: string) : string =
        let builder = Microsoft.Data.Sqlite.SqliteConnectionStringBuilder(connectionString)
        IO.Path.GetFullPath builder.DataSource

    /// <summary>The gate for the file a connection string names.</summary>
    let forConnectionString (connectionString: string) : SemaphoreSlim =
        gates.GetOrAdd(keyOf connectionString, fun _ -> new SemaphoreSlim(1, 1))

/// <summary>Shared boundary helpers for the SQLite stores.</summary>
[<RequireQualifiedAccess>]
module internal Db =

    /// <summary>
    /// The primary result codes, which is what a <see cref="T:Microsoft.Data.Sqlite.SqliteException" />
    /// carries in <c>SqliteErrorCode</c>. The extended code is in <c>SqliteExtendedErrorCode</c>.
    /// </summary>
    [<Literal>]
    let SqliteBusy = 5

    [<Literal>]
    let SqliteLocked = 6

    [<Literal>]
    let SqliteIoErr = 10

    [<Literal>]
    let SqliteFull = 13

    [<Literal>]
    let SqliteCantOpen = 14

    /// <summary>
    /// Which driver failures are worth another attempt in a moment.
    ///
    /// Only contention. <c>SQLITE_BUSY</c> and <c>SQLITE_LOCKED</c> mean another connection held
    /// the write lock for longer than the busy timeout, and the same statement will succeed once
    /// it lets go. Everything else SQLite reports is an answer, not an absence of one: a
    /// constraint, a type, a malformed statement. Repeating those produces the same answer.
    ///
    /// There is no dropped socket to retry here. The failures PostgreSQL retries because no
    /// answer arrived do not exist for an embedded database.
    /// </summary>
    let isTransient (error: exn) =
        match error with
        | :? SqliteException as sqliteError ->
            sqliteError.SqliteErrorCode = SqliteBusy
            || sqliteError.SqliteErrorCode = SqliteLocked
        | _ -> false

    /// <summary>
    /// Which driver failures mean the store could not serve, as opposed to a bug in what it was
    /// asked. Contention, and the file itself failing: an I/O error, a full disk, a file that
    /// cannot be opened. A constraint violation is not among them: every write checks what it
    /// depends on before it writes, so a violation that still happens is a defect and is left
    /// to reach the supervision boundary with its stack trace.
    /// </summary>
    let isUnavailable (error: SqliteException) =
        match error.SqliteErrorCode with
        | SqliteBusy
        | SqliteLocked
        | SqliteIoErr
        | SqliteFull
        | SqliteCantOpen -> true
        | _ -> false

    /// <summary>
    /// The one translation from exceptions to results, run once, after the pipeline. Caller
    /// cancellation stays cancellation, and the failures this layer understands become
    /// <c>StoreError.Unavailable</c>. Anything else is left alone.
    /// </summary>
    let private classify
        (work: unit -> Task<Result<'T, StoreError>>)
        (ct: CancellationToken)
        : Task<Result<'T, StoreError>> =
        task {
            try
                return! work ()
            with
            | :? OperationCanceledException when ct.IsCancellationRequested ->
                return! Task.FromCanceled<Result<'T, StoreError>>(ct)
            | :? SqliteException as error when isUnavailable error -> return Error(StoreError.Unavailable error)
            | :? TimeoutRejectedException as error -> return Error(StoreError.Unavailable error)
            | :? BrokenCircuitException as error -> return Error(StoreError.Unavailable error)
        }

    /// <summary>
    /// Runs a unit of work that is safe to repeat, with the pipeline applied.
    ///
    /// The unit is the whole transaction, from opening the connection to committing it, and
    /// never a single statement inside one. SQLite rolls a transaction back when a statement in
    /// it fails with <c>SQLITE_BUSY</c>, so retrying the statement alone would run it outside the
    /// transaction it was written for.
    /// </summary>
    let protect
        (pipeline: ResiliencePipeline)
        (work: CancellationToken -> Task<Result<'T, StoreError>>)
        (ct: CancellationToken)
        : Task<Result<'T, StoreError>> =
        classify (fun () -> ResiliencePipeline.executeTask pipeline work ct) ct

    /// <summary>
    /// Runs a unit of work that must not be repeated automatically, translating failures the
    /// same way but without the pipeline. Claims go through here, as they do in the PostgreSQL
    /// store: a worker that polls again in a moment recovers as well as a retry would, and never
    /// holds a batch it did not ask for twice.
    /// </summary>
    let protectOnce
        (work: CancellationToken -> Task<Result<'T, StoreError>>)
        (ct: CancellationToken)
        : Task<Result<'T, StoreError>> =
        classify (fun () -> work ct) ct

    /// <summary>
    /// Opens a pooled connection, which the caller owns and disposes. Pooling is on in every
    /// connection string this assembly builds.
    ///
    /// Nothing needs releasing when the open fails: the driver leaves the connection closed with
    /// no native handle behind it, so there is no guard here for a leak that cannot happen.
    /// </summary>
    let openConnection (connectionString: string) (ct: CancellationToken) : Task<SqliteConnection> =
        backgroundTask {
            let conn = new SqliteConnection(connectionString)
            do! conn.OpenAsync(ct)
            return conn
        }

    /// <summary>
    /// Runs <paramref name="work" /> inside one write transaction, begun <c>IMMEDIATE</c>, and
    /// commits it when the work answers <c>Ok</c>. An <c>Error</c> rolls back, so a refusal
    /// never leaves half its writes behind.
    ///
    /// <c>IMMEDIATE</c> takes the write lock at <c>BEGIN</c>. A deferred transaction takes it at
    /// the first write, and under WAL a reader that tries to become a writer after another
    /// connection committed fails with <c>SQLITE_BUSY_SNAPSHOT</c>, which no busy timeout can
    /// wait out. Taking the lock first means contention is a wait at the start and never a
    /// failure in the middle.
    ///
    /// <paramref name="now" /> is read once, before the work, and every statement in the
    /// transaction is meant to bind it. That is PostgreSQL's <c>now()</c>, fixed for a whole
    /// transaction, so a commit's timestamps and the lease deadlines it writes agree with each
    /// other.
    ///
    /// Before any of that, it takes the file's <see cref="T:ByzantineSystems.Automata.Storage.Sqlite.WriteGate" />,
    /// so writers in this process queue asynchronously instead of inside SQLite. It is taken
    /// per attempt, inside the pipeline, so a retry's backoff lets the others through. The gate
    /// is not reentrant: <paramref name="work" /> must never begin another write transaction.
    ///
    /// Keep the work short. One file has one writer, and every machine sharing it waits for
    /// this transaction to end, so encoding and decoding happen outside it.
    /// </summary>
    let writeTransaction
        (connectionString: string)
        (clock: TimeProvider)
        (work: SqliteConnection -> SqliteTransaction -> int64 -> CancellationToken -> Task<Result<'T, StoreError>>)
        (ct: CancellationToken)
        : Task<Result<'T, StoreError>> =
        backgroundTask {
            let gate = WriteGate.forConnectionString connectionString
            do! gate.WaitAsync(ct)

            try
                use! conn = openConnection connectionString ct
                use transaction = conn.BeginTransaction(deferred = false)
                let now = Instant.ofDateTimeOffset (clock.GetUtcNow())
                let! outcome = work conn transaction now ct

                match outcome with
                | Ok _ -> do! transaction.CommitAsync(ct)
                | Error _ -> do! transaction.RollbackAsync(ct)

                return outcome
            finally
                gate.Release() |> ignore
        }

    /// <summary>
    /// Runs typed reads against the generated schema types, under the same pipeline and the same
    /// failure classification as every hand-written statement. SqlHydra builds and runs the
    /// query; the connection, the retries and what a failure means stay here.
    ///
    /// Pass the context's read-only connection string. Reads take no gate and no write lock,
    /// and under WAL they see the last committed state while a writer works.
    /// </summary>
    let query
        (pipeline: ResiliencePipeline)
        (readConnectionString: string)
        (work: SqlHydra.Query.QueryContext -> CancellationToken -> Task<Result<'T, StoreError>>)
        (ct: CancellationToken)
        : Task<Result<'T, StoreError>> =
        protect
            pipeline
            (fun cancel ->
                backgroundTask {
                    let! conn = openConnection readConnectionString cancel

                    use context = new SqlHydra.Query.QueryContext(conn, SqlHydra.Query.SqliteEmitter())

                    return! work context cancel
                })
            ct

    /// <summary>Binds named parameters, in one call rather than one statement each.</summary>
    let parameters (values: (string * obj) list) (cmd: SqliteCommand) : unit =
        values
        |> List.iter (fun (name, value) -> cmd.Parameters.AddWithValue(name, value) |> ignore)

    /// <summary>A boolean as the schema stores it. STRICT tables have no boolean type.</summary>
    let flag (value: bool) : int64 = if value then 1L else 0L

    /// <summary>The instance lifecycle, as the status CHECK spells it.</summary>
    let instanceStatusToString (status: InstanceStatus) : string =
        match status with
        | Running -> "running"
        | Suspended -> "suspended"
        | Terminated -> "terminated"

    let epochOf (value: int64) : Epoch = Epoch.ofUInt64 (uint64 value)

    let toStoreError (error: CodecError) : StoreError =
        match error with
        | CodecError.EncodeError(typeName, ex)
        | CodecError.DecodeError(typeName, ex) -> StoreError.Serialization(typeName, ex)

    /// <summary>Wraps a decode failure carrying only a message as the serialization error the contract promises.</summary>
    let decodeFailure (typeName: string) (message: string) : StoreError =
        StoreError.Serialization(typeName, FormatException message)

    /// <summary>The instance lifecycle, read back from the status CHECK's spelling.</summary>
    let instanceStatusFromString (value: string) : Result<InstanceStatus, StoreError> =
        match value with
        | "running" -> Ok Running
        | "suspended" -> Ok Suspended
        | "terminated" -> Ok Terminated
        | other -> Error(decodeFailure (nameof InstanceStatus) $"unknown instance status {other}")

/// <summary>
/// Column access by name.
///
/// Statement results are read by name rather than by ordinal throughout, for the same reason as
/// in the PostgreSQL store: an ordinal read turns "a column was added in the middle" from a
/// failure into a wrong value that still type-checks.
/// </summary>
[<RequireQualifiedAccess>]
module internal Row =

    let string (reader: SqliteDataReader) (name: string) : string =
        reader.GetString(reader.GetOrdinal name)

    let int32 (reader: SqliteDataReader) (name: string) : int = reader.GetInt32(reader.GetOrdinal name)

    let int64 (reader: SqliteDataReader) (name: string) : int64 = reader.GetInt64(reader.GetOrdinal name)

    /// <summary>An <c>INTEGER</c> flag. The column's CHECK holds it to 0 or 1.</summary>
    let bool (reader: SqliteDataReader) (name: string) : bool = int64 reader name <> 0L

    let instant (reader: SqliteDataReader) (name: string) : DateTimeOffset =
        Instant.toDateTimeOffset (int64 reader name)

    /// <summary>Reads every remaining row through <paramref name="read" />, in result order.</summary>
    let all (read: SqliteDataReader -> 'T) (reader: SqliteDataReader) (ct: CancellationToken) : Task<'T list> =
        let rec next (acc: 'T list) =
            backgroundTask {
                match! reader.ReadAsync ct with
                | true -> return! next (read reader :: acc)
                | false -> return List.rev acc
            }

        next []

/// <summary>
/// Statements run inside a write transaction, one call each.
///
/// Microsoft.Data.Sqlite refuses a command on a connection with an open transaction unless the
/// command names it, so every command here is enlisted before it runs. Parameters are named
/// with their <c>@</c> prefix, exactly as the statement spells them.
/// </summary>
[<RequireQualifiedAccess>]
module internal Statement =

    let private prepare
        (conn: SqliteConnection)
        (transaction: SqliteTransaction)
        (sql: string)
        (values: (string * obj) list)
        : SqliteCommand =
        let cmd = conn.CreateCommand()
        cmd.Transaction <- transaction
        cmd.CommandText <- sql
        Db.parameters values cmd
        cmd

    /// <summary>Runs a statement for its effect and answers how many rows it changed.</summary>
    let execute conn transaction sql values (ct: CancellationToken) : Task<int> =
        backgroundTask {
            use cmd = prepare conn transaction sql values
            return! cmd.ExecuteNonQueryAsync ct
        }

    /// <summary>Every row a statement returns, read through <paramref name="read" />.</summary>
    let rows conn transaction sql values (read: SqliteDataReader -> 'T) (ct: CancellationToken) : Task<'T list> =
        backgroundTask {
            use cmd = prepare conn transaction sql values
            use! reader = cmd.ExecuteReaderAsync ct
            return! Row.all read reader ct
        }

    /// <summary>The first row a statement returns, if it returns any.</summary>
    let tryOne conn transaction sql values (read: SqliteDataReader -> 'T) (ct: CancellationToken) : Task<'T option> =
        backgroundTask {
            use cmd = prepare conn transaction sql values
            use! reader = cmd.ExecuteReaderAsync ct

            match! reader.ReadAsync ct with
            | true -> return Some(read reader)
            | false -> return None
        }
