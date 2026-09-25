namespace ByzantineSystems.Automata.Storage.Sqlite

open System
open ByzantineSystems.Automata.Resilience
open Microsoft.Data.Sqlite
open Polly

/// <summary>
/// One database file, the resilience applied to everything sent to it, and the clock its
/// writes are stamped with.
///
/// They travel together for the same reason the PostgreSQL context pairs its pool with its
/// pipeline: a circuit breaker that sees only part of a file's traffic cannot judge that file's
/// health. Every store takes one of these, so one file has one circuit.
///
/// The clock is here because an embedded database has no clock of its own worth using.
/// SQLite's stops at milliseconds, and the host's clock is the one it would read anyway, so the
/// store reads the host's once per transaction and binds it. A test can pass a controllable one.
/// </summary>
type SqliteContext =
    {
        /// <summary>Opens read-write. Write transactions go through this, one at a time.</summary>
        ConnectionString: string
        /// <summary>
        /// The same file, opened read-only, for every statement that only reads. Such a
        /// connection cannot take the write lock, so a read never queues behind the writer or
        /// makes it wait, and a write sent down the read path fails with <c>SQLITE_READONLY</c>
        /// instead of quietly competing for the file. Pooled separately, because the pool is
        /// keyed by connection string.
        /// </summary>
        ReadConnectionString: string
        Resilience: ResiliencePipeline
        Clock: TimeProvider
    }

/// <summary>Builds the connection strings and the pipeline the stores use.</summary>
[<RequireQualifiedAccess>]
module DataSource =

    /// <summary>
    /// How long a statement waits for another connection's write lock before failing with
    /// <c>SQLITE_BUSY</c>, in seconds. Microsoft.Data.Sqlite implements the wait itself, up to
    /// the command timeout, which is what <c>Default Timeout</c> sets.
    ///
    /// Short on purpose, and shorter than the attempt timeout of
    /// <c>TransientPolicy.defaults</c>. The driver's asynchronous methods run synchronously underneath, so a wait blocks
    /// a thread-pool thread and ignores cancellation until it ends. A wait that outlived the
    /// attempt timeout would leave the pipeline unable to abandon it.
    /// </summary>
    [<Literal>]
    let BusyTimeoutSeconds = 3

    /// <summary>
    /// The connection string for a database file, with everything the stores rely on set.
    ///
    /// Foreign keys are off by default in SQLite and set per connection, which the
    /// <c>Foreign Keys</c> keyword does on every open, pooled connections included. The journal
    /// mode is not set here: WAL is a property of the file rather than of a connection, and the
    /// migrator sets it once. Neither are other pragmas: this keyword set is all the driver
    /// accepts.
    /// </summary>
    let connectionString (path: string) : string =
        SqliteConnectionStringBuilder(
            DataSource = path,
            Mode = SqliteOpenMode.ReadWriteCreate,
            ForeignKeys = Nullable true,
            DefaultTimeout = BusyTimeoutSeconds,
            Pooling = true
        )
            .ToString()

    /// <summary>
    /// The read-only form of a connection string: the same file and keywords, opened with
    /// <c>Mode=ReadOnly</c>. A WAL database opens read-only even when its <c>-wal</c> and
    /// <c>-shm</c> files are absent, provided the directory is writable, which it must be for
    /// the writer anyway.
    /// </summary>
    let readOnly (connectionString: string) : string =
        let builder = SqliteConnectionStringBuilder(connectionString)
        builder.Mode <- SqliteOpenMode.ReadOnly
        builder.ToString()

    /// <summary>
    /// Whether a connection string names a database that exists only inside one connection.
    ///
    /// With pooling, every connection to <c>:memory:</c> opens a different, empty database, and
    /// an in-memory database cannot run in WAL mode. Both break the design rather than slow it,
    /// so a store refuses such a string instead of serving from it.
    /// </summary>
    let isInMemory (connectionString: string) : bool =
        let builder = SqliteConnectionStringBuilder(connectionString)

        builder.Mode = SqliteOpenMode.Memory
        || String.Equals(builder.DataSource, ":memory:", StringComparison.OrdinalIgnoreCase)
        || String.IsNullOrEmpty builder.DataSource

    /// <summary>
    /// The resilience every call through a context runs under, built once and shared so the
    /// circuit breaker sees the whole file's traffic rather than one method's. Pass
    /// <c>ignore</c> when no event sink is wanted.
    /// </summary>
    let resilience (policy: TransientPolicy) (onEvent: PipelineEventSink) : ResiliencePipeline =
        TransientPolicy.toPipeline policy "automata-sqlite" Db.isTransient onEvent

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Storage.Sqlite.SqliteContext" />.</summary>
[<RequireQualifiedAccess>]
module SqliteContext =

    /// <summary>Pairs a connection string with a policy and a clock of the caller's choosing.</summary>
    let create
        (connectionString: string)
        (policy: TransientPolicy)
        (onEvent: PipelineEventSink)
        (clock: TimeProvider)
        : SqliteContext =
        { ConnectionString = connectionString
          ReadConnectionString = DataSource.readOnly connectionString
          Resilience = DataSource.resilience policy onEvent
          Clock = clock }

    /// <summary>
    /// A context for a database file, on the default policy, the system clock, and with no
    /// event sink. The file is created on first use.
    /// </summary>
    let ofPath (path: string) : SqliteContext =
        create (DataSource.connectionString path) TransientPolicy.defaults ignore TimeProvider.System
