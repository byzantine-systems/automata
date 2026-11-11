namespace ByzantineSystems.Automata.Storage.Sqlite

open System
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Internal
open Microsoft.Data.Sqlite

/// <summary>
/// An embedded statement, resolved once when the store is built. A missing resource fails at
/// construction rather than on first use, and the key names the statement in errors.
/// </summary>
type internal Statement = private { Key: string; Text: string }

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Storage.Sqlite.Statement" />.</summary>
[<RequireQualifiedAccess>]
module internal Statement =

    let load (domain: string) (operation: string) : Statement =
        { Key = $"%s{domain}/%s{operation}"
          Text = SqlResources.get domain operation }

    let key (statement: Statement) : string = statement.Key

    let text (statement: Statement) : string = statement.Text

/// <summary>
/// A value a statement binds, as a STRICT table stores it. SQL NULL is only ever sent through an
/// <c>OrNull</c> case, so absence is a decision at the call site and never an accident.
/// </summary>
[<RequireQualifiedAccess>]
type internal Param =
    | Integer of int64
    | Text of string
    | Blob of byte array
    | TextOrNull of string option

/// <summary>Typed identifiers at the SQLite parameter boundary.</summary>
[<RequireQualifiedAccess>]
module internal Params =

    let machineId name (value: MachineId) = name, Param.Text(MachineId.value value)

    let entityKey name (value: ByzantineSystems.Automata.Storage.Internal.EntityKey) =
        name, Param.Text(ByzantineSystems.Automata.Storage.Internal.EntityKey.value value)

    let commandId name (value: CommandId) =
        name, Param.Integer(CommandId.value value)

    let commandLease name (value: LeaseToken<CommandWork>) =
        name, Param.Integer(LeaseToken.value value)

/// <summary>
/// The statements, reads and runners the SQLite stores are written with. Parameters are named
/// with their <c>@</c> prefix, exactly as the statement spells them.
/// </summary>
[<RequireQualifiedAccess>]
module internal Sql =

    /// <summary>
    /// Binds one parameter. A null string is a defect in the caller, not a request for SQL NULL,
    /// and is refused as one.
    /// </summary>
    let private bind (cmd: SqliteCommand) (name: string, value: Param) : unit =
        let parameter =
            match value with
            | Param.Integer number -> SqliteParameter(name, SqliteType.Integer, Value = box number)
            | Param.Blob bytes -> SqliteParameter(name, SqliteType.Blob, Value = box bytes)
            | Param.Text text
            | Param.TextOrNull(Some text) ->
                if isNull text then
                    nullArg name

                SqliteParameter(name, SqliteType.Text, Value = box text)
            | Param.TextOrNull None -> SqliteParameter(name, SqliteType.Text, Value = box DBNull.Value)

        cmd.Parameters.Add parameter |> ignore

    let private command
        (connection: SqliteConnection)
        (transaction: SqliteTransaction)
        (statement: Statement)
        (values: (string * Param) list)
        : SqliteCommand =
        let cmd = connection.CreateCommand()
        cmd.Transaction <- transaction
        cmd.CommandText <- Statement.text statement
        values |> List.iter (bind cmd)
        cmd

    let private sequenceRows rows =
        let prepend row values =
            match row, values with
            | Ok head, Ok tail -> Ok(head :: tail)
            | Error error, _
            | _, Error error -> Error error

        List.foldBack prepend rows (Ok [])

    let private execute connection transaction statement values (ct: CancellationToken) =
        backgroundTask {
            use cmd = command connection transaction statement values
            let! changed = cmd.ExecuteNonQueryAsync ct
            return Ok changed
        }

    let private queryRows
        connection
        transaction
        statement
        values
        (read: Read<SqliteDataReader, 'T>)
        (ct: CancellationToken)
        =
        backgroundTask {
            use cmd = command connection transaction statement values
            use! reader = cmd.ExecuteReaderAsync ct
            let! rows = Row.all read reader ct
            return sequenceRows rows
        }

    let private tryQueryOne
        connection
        transaction
        statement
        values
        (read: Read<SqliteDataReader, 'T>)
        (ct: CancellationToken)
        =
        backgroundTask {
            use cmd = command connection transaction statement values
            use! reader = cmd.ExecuteReaderAsync ct

            match! reader.ReadAsync ct with
            | false -> return Ok None
            | true ->
                let value = read reader

                match! reader.ReadAsync ct with
                | true ->
                    return
                        Error(
                            Db.decodeFailure
                                (Statement.key statement)
                                $"%s{Statement.key statement} returned more than one row"
                        )
                | false -> return value |> Result.map Some
        }

    let private queryOne connection transaction statement values read ct =
        backgroundTask {
            match! tryQueryOne connection transaction statement values read ct with
            | Ok(Some value) -> return Ok value
            | Ok None ->
                return Error(Db.decodeFailure (Statement.key statement) $"%s{Statement.key statement} returned no row")
            | Error error -> return Error error
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
    let private writeTransaction
        (connectionString: string)
        (clock: TimeProvider)
        (work: SqliteConnection -> SqliteTransaction -> int64 -> CancellationToken -> Task<Result<'T, StoreError>>)
        (ct: CancellationToken)
        : Task<Result<'T, StoreError>> =
        backgroundTask {
            let gate = WriteGate.forConnectionString connectionString
            do! gate.WaitAsync(ct)

            try
                use! conn = Db.openConnection connectionString ct
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

    let private programStatements = ConcurrentDictionary<StatementKey, Statement>()

    let private programStatement key =
        programStatements.GetOrAdd(
            key,
            fun key ->
                let domain, operation = StatementKey.parts key
                Statement.load domain operation
        )

    let prepareProgram (key: StatementKey) =
        programStatement key |> ignore
        key

    let private readProgramSession
        (context: SqliteContext)
        (program: StoreProgram<ReadAccess, (string * Param) list, SqliteDataReader, 'T>)
        (ct: CancellationToken)
        =
        backgroundTask {
            use! conn = Db.openConnection context.ReadConnectionString ct
            use transaction = conn.BeginTransaction(deferred = true)

            let interpreter =
                { new IStoreInterpreter<(string * Param) list, SqliteDataReader> with
                    member _.ExecuteAsync(key, _, _) =
                        Task.FromResult(
                            Error(
                                StoreError.Unexpected(
                                    InvalidOperationException(
                                        $"Read program attempted write instruction '%s{StatementKey.value key}'."
                                    )
                                )
                            )
                        )

                    member _.QueryOneAsync<'Row>
                        (key, parameters, read: Read<SqliteDataReader, 'Row>, cancellationToken)
                        : Task<Result<'Row, StoreError>> =
                        queryOne conn transaction (programStatement key) parameters read cancellationToken

                    member _.TryQueryOneAsync<'Row>
                        (key, parameters, read: Read<SqliteDataReader, 'Row>, cancellationToken)
                        : Task<Result<'Row option, StoreError>> =
                        tryQueryOne conn transaction (programStatement key) parameters read cancellationToken

                    member _.QueryRowsAsync<'Row>
                        (key, parameters, read: Read<SqliteDataReader, 'Row>, cancellationToken)
                        : Task<Result<'Row list, StoreError>> =
                        queryRows conn transaction (programStatement key) parameters read cancellationToken }

            return! StoreProgram.run interpreter ct program
        }

    let readProgram
        (context: SqliteContext)
        (program: StoreProgram<ReadAccess, (string * Param) list, SqliteDataReader, 'T>)
        (ct: CancellationToken)
        : Task<Result<'T, StoreError>> =
        Db.protect context.Resilience (readProgramSession context program) ct

    let private runWriteProgram
        (protect:
            (CancellationToken -> Task<Result<'T, StoreError>>) -> CancellationToken -> Task<Result<'T, StoreError>>)
        (context: SqliteContext)
        (program: int64 -> StoreProgram<WriteAccess, (string * Param) list, SqliteDataReader, 'T>)
        (ct: CancellationToken)
        =
        protect
            (fun token ->
                writeTransaction
                    context.ConnectionString
                    context.Clock
                    (fun conn transaction now cancellationToken ->
                        let interpreter =
                            { new IStoreInterpreter<(string * Param) list, SqliteDataReader> with
                                member _.ExecuteAsync(key, parameters, cancel) =
                                    execute conn transaction (programStatement key) parameters cancel

                                member _.QueryOneAsync<'Row>
                                    (key, parameters, read: Read<SqliteDataReader, 'Row>, cancel)
                                    : Task<Result<'Row, StoreError>> =
                                    queryOne conn transaction (programStatement key) parameters read cancel

                                member _.TryQueryOneAsync<'Row>
                                    (key, parameters, read: Read<SqliteDataReader, 'Row>, cancel)
                                    : Task<Result<'Row option, StoreError>> =
                                    tryQueryOne conn transaction (programStatement key) parameters read cancel

                                member _.QueryRowsAsync<'Row>
                                    (key, parameters, read: Read<SqliteDataReader, 'Row>, cancel)
                                    : Task<Result<'Row list, StoreError>> =
                                    queryRows conn transaction (programStatement key) parameters read cancel }

                        StoreProgram.run interpreter cancellationToken (program now))
                    token)
            ct

    let writeProgram
        (context: SqliteContext)
        (program: StoreProgram<WriteAccess, (string * Param) list, SqliteDataReader, 'T>)
        (ct: CancellationToken)
        : Task<Result<'T, StoreError>> =
        runWriteProgram (Db.protect context.Resilience) context (fun _ -> program) ct

    let writeProgramWith
        (context: SqliteContext)
        (program: int64 -> StoreProgram<WriteAccess, (string * Param) list, SqliteDataReader, 'T>)
        (ct: CancellationToken)
        : Task<Result<'T, StoreError>> =
        runWriteProgram (Db.protect context.Resilience) context program ct

    let writeProgramOnce
        (context: SqliteContext)
        (program: StoreProgram<WriteAccess, (string * Param) list, SqliteDataReader, 'T>)
        (ct: CancellationToken)
        : Task<Result<'T, StoreError>> =
        runWriteProgram Db.protectOnce context (fun _ -> program) ct

    let writeProgramOnceWith
        (context: SqliteContext)
        (program: int64 -> StoreProgram<WriteAccess, (string * Param) list, SqliteDataReader, 'T>)
        (ct: CancellationToken)
        : Task<Result<'T, StoreError>> =
        runWriteProgram Db.protectOnce context program ct
