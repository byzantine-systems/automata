namespace ByzantineSystems.Automata.Storage.Sqlite

open System
open System.Data.Common
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage.Internal
open Microsoft.Data.Sqlite
open SqlHydra.Query

/// <summary>
/// What a read sees: a query context over a read-only connection, inside one deferred
/// transaction. The transaction is what makes a read of several statements consistent: under
/// WAL it pins one snapshot, so a writer that commits between two statements is invisible to
/// the second. It takes no write lock, so it never makes the writer wait.
/// </summary>
type internal ReadSession = { Query: QueryContext }

/// <summary>
/// What a write sees: the connection, the <c>IMMEDIATE</c> transaction every statement must be
/// enlisted in, and the one instant the whole transaction binds as its <c>now</c>.
/// </summary>
type internal WriteSession =
    { Connection: SqliteConnection
      Transaction: SqliteTransaction
      Now: int64 }

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
    | TextOrNull of string option

/// <summary>The two builders, one per session, so a write cannot be run on the read path.</summary>
[<AutoOpen>]
module internal Builders =

    let sqliteRead = OpBuilder<ReadSession>()

    let sqliteWrite = OpBuilder<WriteSession>()

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
            | Param.Text text
            | Param.TextOrNull(Some text) ->
                if isNull text then
                    nullArg name

                SqliteParameter(name, SqliteType.Text, Value = box text)
            | Param.TextOrNull None -> SqliteParameter(name, SqliteType.Text, Value = box DBNull.Value)

        cmd.Parameters.Add parameter |> ignore

    /// <summary>
    /// A command enlisted in the session's transaction. Microsoft.Data.Sqlite refuses a command
    /// on a connection with an open transaction unless the command names it.
    /// </summary>
    let private command (session: WriteSession) (statement: Statement) (values: (string * Param) list) : SqliteCommand =
        let cmd = session.Connection.CreateCommand()
        cmd.Transaction <- session.Transaction
        cmd.CommandText <- Statement.text statement
        values |> List.iter (bind cmd)
        cmd

    /// <summary>The instant this transaction binds as its <c>now</c>, read once before it began.</summary>
    let now: Op<WriteSession, int64> = Op.map (fun session -> session.Now) Op.session

    /// <summary>Runs a statement for its effect and answers how many rows it changed.</summary>
    let execute (statement: Statement) (values: (string * Param) list) : Op<WriteSession, int> =
        Op.ofDriver (fun session ct ->
            backgroundTask {
                use cmd = command session statement values
                let! changed = cmd.ExecuteNonQueryAsync ct
                return Ok changed
            })

    /// <summary>Every row a statement returns, read through <paramref name="read" />, in result order.</summary>
    let rows
        (statement: Statement)
        (values: (string * Param) list)
        (read: SqliteDataReader -> 'T)
        : Op<WriteSession, 'T list> =
        Op.ofDriver (fun session ct ->
            backgroundTask {
                use cmd = command session statement values
                use! reader = cmd.ExecuteReaderAsync ct
                let! all = Row.all read reader ct
                return Ok all
            })

    /// <summary>
    /// The row a statement returns, if it returns one. A second row is an error rather than
    /// something to ignore: the statement and its caller disagree about what it selects.
    /// </summary>
    let tryOne
        (typeName: string)
        (statement: Statement)
        (values: (string * Param) list)
        (read: SqliteDataReader -> 'T)
        : Op<WriteSession, 'T option> =
        Op.ofDriver (fun session ct ->
            backgroundTask {
                use cmd = command session statement values
                use! reader = cmd.ExecuteReaderAsync ct

                match! reader.ReadAsync ct with
                | false -> return Ok None
                | true ->
                    let value = read reader

                    match! reader.ReadAsync ct with
                    | true ->
                        return
                            Error(Db.decodeFailure typeName $"%s{Statement.key statement} returned more than one row")
                    | false -> return Ok(Some value)
            })

    /// <summary>The one row a statement returns. None, or more than one, is an error.</summary>
    let one
        (typeName: string)
        (statement: Statement)
        (values: (string * Param) list)
        (read: SqliteDataReader -> 'T)
        : Op<WriteSession, 'T> =
        tryOne typeName statement values read
        |> Op.bind (function
            | Some value -> Op.ok value
            | None -> Op.fail (Db.decodeFailure typeName $"%s{Statement.key statement} returned no row"))

    /// <summary>
    /// A typed read through SqlHydra, against the session's query context. The work must hand
    /// back materialised values, never a lazy sequence, because the connection closes when the
    /// unit ends; and it must never call a runner, which would open a second connection.
    /// </summary>
    let select (work: QueryContext -> CancellationToken -> Task<'T>) : Op<ReadSession, 'T> =
        Op.ofDriver (fun session ct ->
            backgroundTask {
                let! value = work session.Query ct
                return Ok value
            })

    /// <summary>
    /// Runs a read unit: a read-only connection, one deferred transaction, the pipeline around
    /// all of it. Reads take no gate and never contend for the write lock.
    ///
    /// The transaction is ours rather than the query context's own, so that it is deferred by
    /// construction; it is disposed first, which ends it, and the context then closes the
    /// connection.
    /// </summary>
    let read (context: SqliteContext) (op: Op<ReadSession, 'T>) (ct: CancellationToken) : Task<Result<'T, StoreError>> =
        Db.protect
            context.Resilience
            (fun token ->
                backgroundTask {
                    let! conn = Db.openConnection context.ReadConnectionString token
                    use query = new QueryContext(conn, SqliteEmitter())
                    use transaction = conn.BeginTransaction(deferred = true)
                    query.Transaction <- Some(transaction :> DbTransaction)
                    return! Op.run op { Query = query } token
                })
            ct

    let private writeWith
        (protect:
            (CancellationToken -> Task<Result<'T, StoreError>>) -> CancellationToken -> Task<Result<'T, StoreError>>)
        (context: SqliteContext)
        (op: Op<WriteSession, 'T>)
        (ct: CancellationToken)
        : Task<Result<'T, StoreError>> =
        protect
            (fun token ->
                Db.writeTransaction
                    context.ConnectionString
                    context.Clock
                    (fun conn transaction now cancel ->
                        Op.run
                            op
                            { Connection = conn
                              Transaction = transaction
                              Now = now }
                            cancel)
                    token)
            ct

    /// <summary>
    /// Runs a write unit that is safe to repeat: the gate, one <c>IMMEDIATE</c> transaction,
    /// committed on <c>Ok</c> and rolled back on <c>Error</c>, with the pipeline around all of it.
    /// A retry runs the whole unit again in a new transaction with a new <c>now</c>.
    /// </summary>
    let write
        (context: SqliteContext)
        (op: Op<WriteSession, 'T>)
        (ct: CancellationToken)
        : Task<Result<'T, StoreError>> =
        writeWith (Db.protect context.Resilience) context op ct

    /// <summary>
    /// Runs a write unit exactly once, for claims: a claim whose reply was lost has already
    /// leased a batch, and the worker's next poll recovers sooner than a retry would.
    /// </summary>
    let writeOnce
        (context: SqliteContext)
        (op: Op<WriteSession, 'T>)
        (ct: CancellationToken)
        : Task<Result<'T, StoreError>> =
        writeWith Db.protectOnce context op ct
