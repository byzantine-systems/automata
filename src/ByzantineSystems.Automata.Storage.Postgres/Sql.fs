namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage.Internal
open Npgsql
open NpgsqlTypes
open SqlHydra.Query

/// <summary>
/// What a unit of work sees: one pooled connection and a query context over it. There is no
/// transaction here on purpose. Atomicity belongs to the <c>fsm.*</c> routines, so a unit of
/// several statements is neither atomic nor reads one snapshot, and anything that needs either
/// belongs in a routine.
/// </summary>
type internal PgSession =
    { Connection: NpgsqlConnection
      Query: QueryContext }

/// <summary>
/// An embedded statement, resolved once when the store is built. A missing resource fails at
/// construction rather than on first use, and the key names the statement in errors.
/// </summary>
type internal Statement = private { Key: string; Text: string }

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Storage.Postgres.Statement" />.</summary>
[<RequireQualifiedAccess>]
module internal Statement =

    let load (domain: string) (operation: string) : Statement =
        { Key = $"%s{domain}/%s{operation}"
          Text = SqlResources.get domain operation }

    let key (statement: Statement) : string = statement.Key

    let text (statement: Statement) : string = statement.Text

/// <summary>
/// A value a statement binds. Each case fixes the parameter's type, so a statement is always
/// prepared with the same types whichever values it is sent, and SQL NULL is only ever sent
/// through an <c>OrNull</c> case whose type comes from the case.
/// </summary>
[<RequireQualifiedAccess>]
type internal Param =
    | Text of string
    | Int of int
    | Bigint of int64
    | Interval of TimeSpan
    | Timestamp of DateTimeOffset
    | Jsonb of string
    | TextArray of string list
    | TextOrNull of string option
    | JsonbOrNull of string option
    | TimestampOrNull of DateTimeOffset option
    | TextArrayOrNull of string list option

/// <summary>The builder PostgreSQL units of work are written in.</summary>
[<AutoOpen>]
module internal Builders =

    let postgres = OpBuilder<PgSession>()

/// <summary>The statements, reads and runners the PostgreSQL stores are written with.</summary>
[<RequireQualifiedAccess>]
module internal Sql =

    /// <summary>A text value, refusing a null: that is a defect in the caller, not a request for SQL NULL.</summary>
    let private text (name: string) (value: string) : obj =
        if isNull value then
            nullArg name

        box value

    let private array (name: string) (values: string list) : obj =
        values |> List.map (text name) |> ignore
        box (List.toArray values)

    let private orNull (encode: 'T -> obj) (value: 'T option) : obj =
        match value with
        | Some value -> encode value
        | None -> box DBNull.Value

    let private bind (cmd: NpgsqlCommand) (name: string, value: Param) : unit =
        let dbType, boxed =
            match value with
            | Param.Text value -> NpgsqlDbType.Text, text name value
            | Param.Int value -> NpgsqlDbType.Integer, box value
            | Param.Bigint value -> NpgsqlDbType.Bigint, box value
            | Param.Interval value -> NpgsqlDbType.Interval, box value
            | Param.Timestamp value -> NpgsqlDbType.TimestampTz, box (Db.timestamp value)
            | Param.Jsonb value -> NpgsqlDbType.Jsonb, text name value
            | Param.TextArray values -> NpgsqlDbType.Array ||| NpgsqlDbType.Text, array name values
            | Param.TextOrNull value -> NpgsqlDbType.Text, orNull (text name) value
            | Param.JsonbOrNull value -> NpgsqlDbType.Jsonb, orNull (text name) value
            | Param.TimestampOrNull value -> NpgsqlDbType.TimestampTz, orNull (Db.timestamp >> box) value
            | Param.TextArrayOrNull values -> NpgsqlDbType.Array ||| NpgsqlDbType.Text, orNull (array name) values

        cmd.Parameters.Add(NpgsqlParameter(name, dbType, Value = boxed)) |> ignore

    let private command (session: PgSession) (statement: Statement) (values: (string * Param) list) : NpgsqlCommand =
        let cmd = new NpgsqlCommand(Statement.text statement, session.Connection)
        values |> List.iter (bind cmd)
        cmd

    /// <summary>Runs a statement for its effect and answers how many rows it changed.</summary>
    let execute (statement: Statement) (values: (string * Param) list) : Op<PgSession, int> =
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
        (read: NpgsqlDataReader -> 'T)
        : Op<PgSession, 'T list> =
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
        (read: NpgsqlDataReader -> 'T)
        : Op<PgSession, 'T option> =
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
        (read: NpgsqlDataReader -> 'T)
        : Op<PgSession, 'T> =
        tryOne typeName statement values read
        |> Op.bind (function
            | Some value -> Op.ok value
            | None -> Op.fail (Db.decodeFailure typeName $"%s{Statement.key statement} returned no row"))

    /// <summary>
    /// A typed read through SqlHydra, against the session's query context. The work must hand
    /// back materialised values, never a lazy sequence, because the connection closes when the
    /// unit ends; and it must never call a runner, which would take a second pooled connection.
    /// </summary>
    let select (work: QueryContext -> CancellationToken -> Task<'T>) : Op<PgSession, 'T> =
        Op.ofDriver (fun session ct ->
            backgroundTask {
                let! value = work session.Query ct
                return Ok value
            })

    /// <summary>One pooled connection for the whole unit; the query context closes it.</summary>
    let private session (dataSource: NpgsqlDataSource) (op: Op<PgSession, 'T>) (ct: CancellationToken) =
        backgroundTask {
            let! conn = dataSource.OpenConnectionAsync(ct).AsTask()
            use query = new QueryContext(conn, PostgresEmitter())
            return! Op.run op { Connection = conn; Query = query } ct
        }

    /// <summary>
    /// Runs a unit, distinguishing a refusal under a named constraint from every other answer,
    /// for the one caller that expects a particular violation and has something to do about it.
    /// </summary>
    let attempt (context: PostgresContext) (op: Op<PgSession, 'T>) (ct: CancellationToken) : Task<Db.Attempt<'T>> =
        Db.attempt context.Resilience (session context.DataSource op) ct

    /// <summary>
    /// Runs a unit that is safe to repeat, with the pipeline applied. A retry runs the whole unit
    /// again on a fresh connection, so every statement in it must be idempotent: the routines
    /// are, by construction.
    /// </summary>
    let run (context: PostgresContext) (op: Op<PgSession, 'T>) (ct: CancellationToken) : Task<Result<'T, StoreError>> =
        Db.protect context.Resilience (session context.DataSource op) ct

    /// <summary>
    /// Runs a unit exactly once, for claims: a claim whose reply was lost has already leased a
    /// batch, and the worker's next poll recovers sooner than a retry would.
    /// </summary>
    let runOnce
        (context: PostgresContext)
        (op: Op<PgSession, 'T>)
        (ct: CancellationToken)
        : Task<Result<'T, StoreError>> =
        Db.protectOnce (session context.DataSource op) ct
