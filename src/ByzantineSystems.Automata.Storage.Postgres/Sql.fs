namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Internal
open Npgsql
open NpgsqlTypes

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

/// <summary>Typed identifiers at the PostgreSQL parameter boundary.</summary>
[<RequireQualifiedAccess>]
module internal Params =

    let machineId name (value: MachineId) = name, Param.Text(MachineId.value value)

    let entityKey name (value: ByzantineSystems.Automata.Storage.Internal.EntityKey) =
        name, Param.Text(ByzantineSystems.Automata.Storage.Internal.EntityKey.value value)

    let commandId name (value: CommandId) =
        name, Param.Bigint(CommandId.value value)

    let commandLease name (value: LeaseToken<CommandWork>) =
        name, Param.Bigint(LeaseToken.value value)

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

    let private command
        (connection: NpgsqlConnection)
        (statement: Statement)
        (values: (string * Param) list)
        : NpgsqlCommand =
        let cmd = new NpgsqlCommand(Statement.text statement, connection)
        values |> List.iter (bind cmd)
        cmd

    let private sequenceRows rows =
        let prepend row values =
            match row, values with
            | Ok head, Ok tail -> Ok(head :: tail)
            | Error error, _
            | _, Error error -> Error error

        List.foldBack prepend rows (Ok [])

    let private execute
        (connection: NpgsqlConnection)
        (statement: Statement)
        (values: (string * Param) list)
        (ct: CancellationToken)
        : Task<Result<int, StoreError>> =
        backgroundTask {
            use cmd = command connection statement values
            let! changed = cmd.ExecuteNonQueryAsync ct
            return Ok changed
        }

    let private queryRows
        (connection: NpgsqlConnection)
        (statement: Statement)
        (values: (string * Param) list)
        (read: Read<NpgsqlDataReader, 'T>)
        (ct: CancellationToken)
        : Task<Result<'T list, StoreError>> =
        backgroundTask {
            use cmd = command connection statement values
            use! reader = cmd.ExecuteReaderAsync ct
            let! rows = Row.all read reader ct
            return sequenceRows rows
        }

    let private tryQueryOne
        (connection: NpgsqlConnection)
        (statement: Statement)
        (values: (string * Param) list)
        (read: Read<NpgsqlDataReader, 'T>)
        (ct: CancellationToken)
        : Task<Result<'T option, StoreError>> =
        backgroundTask {
            use cmd = command connection statement values
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

    let private queryOne connection statement values read ct =
        backgroundTask {
            match! tryQueryOne connection statement values read ct with
            | Ok(Some value) -> return Ok value
            | Ok None ->
                return Error(Db.decodeFailure (Statement.key statement) $"%s{Statement.key statement} returned no row")
            | Error error -> return Error error
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

    let private programSession
        (dataSource: NpgsqlDataSource)
        (program: StoreProgram<'Access, (string * Param) list, NpgsqlDataReader, 'T>)
        (ct: CancellationToken)
        =
        backgroundTask {
            use! conn = dataSource.OpenConnectionAsync(ct).AsTask()

            let interpreter =
                { new IStoreInterpreter<(string * Param) list, NpgsqlDataReader> with
                    member _.ExecuteAsync(key, parameters, cancellationToken) =
                        execute conn (programStatement key) parameters cancellationToken

                    member _.QueryOneAsync<'Row>
                        (key, parameters, read: Read<NpgsqlDataReader, 'Row>, cancellationToken)
                        : Task<Result<'Row, StoreError>> =
                        queryOne conn (programStatement key) parameters read cancellationToken

                    member _.TryQueryOneAsync<'Row>
                        (key, parameters, read: Read<NpgsqlDataReader, 'Row>, cancellationToken)
                        : Task<Result<'Row option, StoreError>> =
                        tryQueryOne conn (programStatement key) parameters read cancellationToken

                    member _.QueryRowsAsync<'Row>
                        (key, parameters, read: Read<NpgsqlDataReader, 'Row>, cancellationToken)
                        : Task<Result<'Row list, StoreError>> =
                        queryRows conn (programStatement key) parameters read cancellationToken }

            return! StoreProgram.run interpreter ct program
        }

    let runProgram
        (context: PostgresContext)
        (program: StoreProgram<WriteAccess, (string * Param) list, NpgsqlDataReader, 'T>)
        (ct: CancellationToken)
        : Task<Result<'T, StoreError>> =
        Db.protect context.Resilience (programSession context.DataSource program) ct

    let readProgram
        (context: PostgresContext)
        (program: StoreProgram<ReadAccess, (string * Param) list, NpgsqlDataReader, 'T>)
        (ct: CancellationToken)
        : Task<Result<'T, StoreError>> =
        Db.protect context.Resilience (programSession context.DataSource program) ct

    let attemptProgram
        (context: PostgresContext)
        (program: StoreProgram<WriteAccess, (string * Param) list, NpgsqlDataReader, 'T>)
        (ct: CancellationToken)
        : Task<Db.Attempt<'T>> =
        Db.attempt context.Resilience (programSession context.DataSource program) ct

    let runProgramOnce
        (context: PostgresContext)
        (program: StoreProgram<WriteAccess, (string * Param) list, NpgsqlDataReader, 'T>)
        (ct: CancellationToken)
        : Task<Result<'T, StoreError>> =
        Db.protectOnce (programSession context.DataSource program) ct
