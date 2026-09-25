namespace ByzantineSystems.Automata.Storage.Sqlite

open System
open System.Reflection
open DbUp
open DbUp.Engine
open DbUp.Engine.Output
open FsToolkit.ErrorHandling
open Microsoft.Data.Sqlite
open Microsoft.Extensions.Logging

/// <summary>What one migration run applied.</summary>
type MigrationReport =
    {
        /// <summary>
        /// The scripts run, by embedded resource name, in order. Each appears once in a
        /// database's life: SQLite has no routines, so there are no repeatable scripts.
        /// </summary>
        Applied: string list
    }

/// <summary>Why a migration run stopped.</summary>
[<RequireQualifiedAccess>]
type MigrationError =
    /// <summary>
    /// A script failed, and nothing after it ran. Each script runs in its own transaction, so
    /// the scripts before it are applied and this one is not. The script is empty when the
    /// failure came before any script, such as a file that could not be opened.
    /// </summary>
    | Failed of script: string * error: exn

    /// <summary>
    /// The database cannot host this schema, found before any script ran: an in-memory
    /// database, a file that would not switch to WAL, or a SQLite older than the schema needs.
    /// </summary>
    | Unsupported of reason: string

/// <summary>DbUp's log, written through an <c>ILogger</c> instead of the console.</summary>
type private UpgradeLog(logger: ILogger) =
    interface IUpgradeLog with
        member _.LogTrace(format, args) = logger.LogTrace(format, args)
        member _.LogDebug(format, args) = logger.LogDebug(format, args)
        member _.LogInformation(format, args) = logger.LogInformation(format, args)
        member _.LogWarning(format, args) = logger.LogWarning(format, args)
        member _.LogError(format: string, args: obj array) = logger.LogError(format, args)
        member _.LogError(error: exn, format: string, args: obj array) = logger.LogError(error, format, args)

/// <summary>
/// Applies the embedded migrations, each once and journaled in <c>SchemaVersions</c>.
///
/// Run it from one process at a time, as a deploy step. DbUp does not lock the journal, so two
/// processes migrating the same file at once can both try the same script, and the second
/// fails on an object the first created.
/// </summary>
[<RequireQualifiedAccess>]
module Migrator =

    /// <summary>
    /// The oldest SQLite this store runs on. 3.38 added the <c>-&gt;</c> operator, which the
    /// commit uses to fan an action list out as JSON text; 3.37 added the <c>STRICT</c> tables
    /// every table here is; 3.35 added <c>RETURNING</c>. The bundled SQLite is far newer, so
    /// this matters only to a host that swaps in its own.
    /// </summary>
    let minimumVersion = Version(3, 38, 0)

    let private assembly = Assembly.GetExecutingAssembly()

    let private scriptName (script: SqlScript) =
        match script with
        | null -> ""
        | script -> script.Name

    let private outcome (result: DatabaseUpgradeResult) : Result<string list, MigrationError> =
        if result.Successful then
            Ok(result.Scripts |> Seq.map _.Name |> List.ofSeq)
        else
            Error(MigrationError.Failed(scriptName result.ErrorScript, result.Error))

    let private scalar (conn: SqliteConnection) (sql: string) : string =
        use cmd = conn.CreateCommand()
        cmd.CommandText <- sql
        string (cmd.ExecuteScalar())

    /// <summary>
    /// Checks the database can host the schema and puts it in WAL mode, before DbUp opens it.
    ///
    /// WAL is set here rather than in a script for two reasons. It is a property of the file,
    /// kept across connections, so once is enough; and it cannot change inside a transaction,
    /// which is where DbUp runs every script. The pragma answers with the mode now in force
    /// rather than failing, and on a filesystem that cannot share memory between processes it
    /// quietly stays where it was, so the answer is read and checked.
    /// </summary>
    let private prepare (connectionString: string) : Result<unit, MigrationError> =
        if DataSource.isInMemory connectionString then
            Error(
                MigrationError.Unsupported
                    "an in-memory database: each pooled connection would open a different one, and it cannot run in WAL mode"
            )
        else
            try
                use conn = new SqliteConnection(connectionString)
                conn.Open()

                let version = Version.Parse(scalar conn "SELECT sqlite_version();")

                if version < minimumVersion then
                    Error(
                        MigrationError.Unsupported
                            $"SQLite %O{version}, older than the %O{minimumVersion} this schema needs"
                    )
                else
                    match scalar conn "PRAGMA journal_mode = WAL;" with
                    | "wal" -> Ok()
                    | other ->
                        Error(MigrationError.Unsupported $"the database stayed in %s{other} mode rather than WAL")
            with :? SqliteException as error ->
                Error(MigrationError.Failed("", error))

    let private main (log: IUpgradeLog) (connectionString: string) =
        DeployChanges.To
            .SqliteDatabase(connectionString)
            .WithScriptsEmbeddedInAssembly(assembly, Func<string, bool>(fun name -> name.Contains ".migrations.main."))
            .WithTransactionPerScript()
            .LogTo(log)
            .Build()
            .PerformUpgrade()
        |> outcome

    /// <summary>
    /// Refreshes the planner's statistics after a run that changed the schema. SQLite
    /// recommends <c>PRAGMA optimize</c> after schema changes from 3.46 on, and a new index
    /// with no statistics is one the planner may pass over. Skipped when nothing was applied,
    /// so a routine boot-time migration costs nothing.
    /// </summary>
    let private optimize (connectionString: string) (applied: string list) : Result<unit, MigrationError> =
        if List.isEmpty applied then
            Ok()
        else
            try
                use conn = new SqliteConnection(connectionString)
                conn.Open()
                scalar conn "PRAGMA optimize;" |> ignore
                Ok()
            with :? SqliteException as error ->
                Error(MigrationError.Failed("", error))

    /// <summary>
    /// Applies every pending migration, after checking the database can host them. Stops at the
    /// first failure.
    /// </summary>
    let migrate (logger: ILogger) (connectionString: string) : Result<MigrationReport, MigrationError> =
        result {
            do! prepare connectionString
            let! applied = main (UpgradeLog logger) connectionString
            do! optimize connectionString applied
            return { Applied = applied }
        }
