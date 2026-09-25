module ByzantineSystems.Automata.Migrate.Program

open System
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging

// Both stores name their migrator and its error the same, so each is reached through an alias.
module PostgresMigrator = ByzantineSystems.Automata.Storage.Postgres.Migrator
module SqliteMigrator = ByzantineSystems.Automata.Storage.Sqlite.Migrator
type private PostgresError = ByzantineSystems.Automata.Storage.Postgres.MigrationError
type private SqliteError = ByzantineSystems.Automata.Storage.Sqlite.MigrationError

let private read name =
    Environment.GetEnvironmentVariable name
    |> Option.ofObj
    |> Option.filter (String.IsNullOrWhiteSpace >> not)

let private readConnectionString () : string option =
    match read "BS_AUTOMATA_CONN" with
    | Some value -> Some value
    | None -> read "ConnectionStrings__BS_AUTOMATA_CONN"

let private applied (logger: ILogger) (count: int) =
    logger.LogInformation("Automata database migrations applied: {Count} scripts run", count)
    0

/// The SQLite store is chosen by naming its file. The file is created if it does not exist, which
/// is what a first deploy of an embedded store looks like.
let private migrateSqlite (logger: ILogger) (path: string) : int =
    let connectionString =
        ByzantineSystems.Automata.Storage.Sqlite.DataSource.connectionString path

    match SqliteMigrator.migrate logger connectionString with
    | Ok report -> applied logger (List.length report.Applied)
    | Error(SqliteError.Failed(script, error)) ->
        logger.LogError(error, "Automata SQLite migration failed at {Script}", script)
        1
    | Error(SqliteError.Unsupported reason) ->
        logger.LogError("Automata SQLite migration refused the database: {Reason}", reason)
        1

let private migratePostgres (logger: ILogger) (connectionString: string) : int =
    match PostgresMigrator.migrate logger connectionString with
    | Ok report -> applied logger (List.length report.Applied)
    | Error(PostgresError.Failed(script, error)) ->
        logger.LogError(error, "Automata database migration failed at {Script}", script)
        1

[<EntryPoint>]
let main args =
    let builder = Host.CreateApplicationBuilder(args)
    use host = builder.Build()

    let logger =
        host.Services.GetRequiredService<ILoggerFactory>().CreateLogger("ByzantineSystems.Automata.Migrate")

    match read "BS_AUTOMATA_SQLITE_PATH", readConnectionString () with
    | Some path, _ -> migrateSqlite logger path
    | None, Some connectionString -> migratePostgres logger connectionString
    | None, None ->
        logger.LogError(
            "No database; set {SqlitePath} for the SQLite store, or {PrimaryVariable} or {FallbackVariable} for PostgreSQL",
            "BS_AUTOMATA_SQLITE_PATH",
            "BS_AUTOMATA_CONN",
            "ConnectionStrings__BS_AUTOMATA_CONN"
        )

        1
