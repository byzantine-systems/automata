module ByzantineSystems.Automata.Migrate.Program

open System
open ByzantineSystems.Automata.Storage.Postgres
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

let private readSchemaArgument (args: string array) =
    let rec loop selected remaining =
        match remaining with
        | [] -> Ok selected
        | "--schema" :: value :: tail when not (value.StartsWith "--") -> loop (Some value) tail
        | argument :: tail when argument.StartsWith("--schema=", StringComparison.Ordinal) ->
            loop (Some(argument.Substring("--schema=".Length))) tail
        | "--schema" :: _ -> Error "--schema requires a value"
        | _ :: tail -> loop selected tail

    loop None (List.ofArray args)

let private readSchema args =
    readSchemaArgument args
    |> Result.bind (fun argument ->
        let value = argument |> Option.orElseWith (fun () -> read "BS_AUTOMATA_SCHEMA")

        match value with
        | None -> Ok SchemaName.defaults
        | Some name ->
            SchemaName.tryCreate name
            |> Result.mapError (fun error -> $"Invalid Automata schema '{name}': %A{error}"))

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

let private migratePostgres (logger: ILogger) (connectionString: string) (schema: SchemaName) beliefs : int =
    match
        PostgresMigrator.migrate
            logger
            { ConnectionString = connectionString
              Schema = schema
              Beliefs = beliefs }
    with
    | Ok report -> applied logger (List.length report.Applied)
    | Error(PostgresError.Failed(script, error)) ->
        logger.LogError(error, "Automata database migration failed at {Script}", script)
        1
    | Error(PostgresError.ProfileMismatch(configured, installed)) ->
        logger.LogError(
            "Cannot switch installed {Installed} profile to {Configured}; reset explicitly.",
            BeliefModel.value installed,
            BeliefModel.value configured
        )

        1

[<EntryPoint>]
let main args =
    let builder = Host.CreateApplicationBuilder(args)
    use host = builder.Build()

    let logger =
        host.Services.GetRequiredService<ILoggerFactory>().CreateLogger("ByzantineSystems.Automata.Migrate")

    match read "BS_AUTOMATA_SQLITE_PATH", readConnectionString () with
    | Some path, _ -> migrateSqlite logger path
    | None, Some connectionString ->
        match readSchema args with
        | Ok schema ->
            let selected =
                args
                |> Array.tryFindIndex ((=) "--beliefs")
                |> Option.bind (fun index -> args |> Array.tryItem (index + 1))
                |> Option.orElseWith (fun () ->
                    args
                    |> Array.tryPick (fun arg ->
                        if arg.StartsWith("--beliefs=", StringComparison.Ordinal) then
                            Some(arg.Substring(10))
                        else
                            None))
                |> Option.orElseWith (fun () -> read "BS_AUTOMATA_BELIEFS")
                |> Option.defaultValue "snapshot"

            match BeliefModel.tryCreate selected with
            | Ok beliefs -> migratePostgres logger connectionString schema beliefs
            | Error error ->
                logger.LogError("{Error}", error)
                1
        | Error error ->
            logger.LogError("{Error}", error)
            1
    | None, None ->
        logger.LogError(
            "No database; set {SqlitePath} for the SQLite store, or {PrimaryVariable} or {FallbackVariable} for PostgreSQL",
            "BS_AUTOMATA_SQLITE_PATH",
            "BS_AUTOMATA_CONN",
            "ConnectionStrings__BS_AUTOMATA_CONN"
        )

        1
