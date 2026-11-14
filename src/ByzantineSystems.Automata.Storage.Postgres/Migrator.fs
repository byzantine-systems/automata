namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Reflection
open DbUp
open DbUp.Builder
open DbUp.Engine
open DbUp.Engine.Output
open FsToolkit.ErrorHandling
open Microsoft.Extensions.Logging
open Npgsql

/// <summary>The database and schema to migrate.</summary>
type MigrationTarget =
    {
        /// <summary>The PostgreSQL connection string.</summary>
        ConnectionString: string
        /// <summary>The schema that owns this Automata installation.</summary>
        Schema: SchemaName
    }

/// <summary>What one migration run applied.</summary>
type MigrationReport =
    {
        /// <summary>
        /// The scripts run, by embedded resource name, in order. The numbered scripts appear once
        /// in the database's life; the repeatable ones, which hold the routines and views, are
        /// run on every migration and so appear every time.
        /// </summary>
        Applied: string list
    }

/// <summary>Why a migration run stopped.</summary>
[<RequireQualifiedAccess>]
type MigrationError =
    /// <summary>
    /// A script failed, and nothing after it ran. Each script runs in its own transaction, so
    /// the scripts before it are applied and this one is not. The script is empty when the
    /// failure came before any script, such as a connection that could not be opened.
    /// </summary>
    | Failed of script: string * error: exn

/// <summary>DbUp's log, written through an <c>ILogger</c> instead of the console.</summary>
type private UpgradeLog(logger: ILogger) =
    interface IUpgradeLog with
        member _.LogTrace(format, args) = logger.LogTrace(format, args)
        member _.LogDebug(format, args) = logger.LogDebug(format, args)
        member _.LogInformation(format, args) = logger.LogInformation(format, args)
        member _.LogWarning(format, args) = logger.LogWarning(format, args)
        member _.LogError(format: string, args: obj array) = logger.LogError(format, args)
        member _.LogError(error: exn, format: string, args: obj array) = logger.LogError(error, format, args)

type private TemplatePreprocessor(schema: SchemaName) =
    interface IScriptPreprocessor with
        member _.Process(contents) = SqlTemplate.substitute schema contents

/// <summary>
/// Applies the embedded migrations. <c>main</c> scripts run once and are journaled;
/// <c>repeatable</c> scripts hold the routines, and run on every migration.
///
/// A library does not write to the console, so DbUp logs through the logger it is given. A
/// failure is a value naming the script that failed, because "which one" is the first question
/// anybody asks.
/// </summary>
[<RequireQualifiedAccess>]
module Migrator =

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

    let private configure (target: MigrationTarget) (builder: UpgradeEngineBuilder) =
        let schema = SchemaName.value target.Schema

        builder
            .JournalToPostgresqlTable(schema, "schemaversions")
            .WithVariablesDisabled()
            .WithPreprocessor(TemplatePreprocessor target.Schema)

    /// <summary>
    /// Moves a pre-schema-configuration journal into the schema that now owns it.
    ///
    /// Before the schema was configurable, DbUp journaled to <c>public.schemaversions</c> while
    /// creating every object in <c>fsm</c>. To such a database the schema-local journal makes
    /// every numbered script look pending, and the replay fails on objects that already exist.
    /// Adoption runs only when the default schema is already installed and the schema-local
    /// journal is absent, so a deliberate reset — which drops the schema — is followed by a
    /// fresh migration rather than an adopted journal.
    /// </summary>
    let private adoptLegacyJournal (target: MigrationTarget) =
        if target.Schema <> SchemaName.defaults then
            Ok()
        else
            let schema = SchemaName.value target.Schema

            try
                use connection = new NpgsqlConnection(target.ConnectionString)
                connection.Open()
                use command = connection.CreateCommand()

                command.CommandText <-
                    $"""DO $$
                    BEGIN
                        IF TO_REGNAMESPACE('{schema}') IS NOT NULL
                           AND TO_REGCLASS('public.schemaversions') IS NOT NULL
                           AND TO_REGCLASS('{schema}.schemaversions') IS NULL THEN
                            CREATE TABLE {schema}.schemaversions (
                                schemaversionsid serial PRIMARY KEY,
                                scriptname varchar(255) NOT NULL,
                                applied timestamp NOT NULL
                            );
                            INSERT INTO {schema}.schemaversions (scriptname, applied)
                            SELECT scriptname, applied FROM public.schemaversions;
                            DROP TABLE public.schemaversions;
                        END IF;
                    END
                    $$;"""

                command.ExecuteNonQuery() |> ignore
                Ok()
            with error ->
                Error(MigrationError.Failed("", error))

    let private bootstrap (target: MigrationTarget) =
        let schema = SchemaName.value target.Schema

        try
            use connection = new NpgsqlConnection(target.ConnectionString)
            connection.Open()
            use command = connection.CreateCommand()

            command.CommandText <-
                $"""CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public;
                    CREATE EXTENSION IF NOT EXISTS pgmq;
                    DO $$
                    BEGIN
                        IF (
                            SELECT r.rolsuper
                            FROM pg_roles r
                            WHERE r.rolname = CURRENT_USER
                        ) THEN
                            IF CURRENT_DATABASE() = CURRENT_SETTING('cron.database_name', TRUE)
                               AND EXISTS (
                                   SELECT 1
                                   FROM pg_available_extensions a
                                   WHERE a.name = 'pg_cron'
                               ) THEN
                                CREATE EXTENSION IF NOT EXISTS pg_cron;
                            END IF;
                        END IF;
                    END
                    $$;
                    CREATE SCHEMA IF NOT EXISTS {schema};"""

            command.ExecuteNonQuery() |> ignore
            Ok()
        with error ->
            Error(MigrationError.Failed("", error))

    let private main (log: IUpgradeLog) (target: MigrationTarget) =
        DeployChanges.To
            .PostgresqlDatabase(target.ConnectionString, SchemaName.value target.Schema)
            .WithScriptsEmbeddedInAssembly(assembly, Func<string, bool>(fun name -> name.Contains ".migrations.main."))
            .WithTransactionPerScript()
            .LogTo(log)
        |> configure target
        |> fun (builder: UpgradeEngineBuilder) -> builder.Build().PerformUpgrade()
        |> outcome

    let private repeatable (log: IUpgradeLog) (target: MigrationTarget) =
        let options =
            SqlScriptOptions(ScriptType = DbUp.Support.ScriptType.RunAlways, RunGroupOrder = 2)

        DeployChanges.To
            .PostgresqlDatabase(target.ConnectionString, SchemaName.value target.Schema)
            .WithScriptsEmbeddedInAssembly(
                assembly,
                Func<string, bool>(fun name -> name.Contains ".migrations.repeatable."),
                options
            )
            .WithTransactionPerScript()
            .LogTo(log)
        |> configure target
        |> fun (builder: UpgradeEngineBuilder) -> builder.Build().PerformUpgrade()
        |> outcome

    /// <summary>
    /// Applies every pending migration: the numbered scripts first, then the repeatable ones,
    /// which may depend on them. Stops at the first failure.
    /// </summary>
    let migrate (logger: ILogger) (target: MigrationTarget) : Result<MigrationReport, MigrationError> =
        let log = UpgradeLog logger

        result {
            do! bootstrap target
            do! adoptLegacyJournal target
            let! numbered = main log target
            let! routines = repeatable log target
            return { Applied = numbered @ routines }
        }
