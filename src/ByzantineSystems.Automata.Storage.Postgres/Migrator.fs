namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Reflection
open System.IO
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
        /// <summary>The requested PostgreSQL storage profile.</summary>
        Beliefs: BeliefModel
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
    /// <summary>Disabling bitemporality requires an explicit reset, never an automatic drop.</summary>
    | ProfileMismatch of configured: BeliefModel * installed: BeliefModel

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

    let private bootstrap (target: MigrationTarget) =
        let schema = SchemaName.value target.Schema

        try
            use connection = new NpgsqlConnection(target.ConnectionString)
            connection.Open()
            use command = connection.CreateCommand()

            command.CommandText <-
                $"""CREATE EXTENSION IF NOT EXISTS pgmq;
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

            match target.Beliefs with
            | BeliefModel.Snapshot -> ()
            | BeliefModel.Bitemporal ->
                command.CommandText <-
                    $"CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public; CREATE SCHEMA IF NOT EXISTS {schema}_temporal;"

                command.ExecuteNonQuery() |> ignore

            Ok()
        with error ->
            Error(MigrationError.Failed("", error))

    let private main (log: IUpgradeLog) (target: MigrationTarget) =
        DeployChanges.To
            .PostgresqlDatabase(target.ConnectionString, SchemaName.value target.Schema)
            .WithScriptsEmbeddedInAssembly(
                assembly,
                Func<string, bool>(fun name -> name.Contains ".migrations.common.main.")
            )
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
                Func<string, bool>(fun name -> name.Contains ".migrations.common.repeatable."),
                options
            )
            .WithTransactionPerScript()
            .LogTo(log)
        |> configure target
        |> fun (builder: UpgradeEngineBuilder) -> builder.Build().PerformUpgrade()
        |> outcome

    let private checkProfile (target: MigrationTarget) =
        try
            use connection = new NpgsqlConnection(target.ConnectionString)
            connection.Open()
            use command = connection.CreateCommand()
            command.CommandText <- $"SELECT beliefs FROM {SchemaName.value target.Schema}.store_profile"

            match command.ExecuteScalar() :?> string, target.Beliefs with
            | "bitemporal", BeliefModel.Snapshot ->
                Error(MigrationError.ProfileMismatch(BeliefModel.Snapshot, BeliefModel.Bitemporal))
            | _ -> Ok()
        with error ->
            Error(MigrationError.Failed("", error))

    let private profile (log: IUpgradeLog) (target: MigrationTarget) =
        let group, journalSchema, journalTable =
            match target.Beliefs with
            | BeliefModel.Snapshot -> "snapshot", SchemaName.value target.Schema, "snapshot_schemaversions"
            | BeliefModel.Bitemporal -> "bitemporal", $"{SchemaName.value target.Schema}_temporal", "schemaversions"

        let scripts =
            assembly.GetManifestResourceNames()
            |> Array.filter (fun name -> name.Contains $".migrations.{group}.")
            |> Array.map (fun name ->
                use stream = assembly.GetManifestResourceStream name
                use reader = new StreamReader(stream)

                let options =
                    if name.Contains ".main." then
                        SqlScriptOptions(ScriptType = DbUp.Support.ScriptType.RunOnce, RunGroupOrder = 0)
                    else
                        SqlScriptOptions(ScriptType = DbUp.Support.ScriptType.RunAlways, RunGroupOrder = 2)

                SqlScript(name, reader.ReadToEnd(), options))

        DeployChanges.To
            .PostgresqlDatabase(target.ConnectionString, SchemaName.value target.Schema)
            .WithScripts(scripts)
            .JournalToPostgresqlTable(journalSchema, journalTable)
            .WithVariablesDisabled()
            .WithPreprocessor(TemplatePreprocessor target.Schema)
            .WithTransaction()
            .LogTo(log)
            .Build()
            .PerformUpgrade()
        |> outcome

    /// <summary>
    /// Applies every pending migration: the numbered scripts first, then the repeatable ones,
    /// which may depend on them. Stops at the first failure.
    /// </summary>
    let migrate (logger: ILogger) (target: MigrationTarget) : Result<MigrationReport, MigrationError> =
        let log = UpgradeLog logger

        result {
            do! bootstrap target
            let! numbered = main log target
            do! checkProfile target
            let! routines = repeatable log target

            let! selected = profile log target
            return { Applied = numbered @ routines @ selected }
        }
