namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Reflection
open DbUp
open DbUp.Engine
open DbUp.Engine.Output
open FsToolkit.ErrorHandling
open Microsoft.Extensions.Logging

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

    let private main (log: IUpgradeLog) (connectionString: string) =
        DeployChanges.To
            .PostgresqlDatabase(connectionString)
            .WithScriptsEmbeddedInAssembly(assembly, Func<string, bool>(fun name -> name.Contains ".migrations.main."))
            .WithTransactionPerScript()
            .LogTo(log)
            .Build()
            .PerformUpgrade()
        |> outcome

    let private repeatable (log: IUpgradeLog) (connectionString: string) =
        let options =
            SqlScriptOptions(ScriptType = DbUp.Support.ScriptType.RunAlways, RunGroupOrder = 2)

        DeployChanges.To
            .PostgresqlDatabase(connectionString)
            .WithScriptsEmbeddedInAssembly(
                assembly,
                Func<string, bool>(fun name -> name.Contains ".migrations.repeatable."),
                options
            )
            .WithTransactionPerScript()
            .LogTo(log)
            .Build()
            .PerformUpgrade()
        |> outcome

    /// <summary>
    /// Applies every pending migration: the numbered scripts first, then the repeatable ones,
    /// which may depend on them. Stops at the first failure.
    /// </summary>
    let migrate (logger: ILogger) (connectionString: string) : Result<MigrationReport, MigrationError> =
        let log = UpgradeLog logger

        result {
            let! numbered = main log connectionString
            let! routines = repeatable log connectionString
            return { Applied = numbered @ routines }
        }
