namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Reflection
open DbUp

/// <summary>
/// Applies the embedded migrations. <c>main</c> scripts run once and are journaled;
/// <c>repeatable</c> scripts hold the routines, which are <c>CREATE OR REPLACE</c> and reapplied
/// whenever their content changes, so editing a routine body is an edit to its own migration
/// rather than a new file.
/// </summary>
[<RequireQualifiedAccess>]
module Migrator =

    let private assembly = Assembly.GetExecutingAssembly()

    /// <summary>Applies all pending migrations. Raises on failure.</summary>
    let migrate (connectionString: string) : unit =
        let mainResult =
            DeployChanges.To
                .PostgresqlDatabase(connectionString)
                .WithScriptsEmbeddedInAssembly(
                    assembly,
                    Func<string, bool>(fun name -> name.Contains(".migrations.main."))
                )
                .WithTransactionPerScript()
                .LogToConsole()
                .Build()
                .PerformUpgrade()

        if not mainResult.Successful then
            raise (mainResult.Error)

        let repeatableOptions = DbUp.Engine.SqlScriptOptions()
        repeatableOptions.ScriptType <- DbUp.Support.ScriptType.RunAlways
        repeatableOptions.RunGroupOrder <- 2

        let repeatableResult =
            DeployChanges.To
                .PostgresqlDatabase(connectionString)
                .WithScriptsEmbeddedInAssembly(
                    assembly,
                    Func<string, bool>(fun name -> name.Contains(".migrations.repeatable.")),
                    repeatableOptions
                )
                .WithTransactionPerScript()
                .LogToConsole()
                .Build()
                .PerformUpgrade()

        if not repeatableResult.Successful then
            raise (repeatableResult.Error)
