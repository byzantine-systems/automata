module ByzantineSystems.Automata.Migrate.Program

open System
open ByzantineSystems.Automata.Storage.Postgres
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging

let private readConnectionString () : string option =
    let read name =
        Environment.GetEnvironmentVariable name
        |> Option.ofObj
        |> Option.filter (String.IsNullOrWhiteSpace >> not)

    match read "BS_AUTOMATA_CONN" with
    | Some value -> Some value
    | None -> read "ConnectionStrings__BS_AUTOMATA_CONN"

[<EntryPoint>]
let main args =
    let builder = Host.CreateApplicationBuilder(args)
    use host = builder.Build()

    let logger =
        host.Services.GetRequiredService<ILoggerFactory>().CreateLogger("ByzantineSystems.Automata.Migrate")

    match readConnectionString () with
    | None ->
        logger.LogError(
            "No connection string; set {PrimaryVariable} or {FallbackVariable}",
            "BS_AUTOMATA_CONN",
            "ConnectionStrings__BS_AUTOMATA_CONN"
        )

        1
    | Some connectionString ->
        match Migrator.migrate logger connectionString with
        | Ok report ->
            logger.LogInformation(
                "Automata database migrations applied: {Count} scripts run",
                List.length report.Applied
            )

            0
        | Error(MigrationError.Failed(script, error)) ->
            logger.LogError(error, "Automata database migration failed at {Script}", script)
            1
