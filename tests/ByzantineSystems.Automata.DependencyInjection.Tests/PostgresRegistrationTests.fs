module ByzantineSystems.Automata.DependencyInjection.Tests.PostgresRegistrationTests

open System
open System.IO
open ByzantineSystems.Automata.DependencyInjection
open ByzantineSystems.Automata.Storage.Postgres
open Expecto
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Options
open Microsoft.Extensions.Hosting
open Npgsql

/// A connection string that is never connected to: building a pool opens nothing, which is
/// the point — the assertions here are about what is refused BEFORE anything connects.
let private neverConnected =
    "Host=127.0.0.1;Port=5432;Database=never;Username=never;Password=never"

/// Configuration from a written JSON file, because that is the tree a host reads and the
/// binder is part of what is under test.
let private withConfiguration (settings: string) (assertions: IConfiguration -> unit) =
    let path =
        Path.Combine(Path.GetTempPath(), $"automata-postgres-di-{Guid.NewGuid():N}.json")

    File.WriteAllText(
        path,
        $"""{{ "ConnectionStrings": {{ "BS_AUTOMATA_CONN": "{neverConnected}" }}, "Automata": {{ "Postgres": {settings} }} }}"""
    )

    try
        assertions (ConfigurationBuilder().AddJsonFile(path).Build())
    finally
        File.Delete path

let private providerFor (configuration: IConfiguration) =
    ServiceCollection().AddAutomataPostgres(configuration).BuildServiceProvider()

let postgresRegistrationTests =
    testList
        "AddAutomataPostgres"
        [ test "a valid configuration resolves one pool and one context on the chosen schema" {
              withConfiguration """{ "Schema": "automata_ci" }""" (fun configuration ->
                  use provider = providerFor configuration
                  let settings = provider.GetRequiredService<IOptions<PostgresStoreSettings>>().Value

                  Expect.equal settings.Schema "automata_ci" "the section bound"
                  Expect.equal (provider.GetRequiredService<BeliefModel>()) BeliefModel.Snapshot "default profile"

                  let context = provider.GetRequiredService<PostgresContext>()

                  Expect.equal (SchemaName.value context.Schema) "automata_ci" "the context carries the chosen schema"

                  Expect.equal
                      (provider.GetRequiredService<NpgsqlDataSource>())
                      (provider.GetRequiredService<NpgsqlDataSource>())
                      "one pool for the application, not one per ask"

                  Expect.equal
                      (provider.GetRequiredService<PostgresContext>())
                      context
                      "one context for the application"

                  Expect.equal
                      (provider.GetRequiredService<ListenerConnection>())
                      ListenerConnection.SameDataSource
                      "the default listener placement")
          }

          test "an invalid belief profile fails host startup before a data source factory runs" {
              withConfiguration """{ "Beliefs": "unknown" }""" (fun configuration ->
                  let builder =
                      Host.CreateApplicationBuilder(HostApplicationBuilderSettings(DisableDefaults = true))

                  builder.Configuration.AddConfiguration(configuration) |> ignore
                  builder.Services.AddAutomataPostgres(builder.Configuration) |> ignore
                  let mutable constructed = false

                  builder.Services.AddSingleton<NpgsqlDataSource>(fun _ ->
                      constructed <- true
                      DataSource.create neverConnected)
                  |> ignore

                  use host = builder.Build()

                  Expect.throwsT<OptionsValidationException>
                      (fun () -> host.StartAsync().GetAwaiter().GetResult())
                      "startup validates the model"

                  Expect.isFalse constructed "no pool was constructed")
          }

          test "a direct listener resolves its own connection string" {
              withConfiguration
                  """{ "Listener": { "Mode": "direct", "ConnectionStringName": "BS_AUTOMATA_CONN" } }"""
                  (fun configuration ->
                      use provider = providerFor configuration

                      match provider.GetRequiredService<ListenerConnection>() with
                      | ListenerConnection.Direct connection ->
                          Expect.stringContains connection "Host=127.0.0.1" "the named string, resolved"
                      | other -> failtestf "expected a direct listener, got %A" other)
          }

          test "a bad schema name is refused before anything resolves" {
              withConfiguration """{ "Schema": "Public" }""" (fun configuration ->
                  use provider = providerFor configuration

                  Expect.throwsT<OptionsValidationException>
                      (fun () -> provider.GetRequiredService<PostgresContext>() |> ignore)
                      "reading the settings refuses the schema")
          }

          test "a named connection string that does not exist is refused" {
              withConfiguration """{ "ConnectionStringName": "NOT_CONFIGURED_ANYWHERE" }""" (fun configuration ->
                  use provider = providerFor configuration

                  Expect.throwsT<OptionsValidationException>
                      (fun () -> provider.GetRequiredService<NpgsqlDataSource>() |> ignore)
                      "the missing name is a validation failure, not a pool failure")
          } ]
