namespace ByzantineSystems.Automata.DependencyInjection

open System
open System.Runtime.CompilerServices
open ByzantineSystems.Automata.Resilience
open ByzantineSystems.Automata.Runtime
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Postgres
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Options
open Npgsql

/// The parts of the PostgreSQL registration every factory needs. Reading settings through
/// <c>IOptions&lt;_&gt;.Value</c> runs the validators registered below, so an invalid
/// configuration is refused — with the failures listed — before any factory here runs; the
/// guards on the impossible branches exist for the provider built without a host and its
/// startup validation, and are wiring defects rather than configuration outcomes.
module private PostgresRegistration =

    let settingsOf (provider: IServiceProvider) : PostgresStoreSettings =
        provider.GetRequiredService<IOptions<PostgresStoreSettings>>().Value

    let resolvedOf (provider: IServiceProvider) : SchemaName * ListenerMode =
        match PostgresStoreSettings.tryResolve (settingsOf provider) with
        | Ok resolved -> resolved
        | Error failures ->
            let joined = String.concat "; " failures

            invalidOp
                $"The PostgreSQL store settings are invalid, which host startup validation exists to prevent: {joined}"

    let connectionStringOf (configuration: IConfiguration) (name: string) : string =
        match configuration.GetConnectionString name with
        | null ->
            invalidOp
                $"The connection string '{name}' is not configured. It is checked at host startup; resolving it by hand requires the same configuration."
        | connection -> connection

[<Extension>]
type ServiceCollectionExtensions =

    /// <summary>
    /// Registers one typed automata host: a supervised generation running the command processor
    /// and, optionally, the action dispatcher.
    ///
    /// No resilience pipeline is registered here any more. Retrying a call to the database is
    /// the store's business, because the store is the layer that still has driver exceptions to
    /// classify; retrying a command is the inbox's, because that has to survive this process.
    /// A host configures the first when it builds its store context and the second through the
    /// machine's processor policy.
    /// </summary>
    [<Extension>]
    static member AddAutomata<'EntityId, 'State, 'Event, 'Action, 'Err, 'EffectError when 'EntityId: equality>
        (services: IServiceCollection, options: AutomataOptions<'EntityId, 'State, 'Event, 'Action, 'Err, 'EffectError>)
        : IServiceCollection =
        if isNull services then
            nullArg (nameof services)

        if isNull options.MachineKey || String.IsNullOrWhiteSpace options.MachineKey then
            invalidArg (nameof options) "MachineKey must be non-empty."

        // A Generic Host already supplies logging. AddLogging also makes this extension safe
        // in a plain service collection, where the default is a no-op logger until configured.
        services.AddLogging() |> ignore

        services.AddSingleton(options) |> ignore

        services.AddSingleton<IHostedService>(
            Func<IServiceProvider, IHostedService>(fun provider ->
                let scopeFactory = provider.GetRequiredService<IServiceScopeFactory>()

                let logger =
                    provider.GetRequiredService<
                        ILogger<AutomataHostedService<'EntityId, 'State, 'Event, 'Action, 'Err, 'EffectError>>
                     >()

                let auditStore =
                    provider.GetRequiredService<ByzantineSystems.Automata.Storage.ISupervisionEventStore>()

                new AutomataHostedService<'EntityId, 'State, 'Event, 'Action, 'Err, 'EffectError>(
                    provider,
                    scopeFactory,
                    auditStore,
                    options,
                    logger
                )
                :> IHostedService)
        )
        |> ignore

        services

    /// <summary>Registers a factory that cannot return machine configuration errors.</summary>
    [<Extension>]
    static member AddAutomataMachine<'EntityId, 'State, 'Event, 'Action, 'Err, 'EffectError when 'EntityId: equality>
        (
            services: IServiceCollection,
            options: AutomataOptions<'EntityId, 'State, 'Event, 'Action, 'Err, 'EffectError>,
            factory: IServiceProvider -> Machine<'EntityId, 'State, 'Event, 'Action, 'Err>
        ) : IServiceCollection =
        services.AddAutomata(
            { options with
                MachineFactory = fun provider -> Ok(factory provider) }
        )

    /// <summary>
    /// Registers database maintenance: notification, lease reaping, retention and drift
    /// reporting, run by this process or scheduled into the database, per
    /// <see cref="T:ByzantineSystems.Automata.DependencyInjection.MaintenanceScheduler" />.
    ///
    /// Register it once per database, not once per machine. Every machine registers itself with
    /// the database when it boots, and a pass covers all of them. Running it on several hosts is
    /// safe, since a pass is single-flight in the database, and only redundant.
    /// </summary>
    [<Extension>]
    static member AddAutomataMaintenance
        (services: IServiceCollection, options: MaintenanceOptions)
        : IServiceCollection =
        if isNull services then
            nullArg (nameof services)

        if options.NotifyEvery <= TimeSpan.Zero then
            invalidArg (nameof options) "NotifyEvery must be a positive duration."

        if options.RunEvery <= TimeSpan.Zero then
            invalidArg (nameof options) "RunEvery must be a positive duration."

        let batch = ClaimBatch.create options.Batch

        services.AddLogging() |> ignore

        services.AddSingleton<IHostedService>(
            Func<IServiceProvider, IHostedService>(fun provider ->
                new MaintenanceService(
                    options,
                    batch,
                    options.Maintenance provider,
                    provider.GetRequiredService<ILogger<MaintenanceService>>()
                )
                :> IHostedService)
        )

        |> ignore

        services

    /// <summary>
    /// Registers the PostgreSQL store's configuration surface: one connection pool and one
    /// <see cref="T:ByzantineSystems.Automata.Storage.Postgres.PostgresContext" /> for the
    /// whole application, plus the listener placement the settings chose.
    ///
    /// The settings bind from the <c>Automata:Postgres</c> section of the given root
    /// configuration, and the connection string is resolved by name from that same
    /// configuration, so the section and the connection strings must live in one tree. They
    /// are validated on host start: a bad schema name, a listener half-configured, or a named
    /// connection string that does not exist stops the host before a pool is built or a
    /// connection opened. Typed machine stores are still built by their machines — their
    /// codecs and queues are per-machine facts — but they take the registered context and
    /// <see cref="T:ByzantineSystems.Automata.Storage.Postgres.ListenerConnection" />
    /// rather than wiring their own.
    /// </summary>
    [<Extension>]
    static member AddAutomataPostgres
        (services: IServiceCollection, configuration: IConfiguration)
        : IServiceCollection =
        if isNull services then
            nullArg (nameof services)

        if isNull configuration then
            nullArg (nameof configuration)

        services
            .AddOptions<PostgresStoreSettings>()
            .Bind(configuration.GetSection "Automata:Postgres")
            .Validate(
                Func<PostgresStoreSettings, bool>(fun settings ->
                    not (String.IsNullOrWhiteSpace settings.ConnectionStringName)
                    && not (isNull (configuration.GetConnectionString settings.ConnectionStringName))),
                "The connection string named by ConnectionStringName must exist in the host's configuration."
            )
            .ValidateOnStart()
        |> ignore

        services
            .AddSingleton<IValidateOptions<PostgresStoreSettings>, PostgresStoreSettingsValidator>()
            .AddSingleton<BeliefModel>(fun provider ->
                let settings = PostgresRegistration.settingsOf provider

                match BeliefModel.tryCreate settings.Beliefs with
                | Ok model -> model
                | Error reason ->
                    raise (OptionsValidationException(Options.DefaultName, typeof<PostgresStoreSettings>, [ reason ])))
            .AddSingleton<NpgsqlDataSource>(fun provider ->
                // The pool, once per application: the registered context and every store share
                // it, which is the sharing the resilience pipeline is sized for. The
                // configuration is the one this registration was given, not a provider lookup:
                // a bare service collection has no IConfiguration to find.
                DataSource.create (
                    PostgresRegistration.connectionStringOf
                        configuration
                        (PostgresRegistration.settingsOf provider).ConnectionStringName
                ))
            .AddSingleton<PostgresContext>(fun provider ->
                let source = provider.GetRequiredService<NpgsqlDataSource>()
                let schema, _ = PostgresRegistration.resolvedOf provider

                PostgresContext.create source TransientPolicy.defaults ignore
                |> PostgresContext.withSchema schema)
            .AddSingleton<ListenerConnection>(fun provider ->
                match PostgresRegistration.resolvedOf provider with
                | _, ListenerMode.Off -> ListenerConnection.Off
                | _, ListenerMode.SameDataSource -> ListenerConnection.SameDataSource
                | _, ListenerMode.Direct name ->
                    ListenerConnection.Direct(PostgresRegistration.connectionStringOf configuration name))
        |> ignore

        services
