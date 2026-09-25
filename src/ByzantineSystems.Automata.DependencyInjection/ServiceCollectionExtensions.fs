namespace ByzantineSystems.Automata.DependencyInjection

open System
open System.Runtime.CompilerServices
open ByzantineSystems.Automata.Runtime
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging

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

        if options.Batch < 1 then
            invalidArg (nameof options) "Batch must be positive."

        services.AddLogging() |> ignore

        services.AddSingleton<IHostedService>(
            Func<IServiceProvider, IHostedService>(fun provider ->
                new MaintenanceService(
                    options,
                    options.Maintenance provider,
                    provider.GetRequiredService<ILogger<MaintenanceService>>()
                )
                :> IHostedService)
        )
        |> ignore

        services
