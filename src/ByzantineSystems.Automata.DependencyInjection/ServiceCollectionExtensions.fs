namespace ByzantineSystems.Automata.DependencyInjection

open System
open System.Runtime.CompilerServices
open ByzantineSystems.Automata.Resilience
open ByzantineSystems.Automata.Runtime
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Polly
open Polly.Registry

[<Extension>]
type ServiceCollectionExtensions =

    /// Registers one typed automata host and its shared keyed Polly pipeline.
    [<Extension>]
    static member AddAutomata<'EntityId, 'State, 'Event, 'Action, 'Err, 'EffectError when 'EntityId: equality>
        (services: IServiceCollection, options: AutomataOptions<'EntityId, 'State, 'Event, 'Action, 'Err, 'EffectError>)
        : IServiceCollection =
        if isNull services then
            nullArg (nameof services)

        if isNull options.MachineKey || String.IsNullOrWhiteSpace options.MachineKey then
            invalidArg (nameof options) "MachineKey must be non-empty."

        services.AddResiliencePipeline<string, PipelineResult<'Err>>(
            options.MachineKey,
            fun builder ->
                let configured = RetryConfig.toPipeline options.Retry ignore
                ResiliencePipelineBuilderExtensions.AddPipeline(builder, configured) |> ignore
        )
        |> ignore

        services.AddSingleton(options) |> ignore

        services.AddSingleton<IHostedService>(
            Func<IServiceProvider, IHostedService>(fun provider ->
                let pipelines = provider.GetRequiredService<ResiliencePipelineProvider<string>>()
                let pipeline = pipelines.GetPipeline<PipelineResult<'Err>>(options.MachineKey)
                let scopeFactory = provider.GetRequiredService<IServiceScopeFactory>()

                let auditStore =
                    provider.GetRequiredService<ByzantineSystems.Automata.Storage.ISupervisionEventStore>()

                new AutomataHostedService<'EntityId, 'State, 'Event, 'Action, 'Err, 'EffectError>(
                    provider,
                    scopeFactory,
                    auditStore,
                    pipeline,
                    options
                )
                :> IHostedService)
        )
        |> ignore

        services

    /// Registers a factory that cannot return machine configuration errors.
    [<Extension>]
    static member AddAutomataMachine<'EntityId, 'State, 'Event, 'Action, 'Err, 'EffectError when 'EntityId: equality>
        (
            services: IServiceCollection,
            options: AutomataOptions<'EntityId, 'State, 'Event, 'Action, 'Err, 'EffectError>,
            factory:
                IServiceProvider
                    -> ResiliencePipeline<PipelineResult<'Err>>
                    -> Machine<'EntityId, 'State, 'Event, 'Action, 'Err>
        ) : IServiceCollection =
        services.AddAutomata(
            { options with
                MachineFactory = fun provider pipeline -> Ok(factory provider pipeline) }
        )
