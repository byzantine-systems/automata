namespace ByzantineSystems.Automata.Runtime

open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage

/// <summary>
/// Every chart version a machine can still replay, keyed by the version commands pinned.
///
/// All versions share one set of state, event, action and error types, and one set of codecs.
/// An event type that changed shape between versions cannot be replayed under the older chart.
/// </summary>
type ChartCatalog<'State, 'Event, 'Action, 'Err> =
    private
        { Charts: Map<ChartVersion, Chart<'State, 'Event, 'Action, 'Err>> }

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Runtime.ChartCatalog`4" />.</summary>
[<RequireQualifiedAccess>]
module ChartCatalog =

    /// <summary>A catalog with no charts. The machine's current chart is added when it is built.</summary>
    let empty<'State, 'Event, 'Action, 'Err> : ChartCatalog<'State, 'Event, 'Action, 'Err> =
        { Charts = Map.empty }

    /// <summary>A catalog of earlier charts, each under the version it was declared with.</summary>
    let ofList
        (charts: (int * Chart<'State, 'Event, 'Action, 'Err>) list)
        : ChartCatalog<'State, 'Event, 'Action, 'Err> =
        { Charts =
            charts
            |> List.map (fun (version, chart) -> ChartVersion.create version, chart)
            |> Map.ofList }

    let internal add version chart (catalog: ChartCatalog<'State, 'Event, 'Action, 'Err>) =
        { Charts = Map.add version chart catalog.Charts }

    /// <summary>The chart a version names, or <c>None</c>.</summary>
    let tryFind (version: ChartVersion) (catalog: ChartCatalog<'State, 'Event, 'Action, 'Err>) =
        Map.tryFind version catalog.Charts

    /// <summary>Every version and its chart, oldest first.</summary>
    let entries (catalog: ChartCatalog<'State, 'Event, 'Action, 'Err>) = Map.toList catalog.Charts
