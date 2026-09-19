namespace ByzantineSystems.Automata.Resilience

open System

/// <summary>
/// Notable occurrences inside a resilience pipeline, relayed to an optional sink wired
/// through <see cref="M:ByzantineSystems.Automata.Resilience.RetryConfig.toPipeline" />.
/// These are observations, not control flow: a sink cannot change what the pipeline does.
/// </summary>
type PipelineEvent =

    /// <summary>The retry strategy scheduled another attempt.</summary>
    | RetryScheduled of attemptNumber: int * delay: TimeSpan

    /// <summary>An attempt exceeded its per-attempt budget.</summary>
    | AttemptTimedOut of budget: TimeSpan

    /// <summary>The whole operation exceeded its total budget.</summary>
    | TotalTimedOut of budget: TimeSpan

    /// <summary>The circuit breaker opened; calls will fail fast until it closes.</summary>
    | CircuitOpened of breakDuration: TimeSpan

    /// <summary>The circuit breaker closed; calls are attempted again.</summary>
    | CircuitClosed

/// <summary>
/// A best-effort, exception-free pipeline observer. Sinks are invoked inline; keep them
/// cheap. Host-integrated <c>ILogger</c>/<c>Meter</c> wiring lives in the optional
/// dependency-injection package, on top of this seam.
/// </summary>
type PipelineEventSink = PipelineEvent -> unit
