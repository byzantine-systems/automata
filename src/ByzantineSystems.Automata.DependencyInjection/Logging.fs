namespace ByzantineSystems.Automata.DependencyInjection

open System
open ByzantineSystems.Automata.Resilience
open Microsoft.Extensions.Logging

/// <summary>
/// Cached log delegates, one per message: the template is parsed once, so a log call neither
/// parses a format string nor boxes an argument array. Each carries its delegate type
/// explicitly, because F# resolves <c>LoggerMessage.Define</c>'s overloads by return type.
/// </summary>
[<RequireQualifiedAccess>]
module internal Log =

    let retryScheduled: Action<ILogger, string, int, TimeSpan, exn> =
        LoggerMessage.Define<string, int, TimeSpan>(
            LogLevel.Warning,
            EventId(1001, "RetryScheduled"),
            "Automata machine {MachineKey} scheduled retry attempt {AttemptNumber} after {RetryDelay}"
        )

    let attemptTimedOut: Action<ILogger, string, TimeSpan, exn> =
        LoggerMessage.Define<string, TimeSpan>(
            LogLevel.Warning,
            EventId(1002, "AttemptTimedOut"),
            "Automata machine {MachineKey} operation attempt exceeded its {TimeoutBudget} timeout"
        )

    let totalTimedOut: Action<ILogger, string, TimeSpan, exn> =
        LoggerMessage.Define<string, TimeSpan>(
            LogLevel.Error,
            EventId(1003, "TotalTimedOut"),
            "Automata machine {MachineKey} operation exceeded its total {TimeoutBudget} timeout"
        )

    let circuitOpened: Action<ILogger, string, TimeSpan, exn> =
        LoggerMessage.Define<string, TimeSpan>(
            LogLevel.Error,
            EventId(1004, "CircuitOpened"),
            "Automata machine {MachineKey} dependency circuit opened for {BreakDuration}"
        )

    let circuitClosed: Action<ILogger, string, exn> =
        LoggerMessage.Define<string>(
            LogLevel.Information,
            EventId(1005, "CircuitClosed"),
            "Automata machine {MachineKey} dependency circuit closed"
        )

    let childStarted: Action<ILogger, string, string, string, DateTimeOffset, exn> =
        LoggerMessage.Define<string, string, string, DateTimeOffset>(
            LogLevel.Information,
            EventId(1101, "ChildStarted"),
            "Automata machine {MachineKey} child {ChildId}: {SupervisionReason} at {OccurredAt}"
        )

    let childRestarted: Action<ILogger, string, string, string, DateTimeOffset, exn> =
        LoggerMessage.Define<string, string, string, DateTimeOffset>(
            LogLevel.Warning,
            EventId(1102, "ChildRestarted"),
            "Automata machine {MachineKey} child {ChildId}: {SupervisionReason} at {OccurredAt}"
        )

    let childEscalated: Action<ILogger, string, string, string, DateTimeOffset, exn> =
        LoggerMessage.Define<string, string, string, DateTimeOffset>(
            LogLevel.Error,
            EventId(1103, "ChildEscalated"),
            "Automata machine {MachineKey} child {ChildId}: {SupervisionReason} at {OccurredAt}"
        )

    let childStopped: Action<ILogger, string, string, string, DateTimeOffset, exn> =
        LoggerMessage.Define<string, string, string, DateTimeOffset>(
            LogLevel.Information,
            EventId(1104, "ChildStopped"),
            "Automata machine {MachineKey} child {ChildId}: {SupervisionReason} at {OccurredAt}"
        )

    let hostedServiceFailed: Action<ILogger, string, exn> =
        LoggerMessage.Define<string>(
            LogLevel.Error,
            EventId(1201, "HostedServiceFailed"),
            "Automata hosted service for machine {MachineKey} failed"
        )

/// <summary>
/// Structured logging owned by the host-integration layer. Core state transitions and storage
/// contracts remain independent of any logging framework.
///
/// <c>pipelineEvent</c> is public because the pipeline it describes is no longer built here: a
/// host constructs its own store context and can hand this in as the sink, which is the only
/// way those events reach a log now.
/// </summary>
[<RequireQualifiedAccess>]
module AutomataLog =

    [<Literal>]
    let PipelineCategory = "ByzantineSystems.Automata.Resilience"

    /// <summary>
    /// A sink that writes resilience events to a logger, for passing to
    /// <c>DataSource.resilience</c>.
    /// </summary>
    let pipelineEvent (logger: ILogger) machineKey =
        function
        | RetryScheduled(attemptNumber, delay) ->
            Log.retryScheduled.Invoke(logger, machineKey, attemptNumber, delay, null)
        | AttemptTimedOut budget -> Log.attemptTimedOut.Invoke(logger, machineKey, budget, null)
        | TotalTimedOut budget -> Log.totalTimedOut.Invoke(logger, machineKey, budget, null)
        | CircuitOpened breakDuration -> Log.circuitOpened.Invoke(logger, machineKey, breakDuration, null)
        | CircuitClosed -> Log.circuitClosed.Invoke(logger, machineKey, null)

    let internal supervisionEvent (logger: ILogger) machineKey (event: SupervisionEvent) =
        match event.Kind with
        | SupervisionEventKind.Started ->
            Log.childStarted.Invoke(logger, machineKey, event.ChildId, event.Reason, event.At, null)
        | SupervisionEventKind.Restarted ->
            Log.childRestarted.Invoke(logger, machineKey, event.ChildId, event.Reason, event.At, null)
        | SupervisionEventKind.Escalated ->
            Log.childEscalated.Invoke(logger, machineKey, event.ChildId, event.Reason, event.At, null)
        | SupervisionEventKind.Stopped ->
            Log.childStopped.Invoke(logger, machineKey, event.ChildId, event.Reason, event.At, null)

    let internal hostedServiceFailed (logger: ILogger) machineKey (error: exn) =
        Log.hostedServiceFailed.Invoke(logger, machineKey, error)
