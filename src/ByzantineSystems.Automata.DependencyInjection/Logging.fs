namespace ByzantineSystems.Automata.DependencyInjection

open ByzantineSystems.Automata.Resilience
open Microsoft.Extensions.Logging

/// Structured logging owned by the host-integration layer. Core state transitions and
/// storage contracts remain independent of any logging framework.
[<RequireQualifiedAccess>]
module internal AutomataLog =

    [<Literal>]
    let PipelineCategory = "ByzantineSystems.Automata.Resilience"

    let pipelineEvent (logger: ILogger) machineKey =
        function
        | RetryScheduled(attemptNumber, delay) ->
            logger.LogWarning(
                EventId(1001, "RetryScheduled"),
                "Automata machine {MachineKey} scheduled retry attempt {AttemptNumber} after {RetryDelay}",
                [| box machineKey; box attemptNumber; box delay |]
            )
        | AttemptTimedOut budget ->
            logger.LogWarning(
                EventId(1002, "AttemptTimedOut"),
                "Automata machine {MachineKey} operation attempt exceeded its {TimeoutBudget} timeout",
                [| box machineKey; box budget |]
            )
        | TotalTimedOut budget ->
            logger.LogError(
                EventId(1003, "TotalTimedOut"),
                "Automata machine {MachineKey} operation exceeded its total {TimeoutBudget} timeout",
                [| box machineKey; box budget |]
            )
        | CircuitOpened breakDuration ->
            logger.LogError(
                EventId(1004, "CircuitOpened"),
                "Automata machine {MachineKey} dependency circuit opened for {BreakDuration}",
                [| box machineKey; box breakDuration |]
            )
        | CircuitClosed ->
            logger.LogInformation(
                EventId(1005, "CircuitClosed"),
                "Automata machine {MachineKey} dependency circuit closed",
                [| box machineKey |]
            )

    let supervisionEvent (logger: ILogger) machineKey (event: SupervisionEvent) =
        let values = [| box machineKey; box event.ChildId; box event.Reason; box event.At |]

        let template =
            "Automata machine {MachineKey} child {ChildId}: {SupervisionReason} at {OccurredAt}"

        match event.Kind with
        | SupervisionEventKind.Started -> logger.LogInformation(EventId(1101, "ChildStarted"), template, values)
        | SupervisionEventKind.Restarted -> logger.LogWarning(EventId(1102, "ChildRestarted"), template, values)
        | SupervisionEventKind.Escalated -> logger.LogError(EventId(1103, "ChildEscalated"), template, values)
        | SupervisionEventKind.Stopped -> logger.LogInformation(EventId(1104, "ChildStopped"), template, values)

    let hostedServiceFailed (logger: ILogger) machineKey (error: exn) =
        logger.LogError(
            EventId(1201, "HostedServiceFailed"),
            error,
            "Automata hosted service for machine {MachineKey} failed",
            [| box machineKey |]
        )
