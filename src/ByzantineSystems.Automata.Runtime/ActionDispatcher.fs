namespace ByzantineSystems.Automata.Runtime

open System
open System.Threading
open System.Threading.Tasks
open FsToolkit.ErrorHandling
open Microsoft.Extensions.Logging
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage

/// <summary>Log event ids for action delivery.</summary>
module internal DispatcherEvents =

    let claimed = EventId(1401, "ActionsClaimed")
    let delivered = EventId(1402, "ActionDelivered")
    let rescheduled = EventId(1403, "ActionRescheduled")
    let abandoned = EventId(1404, "ActionAbandoned")
    let leaseLost = EventId(1405, "ActionLeaseLost")
    let pollFailed = EventId(1406, "ActionPollFailed")
    let handlerFailed = EventId(1407, "ActionHandlerFailed")

/// <summary>
/// Delivers one action to the outside world.
///
/// The whole leased action is handed over, not just the payload, so a handler can pass
/// <c>(CommandId, Ordinal)</c> to its destination and let that destination recognise a repeat.
/// Delivery is at-least-once and a crash between the effect and the acknowledgement is normal,
/// so a handler that cannot tolerate a duplicate has to say something to the far side.
/// </summary>
type ActionHandler<'EntityId, 'Action, 'EffectError> =
    LeasedAction<'EntityId, 'Action> -> CancellationToken -> Task<Result<unit, 'EffectError>>

/// <summary>What delivering one action did.</summary>
type DeliveryOutcome =
    | Delivered
    | Retrying
    | GaveUp
    | Unfenced

/// <summary>What one poll of the dispatcher did.</summary>
type DeliveryReport =
    { Claimed: int
      Delivered: int
      Rescheduled: int
      Abandoned: int
      LeaseLost: int }

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Runtime.DeliveryReport" />.</summary>
[<RequireQualifiedAccess>]
module DeliveryReport =

    /// <summary>A poll that found nothing.</summary>
    let empty: DeliveryReport =
        { Claimed = 0
          Delivered = 0
          Rescheduled = 0
          Abandoned = 0
          LeaseLost = 0 }

    let internal add (left: DeliveryReport) (right: DeliveryReport) : DeliveryReport =
        { Claimed = left.Claimed + right.Claimed
          Delivered = left.Delivered + right.Delivered
          Rescheduled = left.Rescheduled + right.Rescheduled
          Abandoned = left.Abandoned + right.Abandoned
          LeaseLost = left.LeaseLost + right.LeaseLost }

/// <summary>
/// Drains the queue a commit writes into, delivering each action through an application handler.
///
/// Unordered by design. The order that matters was settled when the commands committed; holding
/// one entity's actions in sequence behind a slow destination would stall every other entity for
/// no benefit, because the audit record already says what happened and in what order.
/// </summary>
type ActionDispatcher<'EntityId, 'State, 'Event, 'Action, 'Err, 'EffectError when 'EntityId: equality>
    internal
    (
        config: RuntimeConfig<'EntityId, 'State, 'Event, 'Action, 'Err>,
        handler: ActionHandler<'EntityId, 'Action, 'EffectError>,
        signal: WorkSignal
    ) =

    let policy = ValidatedProcessorPolicy.value config.Processor
    let queue = config.Store :> IActionQueue<'EntityId, 'Action>
    let machineId = config.MachineId
    let machineName = MachineId.value machineId
    let logger = config.Logger

    let fenced (outcome: Result<LeaseUpdateOutcome, StoreError>) (settled: DeliveryOutcome) =
        match outcome with
        | Ok Updated -> settled
        | Ok(LeaseUpdateOutcome.LeaseLost)
        | Error _ -> Unfenced

    /// A delivery that never reported an outcome, again and again. The ordinary path cannot get
    /// here: a failed delivery at MaxAttempts is abandoned, so the count only passes it when
    /// deliveries ended with no outcome at all, a handler that took the process down with it or
    /// ran past its lease. Handling it again would only do that again, so it is abandoned unrun.
    let poisoned (action: LeasedAction<'EntityId, 'Action>) =
        action.DeliveryCount > policy.MaxAttempts

    /// Why a delivery failed, which otherwise nothing would say. A thrown exception is logged with
    /// its stack, the only way to see where it came from. A reported error is the application's
    /// own value and is not written out, like every other payload; that it happened is.
    let handlerFailed (action: LeasedAction<'EntityId, 'Action>) (attempt: Result<Result<unit, 'EffectError>, exn>) =
        let ordinal = action.Work.Ordinal
        let commandId = CommandId.value action.Work.CommandId

        match attempt with
        | Error thrown ->
            logger.LogWarning(
                DispatcherEvents.handlerFailed,
                thrown,
                "The handler for action {Ordinal} of command {CommandId} for machine {MachineId} threw.",
                ordinal,
                commandId,
                machineName
            )
        | Ok(Error _) ->
            logger.LogWarning(
                DispatcherEvents.handlerFailed,
                "The handler for action {Ordinal} of command {CommandId} for machine {MachineId} reported a failure.",
                ordinal,
                commandId,
                machineName
            )
        | Ok(Ok()) -> ()

    let deliver (action: LeasedAction<'EntityId, 'Action>) (ct: CancellationToken) =
        task {
            if poisoned action then
                let! outcome = queue.Abandon(action, $"delivered %d{action.DeliveryCount} times without an outcome", ct)

                return fenced outcome GaveUp
            else
                // The user's handler is one of the four places this library catches. It is arbitrary
                // application code reaching arbitrary external systems, and a throw from it means a
                // failed delivery rather than a broken worker.
                let! attempt = handler action ct |> TaskOutcome.capture

                let succeeded =
                    match attempt with
                    | Ok(Ok()) -> true
                    | Ok(Error _)
                    | Error _ -> false

                if not succeeded then
                    handlerFailed action attempt

                if succeeded then
                    let! outcome = queue.Complete(action, ct)
                    return fenced outcome Delivered
                elif action.DeliveryCount >= policy.MaxAttempts then
                    let! outcome = queue.Abandon(action, $"undelivered after %d{action.DeliveryCount} attempts", ct)
                    return fenced outcome GaveUp
                else
                    let! outcome = queue.Reschedule(action, policy.Backoff, ct)
                    return fenced outcome Retrying
        }

    let log (action: LeasedAction<'EntityId, 'Action>) (outcome: DeliveryOutcome) =
        let commandId = CommandId.value action.Work.CommandId

        match outcome with
        | Delivered ->
            logger.LogInformation(
                DispatcherEvents.delivered,
                "Delivered action {Ordinal} of command {CommandId} for machine {MachineId}.",
                action.Work.Ordinal,
                commandId,
                machineName
            )
        | Retrying ->
            logger.LogWarning(
                DispatcherEvents.rescheduled,
                "Rescheduled action {Ordinal} of command {CommandId} for machine {MachineId} after {Deliveries} deliveries.",
                action.Work.Ordinal,
                commandId,
                machineName,
                action.DeliveryCount
            )
        | GaveUp ->
            logger.LogError(
                DispatcherEvents.abandoned,
                "Abandoned action {Ordinal} of command {CommandId} for machine {MachineId} after {Deliveries} deliveries.",
                action.Work.Ordinal,
                commandId,
                machineName,
                action.DeliveryCount
            )
        | Unfenced ->
            logger.LogWarning(
                DispatcherEvents.leaseLost,
                "Lost the lease on action {Ordinal} of command {CommandId} for machine {MachineId}.",
                action.Work.Ordinal,
                commandId,
                machineName
            )

    let report (outcomes: DeliveryOutcome list) : DeliveryReport =
        outcomes
        |> List.fold
            (fun running outcome ->
                match outcome with
                | Delivered ->
                    { running with
                        Delivered = running.Delivered + 1 }
                | Retrying ->
                    { running with
                        Rescheduled = running.Rescheduled + 1 }
                | GaveUp ->
                    { running with
                        Abandoned = running.Abandoned + 1 }
                | Unfenced ->
                    { running with
                        LeaseLost = running.LeaseLost + 1 })
            { DeliveryReport.empty with
                Claimed = List.length outcomes }

    /// <summary>Claims one batch of actions and delivers them, bounded by the policy's concurrency.</summary>
    member _.PollAsync(ct: CancellationToken) : Task<Result<DeliveryReport, MachineError<'Err>>> =
        taskResult {
            let! batch =
                queue.Claim(machineId, policy.Batch, policy.Lease, ct)
                |> TaskResult.mapError MachineError.Store

            match batch with
            | [] -> return DeliveryReport.empty
            | claimed ->
                logger.LogDebug(
                    DispatcherEvents.claimed,
                    "Claimed {Count} actions for machine {MachineId}.",
                    List.length claimed,
                    machineName
                )

                use gate = new SemaphoreSlim(policy.Concurrency)

                let run (action: LeasedAction<'EntityId, 'Action>) =
                    task {
                        do! gate.WaitAsync ct

                        try
                            let! outcome = deliver action ct
                            log action outcome
                            return outcome
                        finally
                            gate.Release() |> ignore
                    }

                let! outcomes = claimed |> List.map run |> Task.WhenAll
                return report (List.ofArray outcomes)
        }

    /// <summary>
    /// Polls until cancelled, returning the totals for the run.
    ///
    /// Unlike the command processor there is no escalation: an action that cannot be delivered is
    /// abandoned after its attempts and recorded, which is a fact about that action rather than
    /// about this worker.
    /// </summary>
    member this.RunAsync(ct: CancellationToken) : Task<DeliveryReport> =
        let waitForWork () =
            task {
                try
                    let! _ = signal.WaitOrTimeoutAsync(policy.PollingInterval, config.TimeProvider, ct)
                    return true
                with :? OperationCanceledException when ct.IsCancellationRequested ->
                    return false
            }

        let rec loop (totals: DeliveryReport) =
            task {
                if ct.IsCancellationRequested then
                    return totals
                else
                    let! outcome = this.PollAsync ct

                    let totals =
                        match outcome with
                        | Ok report -> DeliveryReport.add totals report
                        | Error error ->
                            logger.LogWarning(
                                DispatcherEvents.pollFailed,
                                "Action poll failed for machine {MachineId}: {Failure}.",
                                machineName,
                                (match error with
                                 | Store _ -> "store"
                                 | Timeout _ -> "timeout"
                                 | CircuitOpen _ -> "circuit-open"
                                 | _ -> "unclassified")
                            )

                            totals

                    match outcome with
                    | Ok report when report.Claimed = policy.Batch -> return! loop totals
                    | _ ->
                        match! waitForWork () with
                        | true -> return! loop totals
                        | false -> return totals
            }

        loop DeliveryReport.empty
