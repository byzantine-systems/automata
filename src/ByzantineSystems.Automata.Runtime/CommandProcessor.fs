namespace ByzantineSystems.Automata.Runtime

open System
open System.Threading
open System.Threading.Tasks
open FsToolkit.ErrorHandling
open Microsoft.Extensions.Logging
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage

/// <summary>Log event ids for the processor, fixed so a query can pin one.</summary>
module internal ProcessorEvents =

    let claimed = EventId(1301, "CommandsClaimed")
    let committed = EventId(1302, "CommandCommitted")
    let rejected = EventId(1303, "CommandRejected")
    let rescheduled = EventId(1304, "CommandRescheduled")
    let deadLettered = EventId(1305, "CommandDeadLettered")
    let leaseLost = EventId(1306, "CommandLeaseLost")
    let conflict = EventId(1307, "CommandEpochConflict")
    let escalated = EventId(1308, "CommandEscalated")
    let pollFailed = EventId(1309, "CommandPollFailed")

/// <summary>What finishing one command did.</summary>
type CommandOutcome =
    /// <summary>The transition was committed at this epoch.</summary>
    | Applied of Epoch

    /// <summary>The command was refused and the entity released.</summary>
    | Refused

    /// <summary>The command went back to the inbox for a later attempt.</summary>
    | Deferred

    /// <summary>The command was given up on and the entity released.</summary>
    | Abandoned

    /// <summary>Another worker owns the command now; nothing was written.</summary>
    | Fenced

    /// <summary>
    /// The failure is this worker's, not the command's. Reported rather than raised: the
    /// processor stops and hands the decision to whoever owns it, while the command keeps its
    /// lease and is redelivered when that lease lapses.
    /// </summary>
    | Escalating of commandId: CommandId * reason: string

/// <summary>What one poll of the processor did.</summary>
type PollReport =
    {
        Summary: PollSummary
        /// <summary>Set when a command in this batch escalated, which ends the loop.</summary>
        Escalation: (CommandId * string) option
    }

/// <summary>
/// Why a processor's loop ended.
///
/// A worker that simply returns tells its owner nothing. These two cases are the difference
/// between an orderly shutdown and a worker that stopped because something is wrong, which is
/// exactly the distinction a supervisor needs in order to decide whether to restart it.
/// </summary>
[<RequireQualifiedAccess>]
type ProcessorStop =
    /// <summary>Cancelled, with the totals for the run.</summary>
    | Drained of totals: PollSummary

    /// <summary>A command escalated. The totals cover everything before it.</summary>
    | Escalated of commandId: CommandId * reason: string * totals: PollSummary

/// <summary>
/// Claims commands from the inbox, resolves each against the chart, and finalizes it.
///
/// This replaces the per-entity actor, the registry and the retry pump at once, because
/// PostgreSQL now owns what they were for. At most one non-terminal command per entity is
/// claimable, so a claim already excludes that entity across every process; the processor never
/// has to serialise anything itself and never has to know which entities exist. Two processors
/// on two hosts with no knowledge of each other produce one order.
///
/// Concurrency here is therefore free of ordering concerns: a batch never contains two commands
/// for one entity, so processing the batch in parallel cannot reorder anything.
/// </summary>
type CommandProcessor<'EntityId, 'State, 'Event, 'Action, 'Err when 'EntityId: equality>
    internal
    (
        config: RuntimeConfig<'EntityId, 'State, 'Event, 'Action, 'Err>,
        completions: Completions<'EntityId, 'State, 'Event, 'Action, 'Err>,
        observer: ObserverDispatcher<'EntityId, 'State, 'Event, 'Action> option,
        signal: WorkSignal
    ) =

    let policy = ValidatedProcessorPolicy.value config.Processor
    let machineId = config.MachineId
    let logger = config.Logger
    let machineName = MachineId.value machineId

    // The aggregate store is used through one capability at a time. This is not ceremony: the
    // inbox and the action queue both declare Claim and Reschedule with the same shape, so a
    // call on the aggregate is genuinely ambiguous, and naming the capability is what says
    // which one a line means.
    let inbox = config.Store :> ICommandInbox<'EntityId, 'Event>
    let reader = config.Store :> IStateReader<'EntityId, 'State, 'Event, 'Action>

    let processor =
        config.Store :> ICommandProcessorStore<'EntityId, 'State, 'Event, 'Action, 'Err>

    let snapshotOf (entityId: 'EntityId) (ct: CancellationToken) =
        reader.TryGetSnapshot(machineId, entityId, ct)
        |> TaskResult.mapError MachineError.Store
        |> TaskResult.map (function
            // An entity with no belief has never committed. Its first command resolves from the
            // configured initial state at the initial epoch, which is what the store expects
            // when that commit arrives.
            | None -> config.InitialState, Epoch.initial, InstanceStatus.Running
            | Some snapshot -> snapshot.State, snapshot.Epoch, snapshot.Status)

    /// Pushes the lease deadline out while work is still running, and stops the moment the
    /// fence says somebody else owns the command. Renewing past that point would be arguing
    /// with a decision the database has already made.
    let renewWhile (leased: LeasedCommand<'EntityId, 'Event>) (work: Task<'T>) (ct: CancellationToken) =
        let rec renew () =
            task {
                if work.IsCompleted then
                    return ()
                else
                    let! finished = Task.WhenAny(work :> Task, Task.Delay(policy.RenewAfter, config.TimeProvider, ct))

                    if Object.ReferenceEquals(finished, work :> Task) then
                        return ()
                    else
                        match! inbox.ExtendLease(leased.Work.CommandId, leased.Token, policy.Lease, ct) with
                        | Ok Updated -> return! renew ()
                        | Ok(LeaseUpdateOutcome.LeaseLost)
                        | Error _ -> return ()
            }

        task {
            let renewal = renew ()
            let! result = work
            do! renewal
            return result
        }

    let logOutcome (record: CommandRecord<'EntityId, 'Event>) (outcome: CommandOutcome) (elapsed: TimeSpan) =
        // Ids, counts and durations only. States, events, actions, errors, tenants and
        // principals are the application's data and never reach a log message.
        let commandId = CommandId.value record.CommandId

        match outcome with
        | Applied epoch ->
            logger.LogInformation(
                ProcessorEvents.committed,
                "Committed command {CommandId} for machine {MachineId} at epoch {Epoch} in {ElapsedMs}ms after {Attempts} attempts.",
                commandId,
                machineName,
                Epoch.value epoch,
                elapsed.TotalMilliseconds,
                record.Attempts
            )
        | Refused ->
            logger.LogInformation(
                ProcessorEvents.rejected,
                "Rejected command {CommandId} for machine {MachineId} in {ElapsedMs}ms.",
                commandId,
                machineName,
                elapsed.TotalMilliseconds
            )
        | Deferred ->
            logger.LogWarning(
                ProcessorEvents.rescheduled,
                "Rescheduled command {CommandId} for machine {MachineId} after {Attempts} attempts.",
                commandId,
                machineName,
                record.Attempts
            )
        | Abandoned ->
            logger.LogError(
                ProcessorEvents.deadLettered,
                "Dead-lettered command {CommandId} for machine {MachineId} after {Attempts} attempts.",
                commandId,
                machineName,
                record.Attempts
            )
        | Fenced ->
            logger.LogWarning(
                ProcessorEvents.leaseLost,
                "Lost the lease on command {CommandId} for machine {MachineId}; another worker owns it.",
                commandId,
                machineName
            )
        | Escalating(_, reason) ->
            logger.LogError(
                ProcessorEvents.escalated,
                "Escalating command {CommandId} for machine {MachineId}: {Reason}",
                commandId,
                machineName,
                reason
            )

    let publish
        (record: CommandRecord<'EntityId, 'Event>)
        (result: CommandResult<'EntityId, 'State, 'Event, 'Action, 'Err>)
        =
        completions.Publish(record.CommandId, result)

        match result, observer with
        | CommandResult.Committed transition, Some bus -> bus.TryPublish transition
        | _ -> ()

    let reschedule (leased: LeasedCommand<'EntityId, 'Event>) (ct: CancellationToken) =
        task {
            match! inbox.Reschedule(leased.Work.CommandId, leased.Token, policy.Backoff, ct) with
            | Ok Updated -> return Deferred
            | Ok(LeaseUpdateOutcome.LeaseLost)
            | Error _ -> return Fenced
        }

    /// Interprets what the store said about a finalize that carried a transition.
    let interpretCommit
        (leased: LeasedCommand<'EntityId, 'Event>)
        (draft: TransitionDraft<'EntityId, 'State, 'Event, 'Action>)
        (outcome: FinalizeOutcome)
        (ct: CancellationToken)
        =
        task {
            let record = leased.Work

            match outcome with
            // AlreadyFinalized is success, not an anomaly. It is the answer to "I lost my
            // connection mid-commit and do not know whether it landed", and treating it as
            // anything else would turn the one safe recovery into a duplicate.
            | Finalized epoch
            | AlreadyFinalized epoch ->
                publish
                    record
                    (CommandResult.Committed
                        { Draft = draft
                          Epoch = epoch
                          CommandId = record.CommandId
                          ChartVersion = record.ChartVersion
                          CommittedAt = config.TimeProvider.GetUtcNow() })

                return Applied epoch
            | Conflict(expected, actual) ->
                // The head invariant should make this unreachable: no second command for this
                // entity can be claimed while this one is leased. It survives as a backstop
                // against a writer outside the inbox path, and rescheduling is safe because the
                // command was not applied.
                logger.LogWarning(
                    ProcessorEvents.conflict,
                    "Command {CommandId} for machine {MachineId} expected epoch {Expected} but found {Actual}; rescheduling.",
                    CommandId.value record.CommandId,
                    machineName,
                    Epoch.value expected,
                    Epoch.value actual
                )

                return! reschedule leased ct
            | FinalizeOutcome.LeaseLost -> return Fenced
        }

    /// Applies a disposition. Retry becomes a dead letter once the inbox says the command has
    /// had its attempts, so a command that can never succeed cannot hold its entity forever.
    let applyDisposition
        (leased: LeasedCommand<'EntityId, 'Event>)
        (disposition: CommandDisposition<'Err>)
        (ct: CancellationToken)
        =
        let record = leased.Work

        let terminate
            (finalize:
                CommandId * LeaseToken<CommandWork> * CommandFailure<'Err> * CancellationToken
                    -> Task<Result<FinalizeOutcome, StoreError>>)
            (failure: CommandFailure<'Err>)
            (outcome: CommandOutcome)
            =
            task {
                match! finalize (record.CommandId, leased.Token, failure, ct) with
                | Ok(Finalized _)
                | Ok(AlreadyFinalized _) ->
                    publish
                        record
                        (match outcome with
                         | Abandoned -> CommandResult.DeadLettered failure
                         | _ -> CommandResult.Rejected failure)

                    return outcome
                | Ok FinalizeOutcome.LeaseLost
                | Ok(Conflict _)
                | Error _ -> return Fenced
            }

        match disposition with
        | Retry when record.Attempts + 1 >= policy.MaxAttempts ->
            terminate
                (fun args -> processor.DeadLetter args)
                (CommandFailure.Machine $"gave up after %d{record.Attempts + 1} attempts")
                Abandoned
        | Retry -> reschedule leased ct
        | Reject failure -> terminate (fun args -> processor.Reject args) failure Refused
        | DeadLetter failure -> terminate (fun args -> processor.DeadLetter args) failure Abandoned
        | Escalate reason -> Task.FromResult(Escalating(record.CommandId, reason))

    /// One command, start to finish.
    let processOne (leased: LeasedCommand<'EntityId, 'Event>) (ct: CancellationToken) =
        task {
            let record = leased.Work
            let started = config.TimeProvider.GetTimestamp()

            let decide () =
                taskResult {
                    let! state, epoch, status = snapshotOf record.EntityId ct

                    do!
                        match status with
                        | InstanceStatus.Running -> Ok()
                        | ended -> Error(MachineError.Rejected(MachineRejection.InstanceNotRunning ended))

                    // Pure, synchronous, and the only part of this loop that decides anything
                    // about the domain. No try/with, no I/O, no clock.
                    let! resolution =
                        Chart.resolve config.Chart state record.Event
                        |> Result.mapError MachineError.Transition

                    let draft =
                        Draft.ofResolution
                            machineId
                            record.EntityId
                            config.Chart
                            state
                            record.Event
                            // Business time is the command's arrival, not this moment. Replaying
                            // the log then reproduces the drafts it produced the first time,
                            // whenever the replay happens to run.
                            record.ReceivedAt
                            resolution

                    let! outcome =
                        processor.Commit(record.CommandId, leased.Token, epoch, draft, ct)
                        |> TaskResult.mapError MachineError.Store

                    return draft, outcome
                }

            let! decision = renewWhile leased (decide ()) ct

            let! outcome =
                match decision with
                | Ok(draft, outcome) -> interpretCommit leased draft outcome ct
                | Error error -> applyDisposition leased (policy.Classify error) ct

            logOutcome record outcome (config.TimeProvider.GetElapsedTime started)
            return outcome
        }

    let report (outcomes: CommandOutcome list) : PollReport =
        let summary =
            outcomes
            |> List.fold
                (fun summary outcome ->
                    match outcome with
                    | Applied _ ->
                        { summary with
                            Committed = summary.Committed + 1 }
                    | Refused ->
                        { summary with
                            Rejected = summary.Rejected + 1 }
                    | Deferred ->
                        { summary with
                            Rescheduled = summary.Rescheduled + 1 }
                    | Abandoned ->
                        { summary with
                            DeadLettered = summary.DeadLettered + 1 }
                    | Fenced ->
                        { summary with
                            LeaseLost = summary.LeaseLost + 1 }
                    | Escalating _ -> summary)
                { PollSummary.empty with
                    Claimed = List.length outcomes }

        { Summary = summary
          Escalation =
            outcomes
            |> List.tryPick (function
                | Escalating(commandId, reason) -> Some(commandId, reason)
                | _ -> None) }

    /// <summary>The machine this processor drains.</summary>
    member _.MachineId: MachineId = machineId

    /// <summary>
    /// Claims one batch and processes it, bounded by the policy's concurrency.
    ///
    /// A failed claim is reported rather than raised: the loop polls again shortly, which is the
    /// same recovery a retry would produce and costs less. The store deliberately does not retry
    /// a claim, for the same reason.
    /// </summary>
    member _.PollAsync(ct: CancellationToken) : Task<Result<PollReport, MachineError<'Err>>> =
        taskResult {
            let! batch =
                inbox.Claim(machineId, policy.Batch, policy.Lease, ct)
                |> TaskResult.mapError MachineError.Store

            match batch with
            | [] ->
                return
                    { Summary = PollSummary.empty
                      Escalation = None }
            | claimed ->
                logger.LogDebug(
                    ProcessorEvents.claimed,
                    "Claimed {Count} commands for machine {MachineId}.",
                    List.length claimed,
                    machineName
                )

                use gate = new SemaphoreSlim(policy.Concurrency)

                let run (leased: LeasedCommand<'EntityId, 'Event>) =
                    task {
                        do! gate.WaitAsync ct

                        try
                            return! processOne leased ct
                        finally
                            gate.Release() |> ignore
                    }

                let! outcomes = claimed |> List.map run |> Task.WhenAll
                return report (List.ofArray outcomes)
        }

    /// <summary>
    /// Polls until cancelled or until a command escalates, and says which it was.
    ///
    /// A failed poll is logged and waited out rather than ending the loop: a database that is
    /// briefly unreachable is a condition to survive, not a reason for a worker to exit and take
    /// its machine's throughput with it. An escalation is the opposite, and stops the loop so
    /// its owner can decide.
    /// </summary>
    member this.RunAsync(ct: CancellationToken) : Task<ProcessorStop> =
        /// Returns false when the wait ended because the processor is shutting down.
        let waitForWork () =
            task {
                try
                    let! _ = signal.WaitOrTimeoutAsync(policy.PollingInterval, config.TimeProvider, ct)
                    return true
                with :? OperationCanceledException when ct.IsCancellationRequested ->
                    return false
            }

        let rec loop (totals: PollSummary) =
            task {
                if ct.IsCancellationRequested then
                    return ProcessorStop.Drained totals
                else
                    match! this.PollAsync ct with
                    | Ok report ->
                        let totals = PollSummary.add totals report.Summary

                        match report.Escalation with
                        | Some(commandId, reason) -> return ProcessorStop.Escalated(commandId, reason, totals)
                        // A full batch means there is probably more waiting, so going straight
                        // round again drains a backlog at claim speed rather than poll speed.
                        | None when report.Summary.Claimed = policy.Batch -> return! loop totals
                        | None ->
                            match! waitForWork () with
                            | true -> return! loop totals
                            | false -> return ProcessorStop.Drained totals
                    | Error error ->
                        logger.LogWarning(
                            ProcessorEvents.pollFailed,
                            "Poll failed for machine {MachineId}: {Failure}. Waiting before the next attempt.",
                            machineName,
                            // The case name, never the payload: a store error can carry a driver
                            // message, and driver messages carry connection details.
                            (match error with
                             | Store _ -> "store"
                             | Timeout _ -> "timeout"
                             | CircuitOpen _ -> "circuit-open"
                             | _ -> "unclassified")
                        )

                        match! waitForWork () with
                        | true -> return! loop totals
                        | false -> return ProcessorStop.Drained totals
            }

        loop PollSummary.empty
