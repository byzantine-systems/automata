namespace ByzantineSystems.Automata.DependencyInjection

open System
open System.Runtime.ExceptionServices
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open FsToolkit.ErrorHandling
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging

/// <summary>Who runs the maintenance ticks.</summary>
[<RequireQualifiedAccess>]
type MaintenanceScheduler =
    /// <summary>
    /// The database runs them itself, through pg_cron, so maintenance continues while no
    /// application is up. Falls back to <c>InProcess</c> when the database cannot: maintenance is
    /// not optional, and only the scheduler is.
    /// </summary>
    | InDatabase

    /// <summary>This process runs them, on its own clock.</summary>
    | InProcess

/// <summary>How a host maintains the database its machines share.</summary>
type MaintenanceOptions =
    {
        /// <summary>The database to maintain. One per database, however many machines it serves.</summary>
        Maintenance: IServiceProvider -> IDatabaseMaintenance
        Scheduler: MaintenanceScheduler
        /// <summary>
        /// How often waiting work is announced to other processes. This is the latency a command
        /// submitted elsewhere waits for, when it would otherwise wait for a poll.
        /// </summary>
        NotifyEvery: TimeSpan
        /// <summary>How often a maintenance pass runs.</summary>
        RunEvery: TimeSpan
        /// <summary>The most rows one pass deletes per task, so each pass is a short transaction.</summary>
        Batch: int
        /// <summary>
        /// Whether drift in the derived blocked column is repaired as well as reported. Off by
        /// default, because drift is a symptom of a bug elsewhere and repairing it silently hides
        /// that bug.
        /// </summary>
        RepairDrift: bool
        TimeProvider: TimeProvider
    }

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.DependencyInjection.MaintenanceOptions" />.</summary>
[<RequireQualifiedAccess>]
module MaintenanceOptions =

    /// <summary>
    /// In process, announcing every second and running a pass every minute, 1000 rows at a
    /// time, reporting drift without repairing it.
    /// </summary>
    let defaults (maintenance: IServiceProvider -> IDatabaseMaintenance) : MaintenanceOptions =
        { Maintenance = maintenance
          Scheduler = MaintenanceScheduler.InProcess
          NotifyEvery = TimeSpan.FromSeconds 1.
          RunEvery = TimeSpan.FromMinutes 1.
          Batch = 1000
          RepairDrift = false
          TimeProvider = TimeProvider.System }

[<RequireQualifiedAccess>]
module internal MaintenanceEvents =
    let scheduled = EventId(1501, "MaintenanceScheduledInDatabase")
    let fellBack = EventId(1502, "MaintenanceFellBackInProcess")
    let tickFailed = EventId(1503, "MaintenanceTickFailed")
    let pass = EventId(1504, "MaintenancePass")
    let drift = EventId(1505, "BlockedDriftDetected")
    let repaired = EventId(1506, "BlockedDriftRepaired")
    let queueFilling = EventId(1507, "NotificationQueueFilling")

/// <summary>What maintenance writes to the log. Ids and counts only, never a payload.</summary>
[<RequireQualifiedAccess>]
module internal MaintenanceLog =

    let tickFailed (logger: ILogger) (tick: string) (error: StoreError) =
        logger.LogWarning(
            MaintenanceEvents.tickFailed,
            "Maintenance tick {Tick} failed ({Failure}); trying again next tick.",
            tick,
            StoreError.caseName error
        )

    /// The server warns in its own log at half full, so this warns at the same point, where an
    /// application will see it.
    let queueFilling (logger: ILogger) (usage: float) =
        logger.LogWarning(
            MaintenanceEvents.queueFilling,
            "The notification queue is {Usage:P0} full. A session that ran LISTEN and is holding a long transaction open stops it being cleared, and a full queue fails every commit that notifies.",
            usage
        )

    let pass (logger: ILogger) (report: MaintenanceReport) =
        if
            report.Reaped
            + report.PurgedCommands
            + report.PurgedBeliefs
            + report.PurgedActions > 0L
        then
            logger.LogInformation(
                MaintenanceEvents.pass,
                "Maintenance for machine {MachineId}: reaped {Reaped} leases, purged {PurgedCommands} commands, {PurgedBeliefs} superseded beliefs and {PurgedActions} archived actions.",
                MachineId.value report.MachineId,
                report.Reaped,
                report.PurgedCommands,
                report.PurgedBeliefs,
                report.PurgedActions
            )

    let drift (logger: ILogger) (drift: BlockedDrift) =
        logger.LogWarning(
            MaintenanceEvents.drift,
            "Command {CommandId} of machine {MachineId} is blocked={Blocked} but should be blocked={Expected}. This is a bug in submission or acknowledgement, not a condition to wait out.",
            CommandId.value drift.CommandId,
            MachineId.value drift.MachineId,
            drift.Blocked,
            drift.Expected
        )

    let repaired (logger: ILogger) (block: RepairedBlock) =
        logger.LogWarning(
            MaintenanceEvents.repaired,
            "Repaired the blocked flag of command {CommandId}; it is now {Blocked}.",
            CommandId.value block.CommandId,
            block.Blocked
        )

    let scheduled (logger: ILogger) (options: MaintenanceOptions) =
        logger.LogInformation(
            MaintenanceEvents.scheduled,
            "The database runs maintenance itself, notifying every {NotifyEvery} and running every {RunEvery}.",
            options.NotifyEvery,
            options.RunEvery
        )

    let fellBack (logger: ILogger) (reason: string) =
        logger.LogWarning(
            MaintenanceEvents.fellBack,
            "The database cannot run maintenance itself ({Reason}); this process will run it instead.",
            reason
        )

/// <summary>Running loops side by side.</summary>
[<RequireQualifiedAccess>]
module internal Loops =

    /// Raises what the loops failed with: the one exception with its own stack trace, or all of
    /// them together when more than one failed, since awaiting a WhenAll shows only the first.
    let private raiseFailures (all: Task) (first: exn) =
        match all.Exception with
        | null -> ExceptionDispatchInfo.Capture(first).Throw()
        | failures when failures.InnerExceptions.Count > 1 -> raise failures
        | _ -> ExceptionDispatchInfo.Capture(first).Throw()

    /// <summary>
    /// Runs the loops side by side, and ends all of them when any one ends.
    ///
    /// Each loop ends only by cancellation or by an exception nothing here understood. Waiting
    /// for all of them would hide the second case: one loop dead of a fault while the others run
    /// on, the fault unseen until shutdown. So the first to end cancels the rest, and every
    /// failure is then raised to the host.
    /// </summary>
    let together (loops: (CancellationToken -> Task<unit>) list) (ct: CancellationToken) : Task<unit> =
        backgroundTask {
            use linked = CancellationTokenSource.CreateLinkedTokenSource ct
            // List.map is eager, so every loop has started before the first await.
            let running = loops |> List.map (fun loop -> loop linked.Token)
            let! _ = Task.WhenAny running
            linked.Cancel()

            let all = Task.WhenAll running

            match! TaskOutcome.capture all with
            | Ok _ -> return ()
            | Error first -> return raiseFailures all first
        }

/// <summary>
/// Runs database maintenance, or confirms the database runs it itself.
///
/// Every tick is a store call that answers a <c>Result</c>, and a failed one is logged and
/// waited out rather than ending the service: a missed tick delays maintenance, and nothing
/// about a database being briefly unreachable is a reason to take the host down.
///
/// When the database schedules the ticks, this still checks for drift on its own clock. The
/// server log is where pg_cron's output lands, and it is not where an application looks.
/// </summary>
type internal MaintenanceService
    (options: MaintenanceOptions, maintenance: IDatabaseMaintenance, logger: ILogger<MaintenanceService>) =
    inherit BackgroundService()

    let every (interval: TimeSpan) (tick: CancellationToken -> Task) =
        Recurring.every interval options.TimeProvider tick

    /// Runs a store call and hands its value on, or logs its failure and carries on.
    let attempt (tick: string) (work: CancellationToken -> Task<Result<'T, StoreError>>) (onOk: 'T -> Task) ct : Task =
        backgroundTask {
            match! work ct with
            | Ok value -> do! onOk value
            | Error error -> MaintenanceLog.tickFailed logger tick error
        }

    let logAll (log: ILogger -> 'T -> unit) (values: 'T list) : Task =
        values |> List.iter (log logger)
        Task.CompletedTask

    let notified (tick: NotifyReport) : Task =
        if tick.QueueUsage >= 0.5 then
            MaintenanceLog.queueFilling logger tick.QueueUsage

        Task.CompletedTask

    let notify = attempt "notify" maintenance.NotifyPending notified

    let repair =
        attempt "repair" maintenance.RepairDrift (logAll MaintenanceLog.repaired)

    let drifted ct (drift: BlockedDrift list) : Task =
        backgroundTask {
            do! logAll MaintenanceLog.drift drift

            if options.RepairDrift && not drift.IsEmpty then
                do! repair ct
        }

    let checkDrift ct =
        attempt "drift" maintenance.DetectDrift (drifted ct) ct

    let pass ct : Task =
        backgroundTask {
            do! attempt "run" (fun ct -> maintenance.Run(options.Batch, ct)) (logAll MaintenanceLog.pass) ct
            do! checkDrift ct
        }

    let fellBack reason =
        MaintenanceLog.fellBack logger reason
        MaintenanceScheduler.InProcess

    /// Who actually runs the ticks, which is not always who was asked.
    let scheduling (outcome: Result<Scheduling, StoreError>) =
        match outcome with
        | Ok Scheduling.Scheduled ->
            MaintenanceLog.scheduled logger options
            MaintenanceScheduler.InDatabase
        | Ok Scheduling.Unavailable -> fellBack "pg_cron is not installed"
        | Ok Scheduling.NotPermitted -> fellBack "this role may not use pg_cron"
        | Error error -> fellBack (StoreError.caseName error)

    let decide (ct: CancellationToken) : Task<MaintenanceScheduler> =
        match options.Scheduler with
        | MaintenanceScheduler.InProcess -> Task.FromResult MaintenanceScheduler.InProcess
        | MaintenanceScheduler.InDatabase ->
            maintenance.Schedule(options.NotifyEvery, options.RunEvery, options.Batch, ct)
            |> Task.map scheduling

    let run (scheduler: MaintenanceScheduler) (ct: CancellationToken) : Task<unit> =
        match scheduler with
        | MaintenanceScheduler.InProcess ->
            Loops.together [ every options.NotifyEvery notify; every options.RunEvery pass ] ct
        | MaintenanceScheduler.InDatabase -> every options.RunEvery checkDrift ct

    override _.ExecuteAsync(ct: CancellationToken) : Task =
        backgroundTask {
            let! scheduler = decide ct
            do! run scheduler ct
        }
