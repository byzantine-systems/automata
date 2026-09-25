namespace ByzantineSystems.Automata.Storage

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core

/// <summary>
/// Why a store refused to start.
///
/// Store-agnostic on purpose: any provider with a schema can be behind or ahead of the code
/// reading it, and can be missing something it depends on.
/// </summary>
[<RequireQualifiedAccess>]
type BootDefect =
    /// <summary>Nothing has been installed: the store's schema does not exist.</summary>
    | SchemaMissing

    /// <summary>Migrations this build ships that the store has never applied.</summary>
    | SchemaBehind of pending: string list

    /// <summary>
    /// Migrations the store has applied that this build does not know: an older binary pointed
    /// at a newer schema.
    /// </summary>
    | SchemaAhead of unknown: string list

    /// <summary>Something the schema depends on is absent: an extension, a routine.</summary>
    | MissingPrerequisite of name: string

    /// <summary>
    /// The store was configured with something it cannot use, such as a queue name it could not
    /// safely interpolate. Found before the store is touched.
    /// </summary>
    | Misconfigured of reason: string

/// <summary>What a store said when asked whether it can serve.</summary>
[<RequireQualifiedAccess>]
type BootReport =
    | Ready
    | Refused of defects: BootDefect list

/// <summary>
/// Checking that a store can serve, before anything is served.
///
/// Optional, and discovered by type test like the temporal capabilities. A store that implements
/// it checks its preconditions and then performs the idempotent configuration writes it needs
/// before the first command (for PostgreSQL, creating the action queue and registering the
/// machine for maintenance). It writes nothing when a check fails, so a refused boot leaves the
/// store exactly as it found it.
///
/// Refusing is the point. A process that boots into guaranteed write errors reports them one
/// command at a time, long after the deploy that caused them.
/// </summary>
type IStoreBoot =
    abstract Boot: machineId: MachineId * ct: CancellationToken -> Task<Result<BootReport, StoreError>>

/// <summary>
/// Hearing that work may be waiting, from another process.
///
/// Optional. <c>Listen</c> runs until cancelled or until it fails, calling back when the store
/// says a command or an action of this machine may be claimable. Cancellation is how it is meant
/// to end, and it answers <c>Ok</c> when it does. A callback is a hint and nothing more: losing
/// every one of them costs the polling interval and never a command, which is what lets a
/// failed listener be retried rather than treated as a fault.
/// </summary>
type IWorkNotifications =
    abstract Listen:
        machineId: MachineId * onCommands: (unit -> unit) * onActions: (unit -> unit) * ct: CancellationToken ->
            Task<Result<unit, StoreError>>

/// <summary>How long a class of history is kept.</summary>
[<RequireQualifiedAccess>]
type Retain =
    /// <summary>Never deleted.</summary>
    | Forever

    /// <summary>Deleted once older than this.</summary>
    | For of TimeSpan

/// <summary>
/// How long each class of history is kept. Application-defined, because these are business
/// records: the library never deletes one unless told to.
/// </summary>
type RetentionPolicy =
    {
        /// <summary>
        /// A terminal command together with its transition, its error and its undelivered
        /// actions, measured from when the command was received. An entity's newest command is
        /// always kept. A purged idempotency key is forgotten, so this must exceed any client's
        /// retry horizon.
        /// </summary>
        Commands: Retain
        /// <summary>
        /// Superseded beliefs, measured from when they were superseded. This bounds how far back
        /// an as-of read can see.
        /// </summary>
        BeliefHistory: Retain
        /// <summary>Delivered and abandoned actions, which nothing reads back.</summary>
        ActionArchive: Retain
    }

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Storage.RetentionPolicy" />.</summary>
[<RequireQualifiedAccess>]
module RetentionPolicy =

    /// <summary>The default. Deleting audit data is never a default.</summary>
    let keepEverything: RetentionPolicy =
        { Commands = Retain.Forever
          BeliefHistory = Retain.Forever
          ActionArchive = Retain.Forever }

/// <summary>What one maintenance pass did to one machine.</summary>
type MaintenanceReport =
    {
        MachineId: MachineId
        /// <summary>Leases expired long enough ago to be handed back to ready.</summary>
        Reaped: int64
        PurgedCommands: int64
        PurgedBeliefs: int64
        PurgedActions: int64
        /// <summary>Open commands whose derived blocked flag disagrees with their siblings.</summary>
        Drifted: int64
    }

/// <summary>What one notification tick did.</summary>
type NotifyReport =
    {
        /// <summary>How many machines and queues were announced.</summary>
        Sent: int
        /// <summary>
        /// The fraction of the store's notification queue in use, from 0 to 1. A full queue fails
        /// every commit that notifies, and the usual cause is a listening session held inside a
        /// long transaction, which stops the queue being cleared.
        /// </summary>
        QueueUsage: float
    }

/// <summary>What asking the database to schedule its own maintenance came to.</summary>
[<RequireQualifiedAccess>]
type Scheduling =
    /// <summary>The database runs both ticks itself.</summary>
    | Scheduled

    /// <summary>The scheduler extension is not installed here.</summary>
    | Unavailable

    /// <summary>It is installed, and this role may not use it.</summary>
    | NotPermitted

/// <summary>
/// One open command whose derived <c>blocked</c> flag disagrees with its siblings. Carries ids
/// only, never a payload, so it can be logged as it is.
/// </summary>
type BlockedDrift =
    { CommandId: CommandId
      MachineId: MachineId
      EntityId: string
      Sequence: int64
      Blocked: bool
      Expected: bool }

/// <summary>One command a repair changed, and the flag it now carries.</summary>
type RepairedBlock = { CommandId: CommandId; Blocked: bool }

/// <summary>
/// Maintenance of a whole database, rather than of one machine.
///
/// One of these serves every machine sharing the database: each machine registers itself when it
/// boots, and a pass covers everything registered. The same operations run whether the database
/// schedules them itself or the application does, so a deployment without a scheduler in the
/// database behaves identically, only on the application's clock.
/// </summary>
type IDatabaseMaintenance =

    /// <summary>
    /// Wakes whoever is listening for machines and queues with claimable work, and says how full
    /// the notification queue is.
    /// </summary>
    abstract NotifyPending: ct: CancellationToken -> Task<Result<NotifyReport, StoreError>>

    /// <summary>
    /// One pass: reaps long-expired leases, applies each machine's retention, and counts drift
    /// without repairing it. Each task is bounded by <paramref name="batch" />. An empty list
    /// means another process is running a pass right now.
    /// </summary>
    abstract Run: batch: int * ct: CancellationToken -> Task<Result<MaintenanceReport list, StoreError>>

    /// <summary>Every open command whose blocked flag has drifted. Empty means healthy.</summary>
    abstract DetectDrift: ct: CancellationToken -> Task<Result<BlockedDrift list, StoreError>>

    /// <summary>
    /// Rewrites every drifted blocked flag and says which. Never called by a pass: drift is a
    /// symptom of a bug elsewhere, and repairing it silently would hide that bug.
    /// </summary>
    abstract RepairDrift: ct: CancellationToken -> Task<Result<RepairedBlock list, StoreError>>

    /// <summary>
    /// Asks the database to run both ticks itself. Idempotent: asking again updates the schedule
    /// rather than adding a second one.
    /// </summary>
    abstract Schedule:
        notifyEvery: TimeSpan * runEvery: TimeSpan * batch: int * ct: CancellationToken ->
            Task<Result<Scheduling, StoreError>>

    /// <summary>Removes both scheduled ticks. Returns how many were removed.</summary>
    abstract Unschedule: ct: CancellationToken -> Task<Result<int, StoreError>>
