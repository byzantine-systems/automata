namespace ByzantineSystems.Automata.Storage

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core

/// <summary>
/// Database-assigned identity of a submitted command. Opaque: the store owns the conversion,
/// and application code compares ids rather than interpreting them. Values have gaps, because
/// identity allocation is not transactional, and the gaps mean nothing.
/// </summary>
[<Struct>]
type CommandId = private CommandId of int64

/// <summary>
/// A developer-declared chart version, pinned into every command at submission so that replay
/// resolves it against the chart that originally decided it. Never inferred from the chart:
/// keeping it declared is what makes a deliberate change to semantics a deliberate act.
/// </summary>
[<Struct>]
type ChartVersion = private ChartVersion of int

/// <summary>Phantom tag: a lease over a command in the inbox.</summary>
type CommandWork = class end

/// <summary>Phantom tag: a lease over an action awaiting delivery.</summary>
type ActionWork = class end

/// <summary>
/// A time-bounded exclusive claim, identified by a token that fences stale workers. The phantom
/// parameter ties a token to the kind of work it was issued for, using the same trick as
/// <see cref="T:ByzantineSystems.Automata.Core.EntityId`1" />, so an action lease cannot be
/// passed where a command lease is required.
/// </summary>
[<Struct>]
type LeaseToken<'kind> = private LeaseToken of int64

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Storage.CommandId" />.</summary>
[<RequireQualifiedAccess>]
module CommandId =

    /// <summary>Constructs an id from a value read out of storage. Storage layers own this.</summary>
    let ofInt64 (value: int64) : CommandId = CommandId value

    /// <summary>Returns the underlying value for persistence and logging.</summary>
    let value (CommandId value) : int64 = value

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Storage.ChartVersion" />.</summary>
[<RequireQualifiedAccess>]
module ChartVersion =

    /// <summary>Constructs a chart version, rejecting anything below the first one.</summary>
    let tryCreate (value: int) : Result<ChartVersion, string> =
        if value < 1 then
            Error $"A chart version must be 1 or greater, but was {value}."
        else
            Ok(ChartVersion value)

    /// <summary>Constructs a chart version from a literal declared in a machine definition.</summary>
    /// <exception cref="T:System.ArgumentException">The version is below 1.</exception>
    let create (value: int) : ChartVersion =
        match tryCreate value with
        | Ok version -> version
        | Error message -> invalidArg (nameof value) message

    /// <summary>Returns the underlying value.</summary>
    let value (ChartVersion value) : int = value

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Storage.LeaseToken`1" />.</summary>
[<RequireQualifiedAccess>]
module LeaseToken =

    /// <summary>
    /// The token of a work item nobody holds. Live tokens are always greater, so the sentinel
    /// can never be mistaken for one.
    /// </summary>
    let none () : LeaseToken<'kind> = LeaseToken 0L

    /// <summary>Constructs a token from a value read out of storage. Storage layers own this.</summary>
    let ofInt64 (value: int64) : LeaseToken<'kind> = LeaseToken value

    /// <summary>Returns the underlying value for persistence and logging.</summary>
    let value (LeaseToken value) : int64 = value

    /// <summary>Whether this token identifies a live claim rather than the unheld sentinel.</summary>
    let isHeld (token: LeaseToken<'kind>) : bool = value token > 0L

/// <summary>Lifecycle of a command in the inbox. Ready and Leased are non-terminal: a command in
/// either still owns its entity and blocks that entity's later commands.</summary>
[<RequireQualifiedAccess>]
type CommandStatus =
    | Ready
    | Leased
    | Succeeded
    | Rejected
    | DeadLettered

/// <summary>
/// Who caused a command and why, recorded beside it. Every field is the application's; nothing
/// in the claim path reads them. Absent and blank are the same thing here, so absence is
/// <c>None</c> and the store writes the empty string.
/// </summary>
type AuditContext =
    { Tenant: string option
      Principal: string option
      Source: string option
      CorrelationId: string option
      CausationId: string option }

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Storage.AuditContext" />.</summary>
[<RequireQualifiedAccess>]
module AuditContext =

    /// <summary>The context of a command nobody attributed.</summary>
    let empty: AuditContext =
        { Tenant = None
          Principal = None
          Source = None
          CorrelationId = None
          CausationId = None }

/// <summary>An event offered to the inbox, together with everything the audit record needs.</summary>
type CommandSubmission<'EntityId, 'Event> =
    {
        MachineId: MachineId
        EntityId: 'EntityId
        /// <summary>Deduplicates within an entity: resubmitting a key returns the original command.</summary>
        IdempotencyKey: string
        ChartVersion: ChartVersion
        Event: 'Event
        /// <summary>Earliest instant the command may be claimed. <c>None</c> means immediately.</summary>
        VisibleAt: DateTimeOffset option
        /// <summary>
        /// When the caller received the event. <c>None</c> lets the database stamp arrival, which
        /// is the right default; supply it only when the caller genuinely knows better, such as
        /// when replaying a recorded stream.
        /// </summary>
        ReceivedAt: DateTimeOffset option
        Audit: AuditContext
    }

/// <summary>One command as the inbox holds it.</summary>
type CommandRecord<'EntityId, 'Event> =
    {
        CommandId: CommandId
        MachineId: MachineId
        EntityId: 'EntityId
        /// <summary>Gapless per-entity submission order. This is the order history is replayed in.</summary>
        Sequence: int64
        IdempotencyKey: string
        ChartVersion: ChartVersion
        Event: 'Event
        Status: CommandStatus
        /// <summary>An earlier non-terminal command for this entity exists, so this one waits.</summary>
        Blocked: bool
        /// <summary>Earliest claimable instant while ready; the lease deadline while leased.</summary>
        VisibleAt: DateTimeOffset
        /// <summary>Processing attempts the application has reported, not deliveries.</summary>
        Attempts: int
        /// <summary>When the submission reached the database.</summary>
        ReceivedAt: DateTimeOffset
        Audit: AuditContext
    }

/// <summary>
/// A claimed unit of work and the lease that fences it. Completing, rescheduling or extending
/// requires the token, so a worker whose claim expired and was taken over cannot act on it.
/// </summary>
type Leased<'kind, 'Work> =
    {
        Work: 'Work
        Token: LeaseToken<'kind>
        /// <summary>How many times this item has been delivered, redeliveries included.</summary>
        DeliveryCount: int
        /// <summary>Database-assigned instant at which the claim lapses.</summary>
        ExpiresAt: DateTimeOffset
    }

/// <summary>A command claimed for processing.</summary>
type LeasedCommand<'EntityId, 'Event> = Leased<CommandWork, CommandRecord<'EntityId, 'Event>>

/// <summary>
/// What a submission did. Both cases are success: a repeated idempotency key is how a client
/// retry after a lost response is recognised, and it carries the original command's id.
/// </summary>
type SubmissionOutcome =
    | Accepted of accepted: CommandId
    | AlreadySubmitted of existing: CommandId

/// <summary>
/// What a fenced write did. <c>LeaseLost</c> is an outcome, not an error: it is the expected
/// answer when a worker stalled past its lease and the work was reclaimed.
/// </summary>
type LeaseUpdateOutcome =
    | Updated
    | LeaseLost

/// <summary>
/// Retry envelope for a rescheduled command. The store computes the actual delay, jittered, from
/// the attempt count; the caller only says how far apart attempts may drift.
/// </summary>
[<Struct>]
type Backoff =
    private
        { Base: TimeSpan
          Ceiling: TimeSpan }

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Storage.Backoff" />.</summary>
[<RequireQualifiedAccess>]
module Backoff =

    /// <summary>Builds an envelope, rejecting a non-positive base or a ceiling below it.</summary>
    let tryCreate (baseDelay: TimeSpan) (ceiling: TimeSpan) : Result<Backoff, string> =
        if baseDelay <= TimeSpan.Zero then
            Error "A backoff base delay must be positive."
        elif ceiling < baseDelay then
            Error "A backoff ceiling must be at least the base delay."
        else
            Ok { Base = baseDelay; Ceiling = ceiling }

    /// <summary>Builds an envelope.</summary>
    /// <exception cref="T:System.ArgumentException">The delays are not a valid envelope.</exception>
    let create (baseDelay: TimeSpan) (ceiling: TimeSpan) : Backoff =
        match tryCreate baseDelay ceiling with
        | Ok backoff -> backoff
        | Error message -> invalidArg (nameof baseDelay) message

    /// <summary>One second doubling to five minutes.</summary>
    let defaults: Backoff =
        { Base = TimeSpan.FromSeconds 1.0
          Ceiling = TimeSpan.FromMinutes 5.0 }

    /// <summary>The delay before the first retry.</summary>
    let baseDelay (backoff: Backoff) : TimeSpan = backoff.Base

    /// <summary>The delay no retry exceeds.</summary>
    let ceiling (backoff: Backoff) : TimeSpan = backoff.Ceiling

/// <summary>
/// The durable command inbox: submission, claiming, and the fenced writes that finish a claim.
///
/// This is the unit of ordering. At most one non-terminal command per entity is claimable, so a
/// claim excludes that entity across every process, which is what makes per-entity history an
/// order rather than a race.
/// </summary>
type ICommandInbox<'EntityId, 'Event> =

    /// <summary>Records a command durably. Idempotent on the submission's key.</summary>
    abstract Submit:
        submission: CommandSubmission<'EntityId, 'Event> * ct: CancellationToken ->
            Task<Result<SubmissionOutcome, StoreError>>

    /// <summary>
    /// Leases up to <paramref name="batch" /> runnable commands for a machine. Never returns two
    /// commands for the same entity. The lease outlives this call: processing happens outside any
    /// transaction, and the token is what protects the work meanwhile.
    /// </summary>
    abstract Claim:
        machineId: MachineId * batch: int * lease: TimeSpan * ct: CancellationToken ->
            Task<Result<LeasedCommand<'EntityId, 'Event> list, StoreError>>

    /// <summary>
    /// Returns a failed command for a later attempt. It keeps its place in the entity's order, so
    /// nothing else for that entity runs ahead of it.
    /// </summary>
    abstract Reschedule:
        commandId: CommandId * token: LeaseToken<CommandWork> * backoff: Backoff * ct: CancellationToken ->
            Task<Result<LeaseUpdateOutcome, StoreError>>

    /// <summary>Pushes out the deadline of a lease still held, for work that runs long.</summary>
    abstract ExtendLease:
        commandId: CommandId * token: LeaseToken<CommandWork> * lease: TimeSpan * ct: CancellationToken ->
            Task<Result<LeaseUpdateOutcome, StoreError>>

    /// <summary>Reads a command's current state without waiting for it.</summary>
    abstract TryGet:
        commandId: CommandId * ct: CancellationToken ->
            Task<Result<CommandRecord<'EntityId, 'Event> option, StoreError>>

    /// <summary>Reads a command by the key it was submitted under.</summary>
    abstract TryFind:
        machineId: MachineId * entityId: 'EntityId * idempotencyKey: string * ct: CancellationToken ->
            Task<Result<CommandRecord<'EntityId, 'Event> option, StoreError>>
