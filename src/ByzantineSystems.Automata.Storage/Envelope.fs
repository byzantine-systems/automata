namespace ByzantineSystems.Automata.Storage

open System
open ByzantineSystems.Automata.Core

/// <summary>
/// An event paired with the metadata every durable boundary requires. The idempotency key is
/// mandatory: a client retry after a lost response must never create a second transition.
/// Causation and correlation ids are optional tracing metadata carried alongside.
/// </summary>
type EventEnvelope<'Event> =
    private
        { Event: 'Event
          IdempotencyKey: string
          CausationId: string option
          CorrelationId: string option }

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Storage.EventEnvelope`1" />.</summary>
[<RequireQualifiedAccess>]
module EventEnvelope =

    /// <summary>Wraps an event under a stable idempotency key, trimming surrounding whitespace.</summary>
    /// <exception cref="T:System.ArgumentException">The key is null, empty, or whitespace.</exception>
    let create (idempotencyKey: string) (event: 'Event) : EventEnvelope<'Event> =
        if String.IsNullOrWhiteSpace idempotencyKey then
            invalidArg (nameof idempotencyKey) "An event envelope requires a non-empty idempotency key."

        { Event = event
          IdempotencyKey = idempotencyKey.Trim()
          CausationId = None
          CorrelationId = None }

    /// <summary>Attaches a causation id: the identity of the command or message that produced this event.</summary>
    let withCausation (causationId: string) (envelope: EventEnvelope<'Event>) : EventEnvelope<'Event> =
        { envelope with
            CausationId = Some causationId }

    /// <summary>Attaches a correlation id used to trace one workflow across machines and services.</summary>
    let withCorrelation (correlationId: string) (envelope: EventEnvelope<'Event>) : EventEnvelope<'Event> =
        { envelope with
            CorrelationId = Some correlationId }

    /// <summary>The wrapped event.</summary>
    let event (envelope: EventEnvelope<'Event>) : 'Event = envelope.Event

    /// <summary>The stable idempotency key.</summary>
    let idempotencyKey (envelope: EventEnvelope<'Event>) : string = envelope.IdempotencyKey

    /// <summary>The optional causation id.</summary>
    let causationId (envelope: EventEnvelope<'Event>) : string option = envelope.CausationId

    /// <summary>The optional correlation id.</summary>
    let correlationId (envelope: EventEnvelope<'Event>) : string option = envelope.CorrelationId

/// <summary>Identifies one entry in a durable retry queue. Ids are assigned by the store.</summary>
[<Struct>]
type RetryId = private RetryId of int64

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Storage.RetryId" />.</summary>
[<RequireQualifiedAccess>]
module RetryId =

    /// <summary>Constructs a retry id from a positive store-assigned counter.</summary>
    /// <exception cref="T:System.ArgumentException">The value is below 1.</exception>
    let create (value: int64) : RetryId =
        if value < 1L then
            invalidArg (nameof value) "A retry id must be a positive counter."

        RetryId value

    /// <summary>Returns the underlying counter for persistence.</summary>
    let value (RetryId value) : int64 = value

/// <summary>
/// An event being handed to the durable retry queue. Identity is assigned by the store on
/// enqueue, so a request carries no id; attempts start at zero and are counted per claim.
/// </summary>
type RetryRequest<'EntityId, 'Event> =
    {
        MachineId: MachineId
        EntityId: 'EntityId
        IdempotencyKey: string
        Event: 'Event
        /// <summary>Earliest time the item may be claimed again.</summary>
        NextAttemptAt: DateTimeOffset
        /// <summary>The error that caused the deferral, for diagnostics.</summary>
        LastError: string option
    }

/// <summary>
/// A claimed or pending retry-queue entry. <c>Attempts</c> counts claims that have been made
/// (the store increments it on every lease); <c>LockedUntil</c> is the current lease deadline,
/// if any. The durable maximum and its schedule are the pump's policy, not the store's.
/// </summary>
type RetryItem<'EntityId, 'Event> =
    {
        RetryId: RetryId
        MachineId: MachineId
        EntityId: 'EntityId
        IdempotencyKey: string
        Event: 'Event
        Attempts: int
        NextAttemptAt: DateTimeOffset
        /// <summary>Lease deadline held by the claiming worker; <c>None</c> when unleased.</summary>
        LockedUntil: DateTimeOffset option
        LastError: string option
    }

/// <summary>
/// An event the machine has permanently given up on. Dead letters are facts, not work: they
/// are recorded for inspection and never reprocessed automatically.
/// </summary>
type DeadLetter<'EntityId, 'Event> =
    {
        MachineId: MachineId
        EntityId: 'EntityId
        IdempotencyKey: string
        Event: 'Event
        /// <summary>Human-readable final error that ended the retry cycle.</summary>
        FinalError: string
        Attempts: int
        DiedAt: DateTimeOffset
    }

/// <summary>
/// Stable identity of one action within a machine instance. The full scope prevents actions
/// emitted by different machines or entities from colliding when event idempotency keys match.
/// </summary>
type OutboxKey<'EntityId> =
    { MachineId: MachineId
      EntityId: 'EntityId
      EventIdempotencyKey: string
      Ordinal: int }

/// <summary>
/// One pending side effect produced by a committed transition. Redelivery carries the same
/// scoped action key so the destination can detect a repeated effect after a crash.
/// </summary>
type OutboxItem<'EntityId, 'Action> =
    {
        ActionKey: OutboxKey<'EntityId>
        MachineId: MachineId
        EntityId: 'EntityId
        /// <summary>The idempotency key of the event that emitted this action.</summary>
        EventIdempotencyKey: string
        Action: 'Action
        Attempts: int
        NextAttemptAt: DateTimeOffset
        /// <summary>Lease deadline held by the dispatching worker; <c>None</c> when unleased.</summary>
        LockedUntil: DateTimeOffset option
    }

/// <summary>
/// An ascending page over an entity's transition log, bounded by an exclusive epoch cursor
/// and a row limit. The cursor makes pages deterministic: a page pinned to an epoch does not
/// change as new transitions arrive.
/// </summary>
type Page =
    private
        { AfterEpoch: Epoch option
          Limit: int }

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Storage.Page" />.</summary>
[<RequireQualifiedAccess>]
module Page =

    /// <summary>Builds the first page of a history query with the given row limit.</summary>
    /// <exception cref="T:System.ArgumentException">The limit is below 1.</exception>
    let create (limit: int) : Page =
        if limit < 1 then
            invalidArg (nameof limit) "A page must request at least one row."

        { AfterEpoch = None; Limit = limit }

    /// <summary>Builds a page continuing strictly after the given epoch.</summary>
    /// <exception cref="T:System.ArgumentException">The limit is below 1.</exception>
    let after (epoch: Epoch) (limit: int) : Page =
        if limit < 1 then
            invalidArg (nameof limit) "A page must request at least one row."

        { AfterEpoch = Some epoch
          Limit = limit }

    /// <summary>The exclusive lower epoch bound; <c>None</c> reads from the beginning of the log.</summary>
    let cursor (page: Page) : Epoch option = page.AfterEpoch

    /// <summary>The maximum number of rows the page requests.</summary>
    let limit (page: Page) : int = page.Limit
