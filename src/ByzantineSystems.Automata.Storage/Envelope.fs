namespace ByzantineSystems.Automata.Storage

open System

/// <summary>
/// An event offered to a machine, together with everything the durable inbox needs that the
/// machine itself cannot supply. The idempotency key is mandatory: a client retry after a lost
/// response must never create a second command.
///
/// This is the caller's half of a <see cref="T:ByzantineSystems.Automata.Storage.CommandSubmission`2" />.
/// The machine adds its own id, the entity and the declared chart version, because those are
/// configuration rather than per-event data, and a caller that could vary them could submit a
/// command under a chart it was never resolved against.
/// </summary>
type EventEnvelope<'Event> =
    private
        { Event: 'Event
          IdempotencyKey: string
          Audit: AuditContext
          VisibleAt: DateTimeOffset option
          ReceivedAt: DateTimeOffset option }

/// <summary>Expected validation failures when constructing an event envelope.</summary>
type EventEnvelopeError =
    /// <summary>An idempotency key is required to make retries safe.</summary>
    | EmptyIdempotencyKey

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Storage.EventEnvelope`1" />.</summary>
[<RequireQualifiedAccess>]
module EventEnvelope =

    /// <summary>
    /// Validates and wraps an event under a stable idempotency key. Use this at an
    /// untrusted input boundary where an empty key is an expected caller error.
    /// </summary>
    let tryCreate (idempotencyKey: string) (event: 'Event) : Result<EventEnvelope<'Event>, EventEnvelopeError> =
        if String.IsNullOrWhiteSpace idempotencyKey then
            Error EmptyIdempotencyKey
        else
            Ok
                { Event = event
                  IdempotencyKey = idempotencyKey.Trim()
                  Audit = AuditContext.empty
                  VisibleAt = None
                  ReceivedAt = None }

    /// <summary>
    /// Wraps an event under a trusted, stable idempotency key, trimming surrounding
    /// whitespace. Prefer <see cref="M:ByzantineSystems.Automata.Storage.EventEnvelope.tryCreate``1(System.String,``0)" />
    /// when the key comes from an untrusted caller.
    /// </summary>
    /// <exception cref="T:System.ArgumentException">The key is null, empty, or whitespace.</exception>
    let create (idempotencyKey: string) (event: 'Event) : EventEnvelope<'Event> =
        match tryCreate idempotencyKey event with
        | Ok envelope -> envelope
        | Error EmptyIdempotencyKey ->
            invalidArg (nameof idempotencyKey) "An event envelope requires a non-empty idempotency key."

    /// <summary>Attaches the audit context recorded beside the command.</summary>
    let withAudit (audit: AuditContext) (envelope: EventEnvelope<'Event>) : EventEnvelope<'Event> =
        { envelope with Audit = audit }

    /// <summary>Attaches a causation id: the identity of the command or message that produced this event.</summary>
    let withCausation (causationId: string) (envelope: EventEnvelope<'Event>) : EventEnvelope<'Event> =
        { envelope with
            Audit =
                { envelope.Audit with
                    CausationId = Some causationId } }

    /// <summary>Attaches a correlation id used to trace one workflow across machines and services.</summary>
    let withCorrelation (correlationId: string) (envelope: EventEnvelope<'Event>) : EventEnvelope<'Event> =
        { envelope with
            Audit =
                { envelope.Audit with
                    CorrelationId = Some correlationId } }

    /// <summary>
    /// Holds the command back until the given instant. Until then it still owns its place in
    /// the entity's order, so nothing submitted after it runs ahead of it.
    /// </summary>
    let withVisibleAt (visibleAt: DateTimeOffset) (envelope: EventEnvelope<'Event>) : EventEnvelope<'Event> =
        { envelope with
            VisibleAt = Some visibleAt }

    /// <summary>
    /// Declares when the caller received the event. Leaving it unset lets the database stamp
    /// arrival, which is the right default; supply it only when the caller genuinely knows
    /// better, such as when replaying a recorded stream.
    /// </summary>
    let withReceivedAt (receivedAt: DateTimeOffset) (envelope: EventEnvelope<'Event>) : EventEnvelope<'Event> =
        { envelope with
            ReceivedAt = Some receivedAt }

    /// <summary>The wrapped event.</summary>
    let event (envelope: EventEnvelope<'Event>) : 'Event = envelope.Event

    /// <summary>The stable idempotency key.</summary>
    let idempotencyKey (envelope: EventEnvelope<'Event>) : string = envelope.IdempotencyKey

    /// <summary>The audit context recorded beside the command.</summary>
    let audit (envelope: EventEnvelope<'Event>) : AuditContext = envelope.Audit

    /// <summary>The optional causation id.</summary>
    let causationId (envelope: EventEnvelope<'Event>) : string option = envelope.Audit.CausationId

    /// <summary>The optional correlation id.</summary>
    let correlationId (envelope: EventEnvelope<'Event>) : string option = envelope.Audit.CorrelationId

    /// <summary>The earliest instant the command may be claimed; <c>None</c> means immediately.</summary>
    let visibleAt (envelope: EventEnvelope<'Event>) : DateTimeOffset option = envelope.VisibleAt

    /// <summary>The caller-declared arrival instant; <c>None</c> lets the database stamp it.</summary>
    let receivedAt (envelope: EventEnvelope<'Event>) : DateTimeOffset option = envelope.ReceivedAt
