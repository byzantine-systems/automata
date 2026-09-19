namespace ByzantineSystems.Automata.Resilience

/// <summary>
/// The final handling of a failed send, decided exactly once by the machine's
/// <c>Classify</c> hook after the Polly pipeline has finished. Retry and breaker
/// behaviour has already happened by the time a disposition is chosen; a disposition
/// is the policy for what happens next.
/// </summary>
type Disposition =

    /// <summary>Expected domain error: return it to the caller, no durable work.</summary>
    | Reject

    /// <summary>
    /// Transient after short retries, or the circuit is open: write a durable retry and
    /// report a deferred outcome. The event has not been applied yet.
    /// </summary>
    | Defer

    /// <summary>Permanent failure: record a dead letter and stop retrying.</summary>
    | DeadLetter

    /// <summary>Explicitly configured no-op, for example for an unhandled event.</summary>
    | Ignore

    /// <summary>The machine itself is unhealthy: report the failure to the supervisor.</summary>
    | Escalate
