namespace ByzantineSystems.Automata.Core

open System

/// <summary>Expected failures while resolving an event against a chart.</summary>
type TransitionError<'Err> =
    /// <summary>No rule matched anywhere on the chain from the active leaf to the root.</summary>
    | Unhandled of state: StateId * event: string

    /// <summary>A rule matched and returned a domain error.</summary>
    | Rejected of 'Err

    /// <summary>An explicit guard on the matched rule refused the event.</summary>
    | GuardFailed of state: StateId * reason: string

    /// <summary>The classifier produced an id that is not a declared leaf of the chart.</summary>
    | UnknownState of StateId

    /// <summary>The rule's concrete next state did not map to the resolved target leaf.</summary>
    | TargetMismatch of expected: StateId * actual: StateId

/// <summary>
/// Expected failures from the durable store. Splitting infrastructure failure out of the
/// domain error type is what makes "is this retryable?" answerable:
/// <see cref="F:ByzantineSystems.Automata.Core.StoreError.Unavailable" /> is transient by
/// definition; <see cref="F:ByzantineSystems.Automata.Core.StoreError.Concurrency" /> is safe
/// to retry from a fresh snapshot; the rest are not retry candidates.
/// </summary>
type StoreError =
    /// <summary>Another writer won the epoch race. Retry from the latest snapshot.</summary>
    | Concurrency of expected: Epoch * actual: Epoch

    /// <summary>The stored representation could not be encoded or decoded.</summary>
    | Serialization of typeName: string * exn

    /// <summary>The store could not be reached or answered with a transient failure.</summary>
    | Unavailable of exn

    /// <summary>The referenced entity does not exist.</summary>
    | NotFound of entity: string

/// <summary>
/// A machine lifecycle state that prevents new work from being accepted. These are
/// expected, caller-actionable outcomes, so the public API reports them as data rather
/// than throwing lifecycle exceptions or asking callers to parse a reason string.
/// </summary>
type MachineRejection =
    /// <summary>The machine must be started before it accepts sends or state reads.</summary>
    | NotStarted

    /// <summary>Shutdown has started, so no new work can be admitted.</summary>
    | Stopping

    /// <summary>Shutdown has completed.</summary>
    | Stopped

    /// <summary>The persisted entity lifecycle does not permit another transition.</summary>
    | InstanceNotRunning of InstanceStatus

/// <summary>
/// The only error type that crosses the public machine API. Only
/// <see cref="F:ByzantineSystems.Automata.Core.MachineError`1.Store" />
/// <c>Unavailable</c> and <see cref="F:ByzantineSystems.Automata.Core.MachineError`1.Timeout" />
/// are candidates for retry; caller cancellation and
/// <see cref="F:ByzantineSystems.Automata.Core.MachineError`1.Rejected" /> are not. A breaker
/// rejection must not trigger an immediate retry against the same open breaker.
/// </summary>
type MachineError<'Err> =
    /// <summary>The pure resolution step failed in an expected way.</summary>
    | Transition of TransitionError<'Err>

    /// <summary>The durable store failed in an expected way.</summary>
    | Store of StoreError

    /// <summary>An attempt, or the whole operation, exceeded its budget.</summary>
    | Timeout of TimeSpan

    /// <summary>The shared pipeline's breaker is open; carry on after the given delay, if known.</summary>
    | CircuitOpen of retryAfter: TimeSpan option

    /// <summary>The machine refused work because its lifecycle does not permit it.</summary>
    | Rejected of MachineRejection
