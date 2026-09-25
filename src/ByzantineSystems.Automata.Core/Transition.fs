namespace ByzantineSystems.Automata.Core

/// <summary>
/// Optimistic-concurrency token. Epochs are gapless per entity: a commit that advances the
/// epoch must also append exactly one transition at that epoch, which makes the log both
/// auditable and conflict-detectable.
/// </summary>
[<Struct>]
type Epoch = private Epoch of uint64

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Core.Epoch" />.</summary>
[<RequireQualifiedAccess>]
module Epoch =

    /// <summary>The epoch a new instance starts from.</summary>
    let initial: Epoch = Epoch 0UL

    /// <summary>The epoch a successful commit produces.</summary>
    let next (epoch: Epoch) : Epoch =
        let (Epoch value) = epoch
        Epoch(value + 1UL)

    /// <summary>
    /// Constructs an epoch from a raw counter read from storage. Storage layers own this
    /// conversion; application code goes through <see cref="M:ByzantineSystems.Automata.Core.Epoch.initial" />
    /// and <see cref="M:ByzantineSystems.Automata.Core.Epoch.next" />.
    /// </summary>
    let ofUInt64 (value: uint64) : Epoch = Epoch value

    /// <summary>Returns the raw counter for persistence.</summary>
    let value (epoch: Epoch) : uint64 =
        let (Epoch value) = epoch
        value

/// <summary>Lifecycle status of a persisted instance.</summary>
type InstanceStatus =
    | Running
    | Suspended
    | Terminated

/// <summary>
/// The materialised state of one entity together with its concurrency token and lifecycle
/// status. The epoch travels with the snapshot so <c>Commit</c> can be given the expected
/// value without a separate <c>GetEpoch</c> round trip.
/// </summary>
type Snapshot<'State> =
    { State: 'State
      Epoch: Epoch
      Status: InstanceStatus }

// A committed transition is no longer declared here. It is the store's answer rather than
// the runtime's, so it lives beside the contract that produces it:
// `ByzantineSystems.Automata.Storage.TransitionDraft` is what pure resolution decides, and
// `CommittedTransition` is that draft plus the epoch, command id, chart version and commit
// instant that only the database can supply. `CommitReceipt` went with them: a receipt was a
// proxy for "this command already happened", and `CommandResult` answers that question with
// the outcome itself.
