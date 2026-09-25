namespace ByzantineSystems.Automata.Storage

/// <summary>
/// The complete durable surface one machine needs: the inbox that orders its work, the reader
/// that tells it where an entity stands and where it has been, the atomic end of processing a
/// command, and the queue that delivers what a commit asked for.
///
/// A single value implements all four so the machine builder takes one store dependency, while
/// each concern stays expressible on its own. A third party implementing these four is a
/// complete provider.
/// </summary>
type IMachineStore<'EntityId, 'State, 'Event, 'Action, 'Err> =
    inherit ICommandInbox<'EntityId, 'Event>
    inherit IStateReader<'EntityId, 'State, 'Event, 'Action>
    inherit ICommandProcessorStore<'EntityId, 'State, 'Event, 'Action, 'Err>
    inherit IActionQueue<'EntityId, 'Action>

/// <summary>
/// Capabilities a store may offer beyond the required four.
///
/// Discovery is a runtime type test rather than a member on
/// <see cref="T:ByzantineSystems.Automata.Storage.IMachineStore`5" />, and the alternatives are
/// worse for a specific reason each. Members returning an option would force every provider to
/// write <c>None</c> for capabilities it has never heard of, and every new capability would then
/// be a breaking change to every store that exists. Declaring a capability separately when a
/// machine is wired would let a caller hand over a reader belonging to a different store than the
/// one it configured, with nothing to catch it. The type test is the only shape where adding a
/// capability breaks nobody and the wiring cannot disagree with itself.
///
/// It has one honest limitation. The test is by exact generic instantiation, so a store
/// implementing <c>ITemporalReader</c> at a different <c>'State</c> than the machine's answers
/// <c>None</c> rather than reporting a mismatch. That is the right answer for the caller, who
/// cannot use it either way, but it is worth knowing when a capability is missing that should not
/// be.
/// </summary>
[<RequireQualifiedAccess>]
module Store =

    /// <summary>The store's ability to read the past, or <c>None</c> when it offers none.</summary>
    let tryTemporal
        (store: IMachineStore<'EntityId, 'State, 'Event, 'Action, 'Err>)
        : ITemporalReader<'EntityId, 'State> option =
        match store with
        | :? ITemporalReader<'EntityId, 'State> as temporal -> Some temporal
        | _ -> None

    /// <summary>The store's ability to change the past, or <c>None</c> when it offers none.</summary>
    let tryCorrections
        (store: IMachineStore<'EntityId, 'State, 'Event, 'Action, 'Err>)
        : ICorrectionStore<'EntityId, 'State> option =
        match store with
        | :? ICorrectionStore<'EntityId, 'State> as corrections -> Some corrections
        | _ -> None

    /// <summary>The store's boot check, or <c>None</c> when it has none to run.</summary>
    let tryBoot (store: IMachineStore<'EntityId, 'State, 'Event, 'Action, 'Err>) : IStoreBoot option =
        match store with
        | :? IStoreBoot as boot -> Some boot
        | _ -> None

    /// <summary>
    /// The store's cross-process wake-ups, or <c>None</c> when polling is all it offers.
    /// </summary>
    let tryNotifications (store: IMachineStore<'EntityId, 'State, 'Event, 'Action, 'Err>) : IWorkNotifications option =
        match store with
        | :? IWorkNotifications as notifications -> Some notifications
        | _ -> None

    /// <summary>The store's ability to replay a correction, or <c>None</c> when it offers none.</summary>
    let tryReplay
        (store: IMachineStore<'EntityId, 'State, 'Event, 'Action, 'Err>)
        : IReplayStore<'EntityId, 'State, 'Event, 'Action> option =
        match store with
        | :? IReplayStore<'EntityId, 'State, 'Event, 'Action> as replay -> Some replay
        | _ -> None
