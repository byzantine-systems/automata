namespace ByzantineSystems.Automata.Storage

/// <summary>
/// The complete durable surface one machine needs: the inbox that orders its work, the reader
/// that tells it where an entity stands, the atomic end of processing a command, and the queue
/// that delivers what a commit asked for.
///
/// A single value implements all four so the machine builder takes one store dependency, while
/// each concern stays expressible on its own. A third party implementing these four is a
/// complete provider; the optional capabilities described in the design (valid-time queries,
/// corrections, host-transaction enlistment) are discovered by type test and never widen this
/// contract.
/// </summary>
type IMachineStore<'EntityId, 'State, 'Event, 'Action, 'Err> =
    inherit ICommandInbox<'EntityId, 'Event>
    inherit IStateReader<'EntityId, 'State>
    inherit ICommandProcessorStore<'EntityId, 'State, 'Event, 'Action, 'Err>
    inherit IActionQueue<'EntityId, 'Action>
