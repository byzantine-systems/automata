namespace ByzantineSystems.Automata.Runtime

open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage

/// <summary>
/// The messages an entity actor's mailbox understands. Every reply is completed through a
/// <c>TaskCompletionSource</c> created with <c>RunContinuationsAsynchronously</c> so a
/// caller's continuation never runs on the actor's loop. The per-message token is the
/// caller's token: cancellation before mailbox admission means the message never enters
/// the queue at all.
/// </summary>
type internal ActorMessage<'EntityId, 'State, 'Event, 'Action, 'Err> =
    | Send of EventEnvelope<'Event> * CancellationToken * TaskCompletionSource<Result<SendOutcome, MachineError<'Err>>>
    | ReadState of CancellationToken * TaskCompletionSource<Result<Snapshot<'State> option, MachineError<'Err>>>
