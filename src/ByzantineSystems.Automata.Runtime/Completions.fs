namespace ByzantineSystems.Automata.Runtime

open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Storage

/// <summary>
/// Where a caller waiting on <c>Machine.send</c> parks until its command finishes.
///
/// A local waiter alone would be wrong. Any worker on any host may claim the command, and only
/// the one that claims it learns the outcome first, so a registration here is a shortcut and
/// never the mechanism. The mechanism is asking the store. This exists so that the common case,
/// where the machine that submitted the command is also the one that processes it, costs one
/// notification instead of a poll.
/// </summary>
type internal Completions<'EntityId, 'State, 'Event, 'Action, 'Err>() =

    let waiting =
        ConcurrentDictionary<CommandId, TaskCompletionSource<CommandResult<'EntityId, 'State, 'Event, 'Action, 'Err>>>()

    /// <summary>
    /// Registers interest in a command before it can possibly finish, and returns the task to
    /// await. Registering happens before the local processor is woken, so the window in which a
    /// result could be published with nobody listening does not exist.
    /// </summary>
    member _.Register(commandId: CommandId) =
        waiting.GetOrAdd(
            commandId,
            fun _ ->
                TaskCompletionSource<CommandResult<'EntityId, 'State, 'Event, 'Action, 'Err>>(
                    TaskCreationOptions.RunContinuationsAsynchronously
                )
        )

    /// <summary>
    /// Publishes a result to whoever is waiting locally. A command nobody here submitted is the
    /// normal case on a second host, and costs a dictionary miss.
    /// </summary>
    member _.Publish(commandId: CommandId, result: CommandResult<'EntityId, 'State, 'Event, 'Action, 'Err>) : unit =
        match waiting.TryRemove commandId with
        | true, completion -> completion.TrySetResult result |> ignore
        | _ -> ()

    /// <summary>Stops waiting, whether the result arrived or the caller gave up first.</summary>
    member _.Forget(commandId: CommandId) : unit = waiting.TryRemove commandId |> ignore

    /// <summary>
    /// Fails every pending waiter, for a machine that is shutting down. A caller blocked on a
    /// command whose processor has stopped would otherwise wait for a notification that is
    /// never coming; the command itself is durable and unaffected.
    /// </summary>
    member _.CancelAll() : unit =
        for entry in waiting.Keys do
            match waiting.TryRemove entry with
            | true, completion -> completion.TrySetCanceled() |> ignore
            | _ -> ()
