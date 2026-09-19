namespace ByzantineSystems.Automata.Runtime

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage

/// <summary>
/// The map from entity to its actor loop. A <see cref="T:System.Lazy`1" /> guarantees the
/// loop starts exactly once even when many senders race a first send: the dictionary's
/// factory only creates the lazy holder, and only the winning holder's value is evaluated.
/// Idle actors can be evicted atomically by key and value so a stale entry never removes a
/// replacement.
/// </summary>
type internal Registry<'EntityId, 'State, 'Event, 'Action, 'Err when 'EntityId: equality>
    (
        config: RuntimeConfig<'EntityId, 'State, 'Event, 'Action, 'Err>,
        observerBus: ObserverDispatcher<'EntityId, 'State, 'Event, 'Action> option,
        retrySignal: WorkSignal,
        outboxSignal: WorkSignal
    ) =

    let actors =
        ConcurrentDictionary<'EntityId, Lazy<EntityActor<'EntityId, 'State, 'Event, 'Action, 'Err>>>()

    let faulted =
        TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

    let createActor (entityId: 'EntityId) : Lazy<EntityActor<'EntityId, 'State, 'Event, 'Action, 'Err>> =
        let mutable holder =
            Unchecked.defaultof<Lazy<EntityActor<'EntityId, 'State, 'Event, 'Action, 'Err>>>

        holder <-
            Lazy<EntityActor<'EntityId, 'State, 'Event, 'Action, 'Err>>(fun () ->
                let actor = EntityActor(config, entityId, observerBus, retrySignal, outboxSignal)

                actor.Completion.ContinueWith(
                    (fun (completion: Task) ->
                        if completion.IsFaulted then
                            let error = completion.Exception.GetBaseException()
                            faulted.TrySetException(error) |> ignore

                        actors.TryRemove(KeyValuePair(entityId, holder)) |> ignore),
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default
                )
                |> ignore

                actor)

        holder

    /// <summary>Returns the actor for the entity, starting its loop exactly once.</summary>
    member _.GetOrCreate(entityId: 'EntityId) : EntityActor<'EntityId, 'State, 'Event, 'Action, 'Err> =
        actors.GetOrAdd(entityId, fun _ -> createActor entityId).Value

    /// <summary>Number of live actor entries (started or not). Test-facing; internal.</summary>
    member internal _.ActorCount: int = actors.Count
    member internal _.Faulted: Task = faulted.Task

    /// <summary>True when an actor's loop has been started for the entity. Test-facing; internal.</summary>
    member internal _.IsStarted(entityId: 'EntityId) : bool =
        match actors.TryGetValue entityId with
        | true, holder -> holder.IsValueCreated
        | _ -> false

    /// <summary>Routes a send through the entity's actor.</summary>
    member this.Send
        (entityId: 'EntityId, envelope: EventEnvelope<'Event>, ct: CancellationToken)
        : Task<Result<SendOutcome, MachineError<'Err>>> =
        task {
            let actor = this.GetOrCreate entityId
            return! actor.Send(envelope, ct)
        }

    /// <summary>Routes a state read through the entity's actor, ordered behind queued sends.</summary>
    member this.State
        (entityId: 'EntityId, ct: CancellationToken)
        : Task<Result<Snapshot<'State> option, MachineError<'Err>>> =
        task {
            let actor = this.GetOrCreate entityId
            return! actor.State(ct)
        }

    /// <summary>
    /// Removes the actor for an entity only if the observed entry is the current one and
    /// has been idle for the configured timeout. In-flight sends on the old actor are not
    /// interrupted, so work is never lost; a racing send simply creates a fresh actor.
    /// </summary>
    member _.TryEvictIdle(entityId: 'EntityId) : unit =
        match actors.TryGetValue entityId with
        | true, holder when holder.IsValueCreated && holder.Value.IsIdle(config.IdleTimeout) ->
            if actors.TryRemove(KeyValuePair(entityId, holder)) then
                holder.Value.Stop()
        | _ -> ()

    /// <summary>Stops every actor: mailboxes complete, loops drain, then exit.</summary>
    member _.StopAsync(ct: CancellationToken) : Task =
        task {
            let running =
                actors.Values
                |> Seq.choose (fun holder -> if holder.IsValueCreated then Some holder.Value else None)
                |> Array.ofSeq

            running |> Array.iter (fun actor -> actor.Stop())

            let completions = running |> Array.map (fun actor -> actor.Completion)
            let! _ = Task.WhenAll(completions).WaitAsync(ct)
            return ()
        }
