namespace ByzantineSystems.Automata.Runtime

open System
open System.Threading
open System.Threading.Tasks
open FsToolkit.ErrorHandling
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage

/// <summary>Why a correction could not be planned.</summary>
[<RequireQualifiedAccess>]
type CorrectionError<'Err> =
    /// <summary>A store read failed. Worth another attempt.</summary>
    | Store of StoreError
    /// <summary>The history does not replay. Nothing is written.</summary>
    | Replay of ReplayError
    /// <summary>The store cannot read the past or replay into it.</summary>
    | Unsupported

/// <summary>Reading what a correction replays from, then planning it. Shared by commit and preview.</summary>
[<RequireQualifiedAccess>]
module internal Correction =

    /// PostgreSQL keeps microseconds, so this is the instant just before T at the resolution the
    /// beliefs are stored in. The belief in force then is the one the missed event follows; a
    /// belief starting exactly at T came from an event at T, which is replayed rather than
    /// assumed.
    let private justBefore (at: DateTimeOffset) = at.AddTicks -10L

    let plan
        (config: RuntimeConfig<'EntityId, 'State, 'Event, 'Action, 'Err>)
        (entityId: 'EntityId)
        (missed: 'Event)
        (at: DateTimeOffset)
        (policy: CorrectionPolicy)
        (correction: CommandId)
        (ct: CancellationToken)
        : Task<Result<ReplayPlan<'EntityId, 'State, 'Event, 'Action> * Epoch, CorrectionError<'Err>>> =
        match Store.tryTemporal config.Store, Store.tryReplay config.Store with
        | Some temporal, Some replay ->
            let reader = config.Store :> IStateReader<'EntityId, 'State, 'Event, 'Action>

            taskResult {
                let! live = reader.TryGetSnapshot(config.MachineId, entityId, ct)
                let! before = temporal.ValidAt(config.MachineId, entityId, justBefore at, ct)
                // One more than the budget, which is how an over-long suffix is told apart from
                // one exactly at the limit.
                let! suffix = replay.Suffix(config.MachineId, entityId, at, policy.ReplayLimit + 1, ct)

                return live, before, suffix
            }
            |> TaskResult.mapError CorrectionError.Store
            |> TaskResult.bind (fun (live, before, suffix) ->
                let liveState, epoch =
                    live
                    |> Option.map (fun snapshot -> snapshot.State, snapshot.Epoch)
                    |> Option.defaultValue (config.InitialState, Epoch.initial)

                let input =
                    { MachineId = config.MachineId
                      EntityId = entityId
                      Start = before |> Option.map _.Snapshot.State |> Option.defaultValue config.InitialState
                      Live = liveState
                      Missed = missed
                      At = at
                      Correction = correction
                      Current = config.ChartVersion
                      NextEpoch = Epoch.next epoch
                      Policy = policy }

                if suffix.Incomplete then
                    TaskResult.error (CorrectionError.Replay ReplayError.HistoryPurged)
                else
                    Replay.plan config.Catalog input suffix.Transitions
                    |> Result.map (fun plan -> plan, epoch)
                    |> Result.mapError CorrectionError.Replay
                    |> Task.FromResult)
        | _ -> TaskResult.error CorrectionError.Unsupported
