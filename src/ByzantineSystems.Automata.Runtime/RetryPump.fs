namespace ByzantineSystems.Automata.Runtime

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Resilience
open ByzantineSystems.Automata.Storage

/// <summary>
/// A manual worker that drains the durable retry queue. It claims leased items and
/// re-attempts each through the machine's read-resolve-commit path, completing, rescheduling,
/// or dead-lettering them according to the durable policy. A dropped wake-up hint is
/// recovered by the polling cadence, so no durable work is ever lost.
/// </summary>
type RetryPump<'EntityId, 'State, 'Event, 'Action, 'Err when 'EntityId: equality>
    (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>) =

    let config = Machine.config machine
    let signal = Machine.signal machine
    let observerBus = Machine.observerBus machine

    let recordDeadLetter (item: RetryItem<'EntityId, 'Event>) (error: string) (ct: CancellationToken) : Task =
        task {
            let letter =
                { MachineId = item.MachineId
                  EntityId = item.EntityId
                  IdempotencyKey = item.IdempotencyKey
                  Event = item.Event
                  FinalError = error
                  Attempts = item.Attempts
                  DiedAt = config.TimeProvider.GetUtcNow() }

            let! _ = config.DeadLetter.Record(letter, ct)
            let! _ = config.RetryQueue.Complete(item.RetryId, ct)
            return ()
        }

    let processItem (item: RetryItem<'EntityId, 'Event>) (ct: CancellationToken) : Task =
        task {
            let envelope = EventEnvelope.create item.IdempotencyKey item.Event
            let! result = Send.execute config item.EntityId envelope ct

            match result with
            | CommittedResult transition ->
                match observerBus with
                | Some bus -> bus.TryPublish transition
                | None -> ()

                if not (List.isEmpty transition.Actions) then
                    signal.TrySignal()

                let! _ = config.RetryQueue.Complete(item.RetryId, ct)
                return ()

            | AppliedResult _ ->
                let! _ = config.RetryQueue.Complete(item.RetryId, ct)
                return ()

            | FailedResult error ->
                let reason = sprintf "%A" error

                match config.Classify error with
                | Disposition.Ignore
                | Disposition.Reject ->
                    let! _ = config.RetryQueue.Complete(item.RetryId, ct)
                    return ()

                | Disposition.DeadLetter -> return! recordDeadLetter item reason ct

                | Disposition.Defer ->
                    if item.Attempts >= config.RetryPolicy.MaxAttempts then
                        return! recordDeadLetter item reason ct
                    else
                        let next = config.TimeProvider.GetUtcNow().Add config.RetryPolicy.Delay
                        let! _ = config.RetryQueue.Fail(item.RetryId, next, reason, ct)
                        signal.TrySignal()
                        return ()

                | Disposition.Escalate -> return raise (EscalatedSend reason)
        }

    let processBatch (items: RetryItem<'EntityId, 'Event> list) (ct: CancellationToken) : Task =
        task {
            use semaphore = new SemaphoreSlim(config.RetryPolicy.Concurrency)

            let run (item: RetryItem<'EntityId, 'Event>) =
                task {
                    do! semaphore.WaitAsync(ct)

                    try
                        do! processItem item ct
                    finally
                        semaphore.Release() |> ignore
                }

            let! _ = items |> List.map run |> Task.WhenAll
            return ()
        }

    /// <summary>Claims and processes one batch of due items.</summary>
    member _.PollAsync(ct: CancellationToken) : Task =
        task {
            let! claimed = config.RetryQueue.Claim(config.RetryPolicy.BatchSize, config.RetryPolicy.Lease, ct)

            match claimed with
            | Ok items -> do! processBatch items ct
            | Error _ -> return ()
        }

    /// <summary>Polls until the token is cancelled, waking on the signal or the poll interval.</summary>
    member this.RunAsync(ct: CancellationToken) : Task =
        let rec loop () =
            task {
                try
                    ct.ThrowIfCancellationRequested()
                    do! this.PollAsync ct

                    let signalWait = signal.WaitAsync(ct)

                    let pollWait =
                        Task.Delay(config.RetryPolicy.PollingInterval, config.TimeProvider, ct)

                    let! _ = Task.WhenAny(signalWait, pollWait)
                    return! loop ()
                with :? OperationCanceledException ->
                    return ()
            }

        loop ()
