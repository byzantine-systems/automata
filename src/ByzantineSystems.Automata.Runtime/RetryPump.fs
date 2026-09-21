namespace ByzantineSystems.Automata.Runtime

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Resilience
open ByzantineSystems.Automata.Storage

type private RetryItemOutcome =
    | RetryCompleted
    | RetryRescheduled
    | RetryAbandoned

/// <summary>
/// A manual worker that drains the durable retry queue. It claims leased items and
/// re-attempts each through the machine's read-resolve-commit path, completing, rescheduling,
/// or dead-lettering them according to the durable policy. A dropped wake-up hint is
/// recovered by the polling cadence, so no durable work is ever lost.
/// </summary>
type RetryPump<'EntityId, 'State, 'Event, 'Action, 'Err when 'EntityId: equality>
    (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>) =

    let config = Machine.config machine
    let retrySignal = Machine.retrySignal machine
    let outboxSignal = Machine.outboxSignal machine
    let observerBus = Machine.observerBus machine
    let policy = ValidatedRetryPolicy.value config.RetryPolicy

    let storeResult (work: Task<Result<unit, StoreError>>) : Task<Result<unit, WorkerError>> =
        task {
            let! result = work
            return Result.mapError StoreFailure result
        }

    let recordDeadLetter
        (item: RetryItem<'EntityId, 'Event>)
        (error: string)
        (ct: CancellationToken)
        : Task<Result<RetryItemOutcome, WorkerError>> =
        task {
            let letter =
                { MachineId = item.MachineId
                  EntityId = item.EntityId
                  IdempotencyKey = item.IdempotencyKey
                  Event = item.Event
                  FinalError = error
                  Attempts = item.Attempts
                  DiedAt = config.TimeProvider.GetUtcNow() }

            let! recorded = storeResult (config.DeadLetter.Record(letter, ct))

            match recorded with
            | Error error -> return Error error
            | Ok() ->
                let! completed = storeResult (config.RetryQueue.Complete(item.RetryId, ct))
                return Result.map (fun () -> RetryAbandoned) completed
        }

    let processItem
        (item: RetryItem<'EntityId, 'Event>)
        (ct: CancellationToken)
        : Task<Result<RetryItemOutcome, WorkerError>> =
        task {
            let envelope = EventEnvelope.create item.IdempotencyKey item.Event
            let! result = Send.execute config item.EntityId envelope ct

            match result with
            | CommittedResult transition ->
                match observerBus with
                | Some bus -> bus.TryPublish transition
                | None -> ()

                if not (List.isEmpty transition.Actions) then
                    outboxSignal.TrySignal()

                let! completed = storeResult (config.RetryQueue.Complete(item.RetryId, ct))
                return Result.map (fun () -> RetryCompleted) completed

            | AppliedResult _ ->
                let! completed = storeResult (config.RetryQueue.Complete(item.RetryId, ct))
                return Result.map (fun () -> RetryCompleted) completed

            | FailedResult error ->
                let reason = sprintf "%A" error

                match config.Classify error with
                | Disposition.Ignore
                | Disposition.Reject ->
                    let! completed = storeResult (config.RetryQueue.Complete(item.RetryId, ct))
                    return Result.map (fun () -> RetryCompleted) completed

                | Disposition.DeadLetter -> return! recordDeadLetter item reason ct

                | Disposition.Defer ->
                    if item.Attempts >= policy.MaxAttempts then
                        return! recordDeadLetter item reason ct
                    else
                        let next = config.TimeProvider.GetUtcNow().Add policy.Delay
                        let! failed = storeResult (config.RetryQueue.Fail(item.RetryId, next, reason, ct))

                        match failed with
                        | Error error -> return Error error
                        | Ok() ->
                            retrySignal.TrySignal()
                            return Ok RetryRescheduled

                | Disposition.Escalate -> return raise (EscalatedSend reason)
        }

    let processBatch
        (items: RetryItem<'EntityId, 'Event> list)
        (ct: CancellationToken)
        : Task<Result<PollSummary, WorkerError>> =
        task {
            use semaphore = new SemaphoreSlim(policy.Concurrency)

            let run (item: RetryItem<'EntityId, 'Event>) =
                task {
                    do! semaphore.WaitAsync(ct)
                    let! outcome = processItem item ct |> TaskOutcome.capture
                    semaphore.Release() |> ignore

                    match outcome with
                    | Ok result -> return result
                    | Error error -> return raise error
                }

            let! outcomes = items |> List.map run |> Task.WhenAll

            let folder summary outcome =
                match summary, outcome with
                | Error error, _ -> Error error
                | _, Error error -> Error error
                | Ok current, Ok RetryCompleted ->
                    Ok
                        { current with
                            Completed = current.Completed + 1 }
                | Ok current, Ok RetryRescheduled ->
                    Ok
                        { current with
                            Rescheduled = current.Rescheduled + 1 }
                | Ok current, Ok RetryAbandoned ->
                    Ok
                        { current with
                            Abandoned = current.Abandoned + 1 }

            return
                outcomes
                |> Array.fold
                    folder
                    (Ok
                        { Claimed = items.Length
                          Completed = 0
                          Rescheduled = 0
                          Abandoned = 0 })
        }

    /// <summary>Claims and processes one batch of due items.</summary>
    member _.PollAsync(ct: CancellationToken) : Task<Result<PollSummary, WorkerError>> =
        task {
            let! claimed = config.RetryQueue.Claim(policy.BatchSize, policy.Lease, ct)

            match claimed with
            | Ok items -> return! processBatch items ct
            | Error error -> return Error(StoreFailure error)
        }

    /// <summary>Polls until the token is cancelled, waking on the signal or the poll interval.</summary>
    member this.RunAsync(ct: CancellationToken) : Task<Result<unit, WorkerError>> =
        task {
            let mutable outcome: Result<unit, WorkerError> option = None

            while outcome.IsNone && not ct.IsCancellationRequested do
                let! polled = this.PollAsync ct

                match polled with
                | Error error -> outcome <- Some(Error error)
                | Ok _ ->
                    let! signalOpen = retrySignal.WaitOrTimeoutAsync(policy.PollingInterval, config.TimeProvider, ct)

                    if not signalOpen then
                        outcome <- Some(Ok())

            return
                match outcome with
                | Some result -> result
                | None -> Ok()
        }
