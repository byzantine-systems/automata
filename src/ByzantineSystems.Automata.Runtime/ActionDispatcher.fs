namespace ByzantineSystems.Automata.Runtime

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage

type private ActionItemOutcome =
    | ActionCompleted
    | ActionRescheduled
    | ActionAbandoned

module private ActionDispatcherPolicy =

    let validate (policy: RetryPolicy) =
        match RetryPolicy.validate policy with
        | Ok validated -> validated
        | Error errors -> invalidArg (nameof policy) $"Invalid retry policy: %A{errors}"

/// <summary>
/// A manual worker that drains the transactional action outbox. Each action is delivered
/// through a handler that receives the stable <see cref="T:ByzantineSystems.Automata.Storage.OutboxKey`1" />,
/// so a destination can detect at-least-once redelivery after a crash between the external
/// effect and completion. Delivery concurrency is bounded independently of the retry pump.
/// </summary>
type ActionDispatcher<'EntityId, 'Action, 'EffectError>
    (
        outbox: IActionOutbox<'EntityId, 'Action>,
        handler: OutboxKey<'EntityId> -> 'Action -> CancellationToken -> Task<Result<unit, 'EffectError>>,
        validatedPolicy: ValidatedRetryPolicy,
        timeProvider: TimeProvider,
        signal: WorkSignal
    ) =

    let policy = ValidatedRetryPolicy.value validatedPolicy

    let storeResult (work: Task<Result<unit, StoreError>>) : Task<Result<unit, WorkerError>> =
        task {
            let! result = work
            return Result.mapError StoreFailure result
        }

    let processItem
        (item: OutboxItem<'EntityId, 'Action>)
        (ct: CancellationToken)
        : Task<Result<ActionItemOutcome, WorkerError>> =
        task {
            let! result = handler item.ActionKey item.Action ct

            match result with
            | Ok() ->
                let! completed = storeResult (outbox.Complete(item.ActionKey, ct))
                return Result.map (fun () -> ActionCompleted) completed
            | Error _ ->
                if item.Attempts >= policy.MaxAttempts then
                    let! completed = storeResult (outbox.Complete(item.ActionKey, ct))
                    return Result.map (fun () -> ActionAbandoned) completed
                else
                    let next = timeProvider.GetUtcNow().Add policy.Delay
                    let! failed = storeResult (outbox.Fail(item.ActionKey, next, ct))
                    return Result.map (fun () -> ActionRescheduled) failed
        }

    let processBatch
        (items: OutboxItem<'EntityId, 'Action> list)
        (ct: CancellationToken)
        : Task<Result<PollSummary, WorkerError>> =
        task {
            use semaphore = new SemaphoreSlim(policy.Concurrency)

            let run (item: OutboxItem<'EntityId, 'Action>) =
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
                | Ok current, Ok ActionCompleted ->
                    Ok
                        { current with
                            Completed = current.Completed + 1 }
                | Ok current, Ok ActionRescheduled ->
                    Ok
                        { current with
                            Rescheduled = current.Rescheduled + 1 }
                | Ok current, Ok ActionAbandoned ->
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

    /// <summary>Constructs a dispatcher after validating a raw durable retry policy.</summary>
    new
        (
            outbox: IActionOutbox<'EntityId, 'Action>,
            handler: OutboxKey<'EntityId> -> 'Action -> CancellationToken -> Task<Result<unit, 'EffectError>>,
            policy: RetryPolicy,
            timeProvider: TimeProvider,
            signal: WorkSignal
        ) =
        ActionDispatcher<'EntityId, 'Action, 'EffectError>(
            outbox,
            handler,
            ActionDispatcherPolicy.validate policy,
            timeProvider,
            signal
        )

    /// <summary>Claims and dispatches one batch of due actions.</summary>
    member _.PollAsync(ct: CancellationToken) : Task<Result<PollSummary, WorkerError>> =
        task {
            let! claimed = outbox.Claim(policy.BatchSize, policy.Lease, ct)

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
                    let! signalOpen = signal.WaitOrTimeoutAsync(policy.PollingInterval, timeProvider, ct)

                    if not signalOpen then
                        outcome <- Some(Ok())

            return
                match outcome with
                | Some result -> result
                | None -> Ok()
        }

/// <summary>Construction helpers for machine-bound outbox dispatchers.</summary>
[<RequireQualifiedAccess>]
module ActionDispatcher =

    /// <summary>Creates a dispatcher that shares the machine's outbox and wake signal.</summary>
    let forMachine
        (handler: OutboxKey<'EntityId> -> 'Action -> CancellationToken -> Task<Result<unit, 'EffectError>>)
        (machine: Machine<'EntityId, 'State, 'Event, 'Action, 'Err>)
        : ActionDispatcher<'EntityId, 'Action, 'EffectError> =
        let config = Machine.config machine

        ActionDispatcher(config.Outbox, handler, config.RetryPolicy, config.TimeProvider, Machine.outboxSignal machine)
