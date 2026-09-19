namespace ByzantineSystems.Automata.Runtime

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Storage

/// <summary>
/// A manual worker that drains the transactional action outbox. Each action is delivered
/// through a handler that receives the stable <see cref="T:ByzantineSystems.Automata.Storage.OutboxKey`1" />,
/// so a destination can detect at-least-once redelivery after a crash between the external
/// effect and completion. Delivery concurrency is bounded independently of the retry pump.
/// </summary>
type ActionDispatcher<'EntityId, 'Action>
    (
        outbox: IActionOutbox<'EntityId, 'Action>,
        handler: OutboxKey<'EntityId> -> 'Action -> CancellationToken -> Task<Result<unit, string>>,
        policy: RetryPolicy,
        timeProvider: TimeProvider,
        signal: WorkSignal
    ) =

    let processItem (item: OutboxItem<'EntityId, 'Action>) (ct: CancellationToken) : Task =
        task {
            let! result = handler item.ActionKey item.Action ct

            match result with
            | Ok() ->
                let! _ = outbox.Complete(item.ActionKey, ct)
                return ()
            | Error _ ->
                if item.Attempts >= policy.MaxAttempts then
                    let! _ = outbox.Complete(item.ActionKey, ct)
                    return ()
                else
                    let next = timeProvider.GetUtcNow().Add policy.Delay
                    let! _ = outbox.Fail(item.ActionKey, next, ct)
                    return ()
        }

    let processBatch (items: OutboxItem<'EntityId, 'Action> list) (ct: CancellationToken) : Task =
        task {
            use semaphore = new SemaphoreSlim(policy.Concurrency)

            let run (item: OutboxItem<'EntityId, 'Action>) =
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

    /// <summary>Claims and dispatches one batch of due actions.</summary>
    member _.PollAsync(ct: CancellationToken) : Task =
        task {
            let! claimed = outbox.Claim(policy.BatchSize, policy.Lease, ct)

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
                    let pollWait = Task.Delay(policy.PollingInterval, timeProvider, ct)
                    let! _ = Task.WhenAny(signalWait, pollWait)
                    return! loop ()
                with :? OperationCanceledException ->
                    return ()
            }

        loop ()
