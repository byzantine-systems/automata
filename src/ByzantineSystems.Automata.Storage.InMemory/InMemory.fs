namespace ByzantineSystems.Automata.Storage.InMemory

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage

/// <summary>
/// Thread-safe reference implementation of every storage contract, for tests and samples.
/// All state lives behind one lock and no operation awaits while holding it, so every
/// observable interleaving is linearisable. Scheduling decisions (leases, due times) are
/// taken against the injected <see cref="T:System.TimeProvider" /> so retry behaviour is
/// testable with a fake clock.
/// </summary>
type InMemoryStore<'EntityId, 'State, 'Event, 'Action when 'EntityId: equality>(?timeProvider: TimeProvider) =

    let timeProvider = defaultArg timeProvider TimeProvider.System
    let gate = obj ()

    // (machine, entity) -> live snapshot
    let instances = Dictionary<MachineId * 'EntityId, Snapshot<'State>>()

    // (machine, entity) -> append-only log, ordered by epoch
    let history =
        Dictionary<MachineId * 'EntityId, ResizeArray<Transition<'EntityId, 'State, 'Event, 'Action>>>()

    // (machine, entity, event key) -> receipt
    let receipts = Dictionary<MachineId * 'EntityId * string, CommitReceipt>()

    // retry queue: assigned identity -> item, plus the reverse index for idempotent enqueue
    let retries = Dictionary<RetryId, RetryItem<'EntityId, 'Event>>()
    let retryIdByKey = Dictionary<MachineId * 'EntityId * string, RetryId>()

    // action outbox: action key -> item
    let outbox = Dictionary<string, OutboxItem<'EntityId, 'Action>>()

    let deadLetters = ResizeArray<DeadLetter<'EntityId, 'Event>>()
    let mutable nextRetryId = 0L

    let actionKeyOf (eventKey: string) (ordinal: int) = $"%s{eventKey}#{ordinal}"

    let checkCancelled (ct: CancellationToken) = ct.ThrowIfCancellationRequested()

    let tryGet (machineId: MachineId) (entityId: 'EntityId) : Snapshot<'State> option =
        lock gate (fun _ ->
            match instances.TryGetValue((machineId, entityId)) with
            | true, snapshot -> Some snapshot
            | _ -> None)

    let findReceipt (machineId: MachineId) (entityId: 'EntityId) (idempotencyKey: string) : CommitReceipt option =
        lock gate (fun _ ->
            match receipts.TryGetValue((machineId, entityId, idempotencyKey)) with
            | true, receipt -> Some receipt
            | _ -> None)

    let commit
        (transition: Transition<'EntityId, 'State, 'Event, 'Action>)
        (expected: Epoch)
        : Result<CommitReceipt, StoreError> =
        lock gate (fun _ ->
            let instanceKey = (transition.MachineId, transition.EntityId)

            let receiptKey =
                (transition.MachineId, transition.EntityId, transition.IdempotencyKey)

            // An already-committed key answers with the original receipt and writes nothing.
            match receipts.TryGetValue receiptKey with
            | true, receipt -> Ok receipt
            | _ ->
                let actual =
                    match instances.TryGetValue instanceKey with
                    | true, snapshot -> snapshot.Epoch
                    | false, _ -> Epoch.initial

                let wellFormed = actual = expected && transition.Epoch = Epoch.next actual

                if not wellFormed then
                    Error(Concurrency(expected, actual))
                else
                    let receipt =
                        { IdempotencyKey = transition.IdempotencyKey
                          Epoch = transition.Epoch
                          OccurredAt = transition.OccurredAt }

                    instances[instanceKey] <-
                        { State = transition.ToState
                          Epoch = transition.Epoch
                          Status = transition.Status }

                    let log =
                        match history.TryGetValue instanceKey with
                        | true, log -> log
                        | false, _ ->
                            let log = ResizeArray()
                            history[instanceKey] <- log
                            log

                    log.Add transition
                    receipts[receiptKey] <- receipt

                    transition.Actions
                    |> List.iteri (fun ordinal action ->
                        outbox[actionKeyOf transition.IdempotencyKey ordinal] <-
                            { ActionKey = actionKeyOf transition.IdempotencyKey ordinal
                              MachineId = transition.MachineId
                              EntityId = transition.EntityId
                              EventIdempotencyKey = transition.IdempotencyKey
                              Action = action
                              Attempts = 0
                              NextAttemptAt = transition.OccurredAt
                              LockedUntil = None })

                    Ok receipt)

    let historyPage
        (machineId: MachineId)
        (entityId: 'EntityId)
        (page: Page)
        : Transition<'EntityId, 'State, 'Event, 'Action> list =
        lock gate (fun _ ->
            match history.TryGetValue((machineId, entityId)) with
            | false, _ -> []
            | true, log ->
                // Log epochs start at 1, so an absent cursor degrades to `> initial`.
                let cursor = page |> Page.cursor |> Option.defaultValue Epoch.initial

                log
                |> Seq.filter (fun transition -> transition.Epoch > cursor)
                |> Seq.truncate (Page.limit page)
                |> List.ofSeq)

    let enqueue (request: RetryRequest<'EntityId, 'Event>) : unit =
        lock gate (fun _ ->
            let key = (request.MachineId, request.EntityId, request.IdempotencyKey)

            match retryIdByKey.TryGetValue key with
            | true, retryId when retries.ContainsKey retryId ->
                // Still pending: refresh the schedule, keep identity and attempt count.
                retries[retryId] <-
                    { retries[retryId] with
                        NextAttemptAt = request.NextAttemptAt
                        LastError = request.LastError
                        LockedUntil = None }
            | _ ->
                nextRetryId <- nextRetryId + 1L
                let retryId = RetryId.create nextRetryId

                retries[retryId] <-
                    { RetryId = retryId
                      MachineId = request.MachineId
                      EntityId = request.EntityId
                      IdempotencyKey = request.IdempotencyKey
                      Event = request.Event
                      Attempts = 0
                      NextAttemptAt = request.NextAttemptAt
                      LockedUntil = None
                      LastError = request.LastError }

                retryIdByKey[key] <- retryId)

    let claimRetries (batch: int) (lease: TimeSpan) : RetryItem<'EntityId, 'Event> list =
        lock gate (fun _ ->
            let now = timeProvider.GetUtcNow()

            let due =
                retries.Values
                |> Seq.filter (fun item ->
                    item.NextAttemptAt <= now
                    && (item.LockedUntil |> Option.forall (fun lockedUntil -> lockedUntil < now)))
                |> Seq.sortBy (fun item -> item.NextAttemptAt, item.RetryId)
                |> Seq.truncate batch
                |> List.ofSeq

            due
            |> List.map (fun item ->
                let claimed =
                    { item with
                        Attempts = item.Attempts + 1
                        LockedUntil = Some(now.Add lease) }

                retries[item.RetryId] <- claimed
                claimed))

    let completeRetry (retryId: RetryId) : unit =
        lock gate (fun _ ->
            retries.Remove retryId |> ignore

            retryIdByKey
            |> Seq.toList
            |> List.iter (fun (KeyValue(key, id)) ->
                if id = retryId then
                    retryIdByKey.Remove(key) |> ignore))

    let failRetry (retryId: RetryId) (nextAttemptAt: DateTimeOffset) (error: string) : unit =
        lock gate (fun _ ->
            match retries.TryGetValue retryId with
            | true, item ->
                retries[retryId] <-
                    { item with
                        NextAttemptAt = nextAttemptAt
                        LastError = Some error
                        LockedUntil = None }
            | false, _ -> ())

    let claimOutbox (batch: int) (lease: TimeSpan) : OutboxItem<'EntityId, 'Action> list =
        lock gate (fun _ ->
            let now = timeProvider.GetUtcNow()

            let due =
                outbox.Values
                |> Seq.filter (fun item ->
                    item.NextAttemptAt <= now
                    && (item.LockedUntil |> Option.forall (fun lockedUntil -> lockedUntil < now)))
                |> Seq.sortBy (fun item -> item.NextAttemptAt, item.ActionKey)
                |> Seq.truncate batch
                |> List.ofSeq

            due
            |> List.map (fun item ->
                let claimed =
                    { item with
                        Attempts = item.Attempts + 1
                        LockedUntil = Some(now.Add lease) }

                outbox[item.ActionKey] <- claimed
                claimed))

    let completeOutbox (actionKey: string) : unit =
        lock gate (fun _ -> outbox.Remove actionKey |> ignore)

    let failOutbox (actionKey: string) (nextAttemptAt: DateTimeOffset) : unit =
        lock gate (fun _ ->
            match outbox.TryGetValue actionKey with
            | true, item ->
                outbox[actionKey] <-
                    { item with
                        NextAttemptAt = nextAttemptAt
                        LockedUntil = None }
            | false, _ -> ())

    let recordDeadLetter (deadLetter: DeadLetter<'EntityId, 'Event>) : unit =
        lock gate (fun _ -> deadLetters.Add deadLetter)

    /// <summary>Test-facing view of pending retry items; does not mutate state.</summary>
    member _.PendingRetries() : RetryItem<'EntityId, 'Event> list =
        lock gate (fun _ -> retries.Values |> List.ofSeq)

    /// <summary>Test-facing view of pending outbox items; does not mutate state.</summary>
    member _.PendingOutbox() : OutboxItem<'EntityId, 'Action> list =
        lock gate (fun _ -> outbox.Values |> List.ofSeq)

    /// <summary>Test-facing view of the dead-letter log; does not mutate state.</summary>
    member _.DeadLetterLog() : DeadLetter<'EntityId, 'Event> list =
        lock gate (fun _ -> List.ofSeq deadLetters)

    interface IStateStore<'EntityId, 'State, 'Event, 'Action> with

        member _.TryGet(machineId, entityId, ct) =
            checkCancelled ct
            tryGet machineId entityId |> Ok |> Task.FromResult

        member _.FindReceipt(machineId, entityId, idempotencyKey, ct) =
            checkCancelled ct
            findReceipt machineId entityId idempotencyKey |> Ok |> Task.FromResult

        member _.Commit(transition, expected, ct) =
            checkCancelled ct
            commit transition expected |> Task.FromResult

        member _.History(machineId, entityId, paging, ct) =
            checkCancelled ct
            historyPage machineId entityId paging |> Ok |> Task.FromResult

    interface IRetryQueue<'EntityId, 'Event> with

        member _.Enqueue(request, ct) =
            checkCancelled ct
            enqueue request
            Ok() |> Task.FromResult

        member _.Claim(batch, lease, ct) =
            checkCancelled ct
            claimRetries batch lease |> Ok |> Task.FromResult

        member _.Complete(retryId, ct) =
            checkCancelled ct
            completeRetry retryId
            Ok() |> Task.FromResult

        member _.Fail(retryId, nextAttemptAt, error, ct) =
            checkCancelled ct
            failRetry retryId nextAttemptAt error
            Ok() |> Task.FromResult

    interface IDeadLetterStore<'EntityId, 'Event> with

        member _.Record(deadLetter, ct) =
            checkCancelled ct
            recordDeadLetter deadLetter
            Ok() |> Task.FromResult

    interface IActionOutbox<'EntityId, 'Action> with

        member _.Claim(batch, lease, ct) =
            checkCancelled ct
            claimOutbox batch lease |> Ok |> Task.FromResult

        member _.Complete(actionKey, ct) =
            checkCancelled ct
            completeOutbox actionKey
            Ok() |> Task.FromResult

        member _.Fail(actionKey, nextAttemptAt, ct) =
            checkCancelled ct
            failOutbox actionKey nextAttemptAt
            Ok() |> Task.FromResult
