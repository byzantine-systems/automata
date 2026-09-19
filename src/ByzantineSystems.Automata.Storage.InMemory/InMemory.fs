namespace ByzantineSystems.Automata.Storage.InMemory

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage

type private StoreState<'EntityId, 'State, 'Event, 'Action> =
    { Instances: ((MachineId * 'EntityId) * Snapshot<'State>) list
      History: ((MachineId * 'EntityId) * Transition<'EntityId, 'State, 'Event, 'Action> list) list
      Receipts: ((MachineId * 'EntityId * string) * CommitReceipt) list
      Retries: (RetryId * RetryItem<'EntityId, 'Event>) list
      RetryIdsByKey: ((MachineId * 'EntityId * string) * RetryId) list
      Outbox: (OutboxKey<'EntityId> * OutboxItem<'EntityId, 'Action>) list
      DeadLetters: DeadLetter<'EntityId, 'Event> list
      NextRetryId: int64 }

[<RequireQualifiedAccess>]
module private StoreState =

    let empty<'EntityId, 'State, 'Event, 'Action> : StoreState<'EntityId, 'State, 'Event, 'Action> =
        { Instances = []
          History = []
          Receipts = []
          Retries = []
          RetryIdsByKey = []
          Outbox = []
          DeadLetters = []
          NextRetryId = 0L }

    let tryFind key entries =
        entries
        |> List.tryPick (fun (candidate, value) -> if candidate = key then Some value else None)

    // Replace in place or append, matching the observable insertion order of the old stores.
    let upsert key value entries =
        if entries |> List.exists (fun (candidate, _) -> candidate = key) then
            entries
            |> List.map (fun (candidate, current) -> if candidate = key then key, value else candidate, current)
        else
            entries @ [ key, value ]

    let remove key entries =
        entries |> List.filter (fun (candidate, _) -> candidate <> key)

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
    let state = ref StoreState.empty<'EntityId, 'State, 'Event, 'Action>

    let actionKeyOf machineId entityId eventKey ordinal : OutboxKey<'EntityId> =
        { MachineId = machineId
          EntityId = entityId
          EventIdempotencyKey = eventKey
          Ordinal = ordinal }

    let checkCancelled (ct: CancellationToken) = ct.ThrowIfCancellationRequested()

    let read projection =
        lock gate (fun _ -> projection state.Value)

    let update transition =
        lock gate (fun _ -> state.Value <- transition state.Value)

    let transact transition =
        lock gate (fun _ ->
            let next, result = transition state.Value
            state.Value <- next
            result)

    let transactAtCurrentTime transition =
        lock gate (fun _ ->
            let next, result = transition (timeProvider.GetUtcNow()) state.Value
            state.Value <- next
            result)

    let tryGet (machineId: MachineId) (entityId: 'EntityId) : Snapshot<'State> option =
        read (fun current -> current.Instances |> StoreState.tryFind (machineId, entityId))

    let findReceipt (machineId: MachineId) (entityId: 'EntityId) (idempotencyKey: string) : CommitReceipt option =
        read (fun current -> current.Receipts |> StoreState.tryFind (machineId, entityId, idempotencyKey))

    let commitTransition
        (transition: Transition<'EntityId, 'State, 'Event, 'Action>)
        (expected: Epoch)
        (current: StoreState<'EntityId, 'State, 'Event, 'Action>)
        : StoreState<'EntityId, 'State, 'Event, 'Action> * Result<CommitReceipt, StoreError> =
        let instanceKey = transition.MachineId, transition.EntityId

        let receiptKey =
            transition.MachineId, transition.EntityId, transition.IdempotencyKey

        // An already-committed key answers with the original receipt and writes nothing.
        match current.Receipts |> StoreState.tryFind receiptKey with
        | Some receipt -> current, Ok receipt
        | None ->
            let actual =
                current.Instances
                |> StoreState.tryFind instanceKey
                |> Option.map (fun snapshot -> snapshot.Epoch)
                |> Option.defaultValue Epoch.initial

            let wellFormed = actual = expected && transition.Epoch = Epoch.next actual

            if not wellFormed then
                current, Error(Concurrency(expected, actual))
            else
                let receipt =
                    { IdempotencyKey = transition.IdempotencyKey
                      Epoch = transition.Epoch
                      OccurredAt = transition.OccurredAt }

                let snapshot =
                    { State = transition.ToState
                      Epoch = transition.Epoch
                      Status = transition.Status }

                let log =
                    current.History |> StoreState.tryFind instanceKey |> Option.defaultValue []

                let outbox =
                    transition.Actions
                    |> List.indexed
                    |> List.fold
                        (fun items (ordinal, action) ->
                            let actionKey =
                                actionKeyOf transition.MachineId transition.EntityId transition.IdempotencyKey ordinal

                            let item =
                                { ActionKey = actionKey
                                  MachineId = transition.MachineId
                                  EntityId = transition.EntityId
                                  EventIdempotencyKey = transition.IdempotencyKey
                                  Action = action
                                  Attempts = 0
                                  NextAttemptAt = transition.OccurredAt
                                  LockedUntil = None }

                            items |> StoreState.upsert actionKey item)
                        current.Outbox

                { current with
                    Instances = current.Instances |> StoreState.upsert instanceKey snapshot
                    History = current.History |> StoreState.upsert instanceKey (log @ [ transition ])
                    Receipts = current.Receipts |> StoreState.upsert receiptKey receipt
                    Outbox = outbox },
                Ok receipt

    let commit transition expected =
        transact (commitTransition transition expected)

    let historyPage
        (machineId: MachineId)
        (entityId: 'EntityId)
        (page: Page)
        : Transition<'EntityId, 'State, 'Event, 'Action> list =
        read (fun current ->
            let cursor = page |> Page.cursor |> Option.defaultValue Epoch.initial

            current.History
            |> StoreState.tryFind (machineId, entityId)
            |> Option.defaultValue []
            |> List.filter (fun transition -> transition.Epoch > cursor)
            |> List.truncate (Page.limit page))

    let enqueueTransition
        (request: RetryRequest<'EntityId, 'Event>)
        (current: StoreState<'EntityId, 'State, 'Event, 'Action>)
        : StoreState<'EntityId, 'State, 'Event, 'Action> =
        let key = request.MachineId, request.EntityId, request.IdempotencyKey

        let pending =
            current.RetryIdsByKey
            |> StoreState.tryFind key
            |> Option.bind (fun retryId ->
                current.Retries
                |> StoreState.tryFind retryId
                |> Option.map (fun item -> retryId, item))

        match pending with
        | Some(retryId, item) ->
            let refreshed =
                { item with
                    NextAttemptAt = request.NextAttemptAt
                    LastError = request.LastError }

            { current with
                Retries = current.Retries |> StoreState.upsert retryId refreshed }
        | None ->
            let nextRetryId = current.NextRetryId + 1L
            let retryId = RetryId.create nextRetryId

            let item =
                { RetryId = retryId
                  MachineId = request.MachineId
                  EntityId = request.EntityId
                  IdempotencyKey = request.IdempotencyKey
                  Event = request.Event
                  Attempts = 0
                  NextAttemptAt = request.NextAttemptAt
                  LockedUntil = None
                  LastError = request.LastError }

            { current with
                Retries = current.Retries |> StoreState.upsert retryId item
                RetryIdsByKey = current.RetryIdsByKey |> StoreState.upsert key retryId
                NextRetryId = nextRetryId }

    let enqueue request = update (enqueueTransition request)

    let claimRetryTransition
        (now: DateTimeOffset)
        (batch: int)
        (lease: TimeSpan)
        (current: StoreState<'EntityId, 'State, 'Event, 'Action>)
        : StoreState<'EntityId, 'State, 'Event, 'Action> * RetryItem<'EntityId, 'Event> list =
        let claimed =
            current.Retries
            |> List.map snd
            |> List.filter (fun item ->
                item.NextAttemptAt <= now
                && (item.LockedUntil |> Option.forall (fun lockedUntil -> lockedUntil < now)))
            |> List.sortBy (fun item -> item.NextAttemptAt, item.RetryId)
            |> List.truncate batch
            |> List.map (fun item ->
                { item with
                    Attempts = item.Attempts + 1
                    LockedUntil = Some(now.Add lease) })

        let retries =
            claimed
            |> List.fold (fun items item -> items |> StoreState.upsert item.RetryId item) current.Retries

        { current with Retries = retries }, claimed

    let claimRetries batch lease =
        transactAtCurrentTime (fun now -> claimRetryTransition now batch lease)

    let completeRetryTransition
        (retryId: RetryId)
        (current: StoreState<'EntityId, 'State, 'Event, 'Action>)
        : StoreState<'EntityId, 'State, 'Event, 'Action> =
        { current with
            Retries = current.Retries |> StoreState.remove retryId
            RetryIdsByKey =
                current.RetryIdsByKey
                |> List.filter (fun (_, mappedRetryId) -> mappedRetryId <> retryId) }

    let completeRetry retryId =
        update (completeRetryTransition retryId)

    let failRetryTransition
        (retryId: RetryId)
        (nextAttemptAt: DateTimeOffset)
        (error: string)
        (current: StoreState<'EntityId, 'State, 'Event, 'Action>)
        : StoreState<'EntityId, 'State, 'Event, 'Action> =
        match current.Retries |> StoreState.tryFind retryId with
        | None -> current
        | Some item ->
            let failed =
                { item with
                    NextAttemptAt = nextAttemptAt
                    LastError = Some error
                    LockedUntil = None }

            { current with
                Retries = current.Retries |> StoreState.upsert retryId failed }

    let failRetry retryId nextAttemptAt error =
        update (failRetryTransition retryId nextAttemptAt error)

    let claimOutboxTransition
        (now: DateTimeOffset)
        (batch: int)
        (lease: TimeSpan)
        (current: StoreState<'EntityId, 'State, 'Event, 'Action>)
        : StoreState<'EntityId, 'State, 'Event, 'Action> * OutboxItem<'EntityId, 'Action> list =
        let claimed =
            current.Outbox
            |> List.map snd
            |> List.filter (fun item ->
                item.NextAttemptAt <= now
                && (item.LockedUntil |> Option.forall (fun lockedUntil -> lockedUntil < now)))
            |> List.sortBy _.NextAttemptAt
            |> List.truncate batch
            |> List.map (fun item ->
                { item with
                    Attempts = item.Attempts + 1
                    LockedUntil = Some(now.Add lease) })

        let outbox =
            claimed
            |> List.fold (fun items item -> items |> StoreState.upsert item.ActionKey item) current.Outbox

        { current with Outbox = outbox }, claimed

    let claimOutbox batch lease =
        transactAtCurrentTime (fun now -> claimOutboxTransition now batch lease)

    let completeOutbox actionKey =
        update (fun current ->
            { current with
                Outbox = current.Outbox |> StoreState.remove actionKey })

    let failOutboxTransition
        (actionKey: OutboxKey<'EntityId>)
        (nextAttemptAt: DateTimeOffset)
        (current: StoreState<'EntityId, 'State, 'Event, 'Action>)
        : StoreState<'EntityId, 'State, 'Event, 'Action> =
        match current.Outbox |> StoreState.tryFind actionKey with
        | None -> current
        | Some item ->
            let failed =
                { item with
                    NextAttemptAt = nextAttemptAt
                    LockedUntil = None }

            { current with
                Outbox = current.Outbox |> StoreState.upsert actionKey failed }

    let failOutbox actionKey nextAttemptAt =
        update (failOutboxTransition actionKey nextAttemptAt)

    let recordDeadLetter deadLetter =
        update (fun current ->
            { current with
                DeadLetters = current.DeadLetters @ [ deadLetter ] })

    /// <summary>Test-facing view of pending retry items; does not mutate state.</summary>
    member _.PendingRetries() : RetryItem<'EntityId, 'Event> list =
        read (fun current -> current.Retries |> List.map snd)

    /// <summary>Test-facing view of pending outbox items; does not mutate state.</summary>
    member _.PendingOutbox() : OutboxItem<'EntityId, 'Action> list =
        read (fun current -> current.Outbox |> List.map snd)

    /// <summary>Test-facing view of the dead-letter log; does not mutate state.</summary>
    member _.DeadLetterLog() : DeadLetter<'EntityId, 'Event> list =
        read (fun current -> current.DeadLetters)

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
