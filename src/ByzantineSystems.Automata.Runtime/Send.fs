namespace ByzantineSystems.Automata.Runtime

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Resilience
open ByzantineSystems.Automata.Storage

/// <summary>Raw outcome of one send attempt, before disposition and durable side effects.</summary>
type internal SendResult<'EntityId, 'State, 'Event, 'Action, 'Err> =
    | CommittedResult of Transition<'EntityId, 'State, 'Event, 'Action>
    | AppliedResult of CommitReceipt
    | FailedResult of MachineError<'Err>

/// <summary>
/// The read-resolve-commit machinery shared by the actor path and the durable retry pump.
/// The whole operation (idempotency check, snapshot read, resolution, and commit) runs
/// inside the resilience pipeline so every concurrency conflict re-reads and re-resolves
/// against the winning state rather than replaying a stale transition.
/// </summary>
module internal Send =

    let private rejectNotRunning<'Err> (status: InstanceStatus) : MachineError<'Err> =
        MachineError.Rejected(MachineRejection.InstanceNotRunning status)

    /// <summary>Runs one attempt under the shared pipeline, returning the raw outcome.</summary>
    let execute
        (config: RuntimeConfig<'EntityId, 'State, 'Event, 'Action, 'Err>)
        (entityId: 'EntityId)
        (envelope: EventEnvelope<'Event>)
        (ct: CancellationToken)
        : Task<SendResult<'EntityId, 'State, 'Event, 'Action, 'Err>> =
        task {
            let event = EventEnvelope.event envelope
            let key = EventEnvelope.idempotencyKey envelope
            let machineId = config.MachineId
            let applied = ref false
            let committedTransition = ref None

            let operation (opCt: CancellationToken) : Task<PipelineResult<'Err>> =
                task {
                    let! receipt = config.Store.FindReceipt(machineId, entityId, key, opCt)

                    match receipt with
                    | Error e -> return Error(MachineError.Store e)
                    | Ok(Some found) ->
                        applied.Value <- true
                        return Ok found
                    | Ok None ->
                        let! snapshot = config.Store.TryGet(machineId, entityId, opCt)

                        match snapshot with
                        | Error e -> return Error(MachineError.Store e)
                        | Ok snapshotOption ->
                            let state, epoch, status =
                                match snapshotOption with
                                | None -> config.InitialState, Epoch.initial, InstanceStatus.Running
                                | Some current -> current.State, current.Epoch, current.Status

                            if status <> InstanceStatus.Running then
                                return Error(rejectNotRunning status)
                            else
                                match Chart.resolve config.Chart state event with
                                | Error transitionError -> return Error(MachineError.Transition transitionError)
                                | Ok resolution ->
                                    let nextStatus =
                                        if Chart.isTerminal config.Chart resolution.Next then
                                            InstanceStatus.Terminated
                                        else
                                            InstanceStatus.Running

                                    let occurredAt = config.TimeProvider.GetUtcNow()

                                    let transition =
                                        { MachineId = machineId
                                          EntityId = entityId
                                          IdempotencyKey = key
                                          OccurredAt = occurredAt
                                          Epoch = Epoch.next epoch
                                          Event = event
                                          Actions = resolution.Actions
                                          FromState = state
                                          ToState = resolution.Next
                                          Status = nextStatus
                                          HandledBy = resolution.HandledBy
                                          Exited = resolution.Exited
                                          Entered = resolution.Entered }

                                    let! commitResult = config.Store.Commit(transition, epoch, opCt)

                                    match commitResult with
                                    | Error e -> return Error(MachineError.Store e)
                                    | Ok receipt ->
                                        committedTransition.Value <- Some transition
                                        return Ok receipt
                }

            let! pipelineResult = Retry.execute config.Pipeline operation ct

            match pipelineResult with
            | Ok receipt when applied.Value -> return AppliedResult receipt
            | Ok receipt ->
                match committedTransition.Value with
                | Some transition -> return CommittedResult transition
                | None -> return AppliedResult receipt
            | Error e -> return FailedResult e
        }

    /// <summary>
    /// Maps a raw attempt to a caller-facing outcome, applying the disposition exactly once
    /// and performing the durable side effects (observer, wake signal, enqueue, dead letter).
    /// </summary>
    let dispatch
        (config: RuntimeConfig<'EntityId, 'State, 'Event, 'Action, 'Err>)
        (entityId: 'EntityId)
        (envelope: EventEnvelope<'Event>)
        (observer: ObserverDispatcher<'EntityId, 'State, 'Event, 'Action> option)
        (retrySignal: WorkSignal)
        (outboxSignal: WorkSignal)
        (ct: CancellationToken)
        : Task<Result<SendOutcome, MachineError<'Err>>> =
        task {
            let! result = execute config entityId envelope ct

            match result with
            | CommittedResult transition ->
                match observer with
                | Some bus -> bus.TryPublish transition
                | None -> ()

                if not (List.isEmpty transition.Actions) then
                    outboxSignal.TrySignal()

                return Ok Committed

            | AppliedResult _ -> return Ok AlreadyApplied

            | FailedResult error ->
                match config.Classify error with
                | Disposition.Reject -> return Error error
                | Disposition.Ignore -> return Ok Ignored
                | Disposition.Defer ->
                    let event = EventEnvelope.event envelope
                    let key = EventEnvelope.idempotencyKey envelope

                    let request =
                        { MachineId = config.MachineId
                          EntityId = entityId
                          IdempotencyKey = key
                          Event = event
                          NextAttemptAt =
                            config.TimeProvider.GetUtcNow().Add((ValidatedRetryPolicy.value config.RetryPolicy).Delay)
                          LastError = Some(sprintf "%A" error) }

                    let! enqueued = config.RetryQueue.Enqueue(request, ct)

                    match enqueued with
                    | Error storeError -> return Error(MachineError.Store storeError)
                    | Ok retryId ->
                        retrySignal.TrySignal()
                        return Ok(Deferred retryId)

                | Disposition.DeadLetter ->
                    let event = EventEnvelope.event envelope
                    let key = EventEnvelope.idempotencyKey envelope

                    let letter =
                        { MachineId = config.MachineId
                          EntityId = entityId
                          IdempotencyKey = key
                          Event = event
                          FinalError = sprintf "%A" error
                          Attempts = 0
                          DiedAt = config.TimeProvider.GetUtcNow() }

                    let! recorded = config.DeadLetter.Record(letter, ct)

                    match recorded with
                    | Error storeError -> return Error(MachineError.Store storeError)
                    | Ok() -> return Error error

                | Disposition.Escalate -> return raise (EscalatedSend(sprintf "%A" error))
        }
