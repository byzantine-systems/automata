namespace ByzantineSystems.Automata.Storage.Postgres

open System
open ByzantineSystems.Automata.Core

/// <summary>All SQL in one place: query strings and their parameter names.</summary>
[<RequireQualifiedAccess>]
module internal Sql =

    let tryGet =
        "SELECT epoch, state, status FROM fsm.instance WHERE machine_id = @machine_id AND entity_id = @entity_id"

    let findReceipt =
        "SELECT epoch, occurred_at FROM fsm.transition WHERE machine_id = @machine_id AND entity_id = @entity_id AND idempotency_key = @idempotency_key"

    let readEpoch =
        "SELECT epoch FROM fsm.instance WHERE machine_id = @machine_id AND entity_id = @entity_id"

    let insertInstance =
        """INSERT INTO fsm.instance (machine_id, entity_id, epoch, state, state_path, status, updated_at)
           VALUES (@machine_id, @entity_id, @epoch, @state::jsonb, @state_path, @status, @updated_at)
           ON CONFLICT (machine_id, entity_id) DO NOTHING"""

    let updateInstance =
        """UPDATE fsm.instance
           SET epoch = @epoch, state = @state::jsonb, state_path = @state_path, status = @status, updated_at = @updated_at
           WHERE machine_id = @machine_id AND entity_id = @entity_id AND epoch = @expected"""

    let insertTransition =
        """INSERT INTO fsm.transition
             (machine_id, entity_id, epoch, occurred_at, event, actions, from_state, to_state, status, handled_by, exited, entered, idempotency_key)
           VALUES (@machine_id, @entity_id, @epoch, @occurred_at, @event::jsonb, @actions::jsonb, @from_state::jsonb, @to_state::jsonb, @status, @handled_by, @exited, @entered, @idempotency_key)"""

    let insertOutbox =
        """INSERT INTO fsm.outbox (machine_id, entity_id, event_idempotency_key, ordinal, action, attempts, next_attempt_at, locked_until)
           SELECT @machine_id, @entity_id, @idempotency_key, o.ordinal, o.action::jsonb, 0, @occurred_at, '-infinity'
           FROM unnest(@actions, @ordinals) AS o(action, ordinal)"""

    let history =
        """SELECT machine_id, entity_id, epoch, occurred_at, event, actions, from_state, to_state, status, handled_by, exited, entered, idempotency_key
           FROM fsm.transition
           WHERE machine_id = @machine_id AND entity_id = @entity_id AND epoch > @cursor
           ORDER BY epoch
           LIMIT @limit"""

    let enqueueRetry =
        """INSERT INTO fsm.retry_queue (machine_id, entity_id, idempotency_key, event, attempts, next_retry_at, locked_until, last_error)
           VALUES (@machine_id, @entity_id, @idempotency_key, @event::jsonb, 0, @next_retry_at, '-infinity', @last_error)
           ON CONFLICT (machine_id, entity_id, idempotency_key)
           DO UPDATE SET next_retry_at = EXCLUDED.next_retry_at, last_error = EXCLUDED.last_error
           RETURNING id"""

    let claimRetries = "SELECT * FROM fsm.claim_retries(@batch, @lease, @now)"

    let completeRetry = "DELETE FROM fsm.retry_queue WHERE id = @id"

    let failRetry =
        "UPDATE fsm.retry_queue SET next_retry_at = @next_retry_at, last_error = @error, locked_until = '-infinity' WHERE id = @id"

    let recordDeadLetter =
        """INSERT INTO fsm.dead_letter (machine_id, entity_id, idempotency_key, event, final_error, attempts, died_at)
           VALUES (@machine_id, @entity_id, @idempotency_key, @event::jsonb, @final_error, @attempts, @died_at)"""

    let claimOutbox = "SELECT * FROM fsm.claim_outbox(@batch, @lease, @now)"

    let completeOutbox =
        """DELETE FROM fsm.outbox
           WHERE machine_id = @machine_id AND entity_id = @entity_id
             AND event_idempotency_key = @event_idempotency_key AND ordinal = @ordinal"""

    let failOutbox =
        """UPDATE fsm.outbox SET next_attempt_at = @next_attempt_at, locked_until = '-infinity'
           WHERE machine_id = @machine_id AND entity_id = @entity_id
             AND event_idempotency_key = @event_idempotency_key AND ordinal = @ordinal"""

/// <summary>Shared column-mapping helpers for the PostgreSQL stores.</summary>
[<RequireQualifiedAccess>]
module internal Db =

    let timestamp (dto: DateTimeOffset) : DateTime = dto.UtcDateTime

    let fromTimestamp (dt: DateTime) : DateTimeOffset = DateTimeOffset(dt, TimeSpan.Zero)

    /// <summary>Maps the F# option lease to the '-infinity' ("not leased") sentinel and back.</summary>
    let lockedUntilToDb (lockedUntil: DateTimeOffset option) : DateTime =
        match lockedUntil with
        | Some dto -> dto.UtcDateTime
        | None -> DateTime.MinValue

    let lockedUntilFromDb (dt: DateTime) : DateTimeOffset option =
        if dt = DateTime.MinValue then
            None
        else
            Some(DateTimeOffset(dt, TimeSpan.Zero))

    let statusToString (status: InstanceStatus) : string =
        match status with
        | InstanceStatus.Running -> "Running"
        | InstanceStatus.Suspended -> "Suspended"
        | InstanceStatus.Terminated -> "Terminated"

    let statusFromString (value: string) : Result<InstanceStatus, StoreError> =
        match value with
        | "Running" -> Ok InstanceStatus.Running
        | "Suspended" -> Ok InstanceStatus.Suspended
        | "Terminated" -> Ok InstanceStatus.Terminated
        | other ->
            Error(
                StoreError.Serialization(
                    typeof<InstanceStatus>.Name,
                    FormatException $"unknown instance status {other}"
                )
            )

    let toStoreError (error: CodecError) : StoreError =
        match error with
        | CodecError.EncodeError(typeName, ex)
        | CodecError.DecodeError(typeName, ex) -> StoreError.Serialization(typeName, ex)
