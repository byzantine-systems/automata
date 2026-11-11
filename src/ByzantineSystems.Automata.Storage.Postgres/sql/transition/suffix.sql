-- The events that took effect at or after a corrected instant, earliest first, for a correction
-- to replay. Ordering by effective_at then epoch is deterministic: two transitions of one entity
-- cannot share an epoch, which is the log's key.
SELECT
    machine_id,
    entity_id,
    epoch,
    command_id,
    chart_version,
    event,
    actions,
    from_state,
    to_state,
    handled_by,
    exited,
    entered,
    status,
    effective_at,
    committed_at
FROM
    fsm.transition
WHERE
    machine_id = @machine_id
    AND entity_id = @entity_id
    AND effective_at >= @from
ORDER BY
    effective_at,
    epoch
LIMIT @limit;

