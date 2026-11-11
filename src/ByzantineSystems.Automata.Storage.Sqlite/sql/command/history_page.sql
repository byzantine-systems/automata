-- One page of an entity's committed transitions, after a cursor. The cursor is an epoch, and an
-- absent cursor is zero, which transition_epoch_positive makes smaller than any stored epoch, so
-- one statement shape serves every page.
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
    fsm_transition
WHERE
    machine_id = @machine_id
    AND entity_id = @entity_id
    AND epoch > @after
ORDER BY
    epoch
LIMIT @limit;
