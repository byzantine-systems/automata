-- The entity's oldest surviving transition. Retention purges an entity's oldest commands with
-- their transitions, so the oldest epoch says whether the log still starts at the beginning.
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
ORDER BY
    epoch
LIMIT 1;

