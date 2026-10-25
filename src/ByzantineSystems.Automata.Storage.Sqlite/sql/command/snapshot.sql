-- The entity's current snapshot. One row per entity: the table's key.
SELECT
    machine_id,
    entity_id,
    state,
    status,
    epoch
FROM
    fsm_entity_snapshot
WHERE
    machine_id = @machine_id
    AND entity_id = @entity_id;
