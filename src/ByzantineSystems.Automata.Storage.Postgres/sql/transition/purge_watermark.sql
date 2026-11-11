-- The greatest effective instant retention has removed for this entity, or no row when nothing
-- has ever been purged. Absence means the timeline still starts at the beginning, so a suffix
-- from any instant is complete; the store compares the value against the requested start.
SELECT
    max_purged_effective_at
FROM
    fsm.entity_purge
WHERE
    machine_id = @machine_id
    AND entity_id = @entity_id;

