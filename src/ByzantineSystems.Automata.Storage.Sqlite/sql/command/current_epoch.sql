-- The entity's current epoch, which a commit's expected epoch must equal. No
-- row means the entity has never committed, which is epoch zero.
SELECT
    s.epoch
FROM
    fsm_entity_snapshot s
WHERE
    s.machine_id = @machine_id
    AND s.entity_id = @entity_id;
