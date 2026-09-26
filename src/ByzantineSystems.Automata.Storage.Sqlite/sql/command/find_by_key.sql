-- The command already submitted under an idempotency key, if any. Served by
-- command_unique_idem's index.
SELECT
    c.command_id
FROM
    fsm_command c
WHERE
    c.machine_id = @machine_id
    AND c.entity_id = @entity_id
    AND c.idempotency_key = @idempotency_key;
