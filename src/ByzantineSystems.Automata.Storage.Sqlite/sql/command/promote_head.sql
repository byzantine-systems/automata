-- Promotes the entity's next open command to head.
--
-- Runs after close.sql in the same transaction, so the command just finished
-- is no longer open and cannot be re-elected. A dead letter promotes too: a
-- command the machine gave up on must not wedge its entity forever.
--
-- The subquery reads one entry of fsm_command_entity_open_idx, whose key ends
-- in seq for exactly this lookup. Without seq in the key it would walk the
-- entity's whole open set to find the lowest.
UPDATE
    fsm_command
SET
    blocked = 0
WHERE
    command_id = (
        SELECT
            n.command_id
        FROM
            fsm_command n
        WHERE
            n.machine_id = @machine_id
            AND n.entity_id = @entity_id
            AND n.status IN ('ready', 'leased')
        ORDER BY
            n.seq
        LIMIT 1);
