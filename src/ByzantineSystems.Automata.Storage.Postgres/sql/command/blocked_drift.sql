-- Commands whose durable blocked flag disagrees with their position in the entity queue.
SELECT
    command_id,
    machine_id,
    entity_id,
    seq,
    blocked,
    expected_blocked
FROM
    fsm.blocked_drift
ORDER BY
    machine_id,
    entity_id,
    seq;

