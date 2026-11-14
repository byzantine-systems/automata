-- The events that took effect at or after a corrected instant, earliest first, for a correction
-- to replay. Ordering by effective_at then epoch is deterministic: two transitions of one entity
-- cannot share an epoch, which is the log's key.
SELECT
    t.machine_id, t.entity_id, t.epoch, t.command_id, t.chart_version, c.event,
    t.actions, t.from_state, t.to_state, t.handled_by, t.exited, t.entered,
    t.status, t.effective_at, t.committed_at
FROM
    $schema$.transition t JOIN $schema$.command c ON c.command_id = t.command_id
WHERE
    t.machine_id = @machine_id
    AND t.entity_id = @entity_id
    AND t.effective_at >= @from
ORDER BY
    t.effective_at,
    t.epoch
LIMIT @limit;
