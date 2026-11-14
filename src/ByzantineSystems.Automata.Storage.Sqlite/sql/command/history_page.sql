-- One page of an entity's committed transitions, after a cursor. The cursor is an epoch, and an
-- absent cursor is zero, which transition_epoch_positive makes smaller than any stored epoch, so
-- one statement shape serves every page.
SELECT
    t.machine_id, t.entity_id, t.epoch, t.command_id, t.chart_version, c.event,
    t.actions, t.from_state, t.to_state, t.handled_by, t.exited, t.entered,
    t.status, t.effective_at, t.committed_at
FROM
    fsm_transition t JOIN fsm_command c ON c.command_id = t.command_id
WHERE
    t.machine_id = @machine_id
    AND t.entity_id = @entity_id
    AND t.epoch > @after
ORDER BY
    t.epoch
LIMIT @limit;
