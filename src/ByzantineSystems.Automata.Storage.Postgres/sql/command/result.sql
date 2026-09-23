-- What happened to a command.
--
-- Three shapes behind one read. The command's status says which: still open,
-- or finished with a transition, or finished with an error. A succeeded
-- command's result is its transition, and the other two carry an error, so both
-- joins are LEFT and at most one of them ever matches.
SELECT
    c.status,
    c.machine_id,
    c.entity_id,
    t.epoch,
    t.chart_version,
    t.event,
    t.actions,
    t.from_state,
    t.to_state,
    t.handled_by,
    t.exited,
    t.entered,
    t.status AS instance_status,
    t.effective_at,
    t.committed_at,
    e.error
FROM
    fsm.command c
    LEFT JOIN fsm.transition t ON t.command_id = c.command_id
    LEFT JOIN fsm.command_error e ON e.command_id = c.command_id
WHERE
    c.command_id = @command_id;

