-- What became of one command: its status, its transition if it committed, and its recorded
-- error if it failed. The command's own status column is aliased, because the transition table
-- has a column of the same name and the reader maps by name. A NULL transition command_id or
-- error column means that half is absent.
SELECT
    c.status AS command_status,
    t.machine_id,
    t.entity_id,
    t.epoch,
    t.command_id,
    t.chart_version,
    t.event,
    t.actions,
    t.from_state,
    t.to_state,
    t.handled_by,
    t.exited,
    t.entered,
    t.status,
    t.effective_at,
    t.committed_at,
    e.error
FROM
    fsm_command c
    LEFT JOIN fsm_transition t ON t.command_id = c.command_id
    LEFT JOIN fsm_command_error e ON e.command_id = c.command_id
WHERE
    c.command_id = @command_id;
