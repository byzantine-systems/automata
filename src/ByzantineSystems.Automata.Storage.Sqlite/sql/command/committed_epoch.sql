-- The epoch a command's commit wrote, answering a retried finalize with the
-- work it already did. No row for a command that failed rather than committed.
SELECT
    t.epoch
FROM
    fsm_transition t
WHERE
    t.command_id = @command_id;
