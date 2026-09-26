-- What finalizing a command decides from: whose it is, where it stands and who
-- holds it. Read inside the finalize's BEGIN IMMEDIATE transaction, which holds
-- the write lock, so nothing can change these values before the writes that
-- depend on them. PostgreSQL needs FOR UPDATE for the same guarantee.
SELECT
    c.machine_id,
    c.entity_id,
    c.status,
    c.lease_token,
    c.chart_version
FROM
    fsm_command c
WHERE
    c.command_id = @command_id;
