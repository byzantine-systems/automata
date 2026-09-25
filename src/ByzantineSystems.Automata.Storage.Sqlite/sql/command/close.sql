-- Moves a command to its terminal status. The lease token stays: a terminal
-- row keeps the token of the lease that finished it, which is how a retried
-- finalize from that lease is told apart from anyone else's. After this, the
-- fsm_command_terminal_is_final trigger refuses every update to the row.
UPDATE
    fsm_command
SET
    status = @status
WHERE
    command_id = @command_id;
