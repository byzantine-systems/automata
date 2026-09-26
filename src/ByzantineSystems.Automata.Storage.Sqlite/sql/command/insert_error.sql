-- Why a command was rejected or dead-lettered, as the tagged JSON envelope the
-- processor writes.
INSERT INTO fsm_command_error (command_id, error, recorded_at)
    VALUES (@command_id, @error, @now);
