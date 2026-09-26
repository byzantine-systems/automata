-- Keeps a long-running command claimed. Fenced by the token alone.
--
-- Deliberately does not check whether the lease has expired. If nobody has
-- reclaimed the command, extending is the right outcome; if somebody has, the
-- token differs and nothing changes.
UPDATE
    fsm_command
SET
    visible_at = @deadline
WHERE
    command_id = @command_id
    AND lease_token = @lease_token
    AND status = 'leased';
