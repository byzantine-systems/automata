-- Returns a failed delivery to the queue after a backoff. Fenced.
--
-- read_ct counts deliveries, so it is the attempt number: the delay is a
-- uniform draw below min(cap, base << (read_ct - 1)), as in
-- sql/command/reschedule.sql and PostgreSQL's fsm.retry_backoff.
--
-- The token is cleared. A worker that rescheduled and then, by mistake, also
-- completes finds its token gone, and the delivery stays queued. pgmq's set_vt
-- keeps read_ct as it was, so the PostgreSQL store is looser here.
UPDATE
    fsm_action
SET
    lease_token = 0,
    visible_at = @now + ABS(RANDOM() % (
            CASE WHEN @base_us > (@cap_us >> MIN(MAX(read_ct - 1, 0), 30)) THEN
                @cap_us
            ELSE
                @base_us << MIN(MAX(read_ct - 1, 0), 30)
            END))
WHERE
    command_id = @command_id
    AND ordinal = @ordinal
    AND lease_token = @lease_token;
