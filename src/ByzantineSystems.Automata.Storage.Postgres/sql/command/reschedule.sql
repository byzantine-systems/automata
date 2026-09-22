-- Returns a failed command to the queue for a later attempt.
--
-- The delay is computed in the database from the attempt count, so the caller
-- supplies only the backoff envelope. visible_at comes back as '-infinity' when
-- the fence rejected the caller, which pairs with outcome = 'lease_lost'.
SELECT
    r.outcome,
    r.visible_at
FROM
    fsm.reschedule_command (@command_id, @lease_token, @base_ms, @cap_ms) r;

