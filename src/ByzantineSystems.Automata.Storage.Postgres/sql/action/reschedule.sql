-- Returns a failed delivery to the queue after a jittered backoff, fenced.
--
-- The delay comes from the same routine the command inbox uses, so a queue full
-- of failures spreads out instead of retrying in lockstep.
SELECT
    r.outcome,
    r.visible_at
FROM
    fsm.reschedule_action (@queue, @msg_id, @read_ct, @base_ms, @cap_ms) r;
