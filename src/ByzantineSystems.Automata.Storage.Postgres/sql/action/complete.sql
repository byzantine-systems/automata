-- Archives a delivered action, refusing if this lease is no longer the holder.
--
-- Never pgmq.archive directly: that function ignores read_ct, so it cannot tell
-- the legitimate owner from a worker that stalled past its visibility timeout
-- and woke up after somebody else reclaimed the message.
SELECT
    fsm.complete_action (@queue, @msg_id, @read_ct) AS outcome;

