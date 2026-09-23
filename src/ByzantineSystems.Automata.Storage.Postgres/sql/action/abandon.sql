-- Records an effect that never happened and removes it from the queue, fenced.
--
-- The durable record is written before the archive, so a failure between the
-- two leaves the message in the queue rather than leaving the fact unrecorded.
SELECT
    fsm.abandon_action (@queue, @msg_id, @read_ct, @reason) AS outcome;
