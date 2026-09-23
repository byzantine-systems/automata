-- Creates this machine's action queue if nothing has created it yet.
--
-- Called once at startup rather than on every send. pgmq.create builds tables
-- and indexes, which is not something a hot path should be asking about.
SELECT
    fsm.ensure_action_queue (@queue) AS created;
