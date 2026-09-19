-- ---------------------------------------------------------------------------
-- Dequeue and lookup indexes.
--
-- The claim predicate is `next_retry_at <= now AND locked_until < now`, which
-- spans both never-leased ('-infinity') and lease-expired rows, so a partial
-- index on `locked_until = '-infinity'` cannot serve it. A plain timestamp
-- index is the correct, always-used choice for the dequeue path.
-- ---------------------------------------------------------------------------
CREATE INDEX ix_instance_status ON fsm.instance (machine_id, status);

CREATE INDEX ix_instance_state_path ON fsm.instance USING gin (state_path);

CREATE INDEX ix_retry_queue_dequeue ON fsm.retry_queue (next_retry_at, id);

CREATE INDEX ix_outbox_dequeue ON fsm.outbox (next_attempt_at, ordinal);

CREATE INDEX ix_dead_letter_machine ON fsm.dead_letter (machine_id, died_at DESC);

