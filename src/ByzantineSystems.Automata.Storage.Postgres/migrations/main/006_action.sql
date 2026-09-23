-- ---------------------------------------------------------------------------
-- fsm.action_dead_letter: the effects that never happened.
--
-- A pgmq archive is operational forensics. It records that a message left the
-- queue, is truncatable at will, and says nothing about why. This table is the
-- business fact underneath: the machine decided an action should happen, the
-- application could not make it happen, and after its attempts we stopped
-- trying. That is something an operator has to be able to find later, alongside
-- the transition that asked for it.
--
-- Identity is (command_id, ordinal) rather than the queue's message id. A
-- redelivery carries a new msg_id and the same pair, so the pair is what a
-- destination deduplicates on and what this row has to be searchable by.
--
-- No column is nullable, as everywhere else in this schema: a CHECK passes when
-- its expression is null, so a nullable column quietly turns every constraint
-- mentioning it into a suggestion.
-- ---------------------------------------------------------------------------
CREATE TABLE fsm.action_dead_letter (
    machine_id text NOT NULL,
    entity_id text NOT NULL,
    command_id bigint NOT NULL,
    epoch bigint NOT NULL,
    ordinal integer NOT NULL,
    action jsonb NOT NULL,
    -- Written by the library, never by the application: an application's own
    -- error belongs to its handler, and what this records is that delivery was
    -- given up on.
    reason text NOT NULL,
    deliveries integer NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT STATEMENT_TIMESTAMP(),
    CONSTRAINT action_dead_letter_pkey PRIMARY KEY (command_id, ordinal),
    CONSTRAINT action_dead_letter_ordinal_not_negative CHECK (ordinal >= 0),
    CONSTRAINT action_dead_letter_deliveries_positive CHECK (deliveries > 0),
    CONSTRAINT action_dead_letter_reason_not_blank CHECK (LENGTH(BTRIM(reason)) > 0),
    CONSTRAINT action_dead_letter_command_fkey FOREIGN KEY (command_id) REFERENCES fsm.command (command_id)
);

-- Answering "what has this machine failed to deliver lately", which is the only
-- question this table is asked and the one a dead-letter rate alert runs.
CREATE INDEX action_dead_letter_machine_idx ON fsm.action_dead_letter (machine_id, recorded_at DESC);

-- TODO: no autovacuum settings, as in 002_command.sql. This table should stay
-- small, because a healthy deployment writes to it rarely. A benchmark would
-- need to show the opposite before tuning it.
