-- ---------------------------------------------------------------------------
-- fsm.transition: the append-only record of what each command decided.
--
-- One row per finalized command that produced a state change. The belief table
-- says what an entity's state is; this says how it got there, which command was
-- responsible, and under which chart version the decision was made. Replay and
-- audit both read this, and neither can work from the belief alone.
-- ---------------------------------------------------------------------------
-- The answer fsm.finalize_command gives. A checked text domain, like the others.
CREATE DOMAIN fsm.finalize_outcome AS text CONSTRAINT finalize_outcome_valid CHECK (VALUE IN ('finalized', 'already_finalized', 'conflict', 'lease_lost'));

CREATE TABLE fsm.transition (
    machine_id text NOT NULL,
    entity_id text NOT NULL,
    epoch bigint NOT NULL,
    command_id bigint NOT NULL,
    chart_version integer NOT NULL,
    event jsonb NOT NULL,
    actions jsonb NOT NULL,
    from_state jsonb NOT NULL,
    to_state jsonb NOT NULL,
    -- Which node in the chain handled the event, and the exit and entry paths
    -- around the least common ancestor. Recorded because resolution is a
    -- bubbling search: knowing the answer without knowing who gave it makes an
    -- unexpected transition impossible to explain after the fact.
    handled_by text NOT NULL,
    exited text[] NOT NULL,
    entered text[] NOT NULL,
    status fsm.instance_status NOT NULL,
    -- Business time, from the caller. See committed_at for the other one.
    effective_at timestamptz NOT NULL,
    -- Database time, never caller-supplied. The application's clock decides
    -- when an event was effective; only this database decides the order history
    -- is written in, so a skewed or malicious client clock cannot reorder it.
    committed_at timestamptz NOT NULL DEFAULT STATEMENT_TIMESTAMP(),
    -- Gapless per entity, which the key enforces rather than merely records.
    --
    -- Nothing serialises finalizers explicitly and nothing needs to: at most one
    -- command per entity is claimable, so at most one worker can be finalizing
    -- an entity at a time. This key is the backstop for that reasoning, not the
    -- mechanism behind it.
    PRIMARY KEY (machine_id, entity_id, epoch),
    CONSTRAINT transition_epoch_positive CHECK (epoch > 0),
    -- Idempotence as a storage guarantee rather than a property of a routine.
    -- Whatever fsm.finalize_command does, and whatever a caller retries after a
    -- dropped connection, one command can never produce two transitions.
    CONSTRAINT transition_unique_command UNIQUE (command_id),
    CONSTRAINT transition_command_fkey FOREIGN KEY (command_id) REFERENCES fsm.command (command_id)
);

COMMENT ON COLUMN fsm.transition.effective_at IS 'When the event was effective in the world. Caller-supplied, because a back-dated correction is a legitimate thing to record.';

COMMENT ON COLUMN fsm.transition.committed_at IS 'When this database wrote the transition. Never caller-supplied: one clock decides durable order.';

-- The audit read: one entity's history, oldest first.
CREATE INDEX transition_history_idx ON fsm.transition (machine_id, entity_id, epoch);

-- ---------------------------------------------------------------------------
-- Storage
-- ---------------------------------------------------------------------------
-- TODO: no autovacuum settings, as in 002_command.sql. This table is
-- append-only and grows without bound, so it likely wants a different ANALYZE
-- cadence from the tables that churn, but nothing here has been measured.
-- ---------------------------------------------------------------------------
