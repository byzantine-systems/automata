-- ---------------------------------------------------------------------------
-- $schema$.supervision_event: append-only audit of supervisor decisions.
--
-- Carried over unchanged from the v1 schema. It is orthogonal to the command
-- inbox: supervision records why a worker was restarted or escalated, which is
-- a fact about the host process and not about any entity's state. The hosted
-- services and the supervision example both depend on it, so it outlives the
-- v1 store that was replaced around it.
-- ---------------------------------------------------------------------------
CREATE DOMAIN $schema$.supervision_kind AS text CHECK (VALUE IN ('Started', 'Restarted', 'Escalated', 'Stopped'));

CREATE DOMAIN $schema$.supervision_strategy AS text CHECK (VALUE IN ('OneForOne', 'OneForAll', 'RestForOne'));

CREATE TABLE $schema$.supervision_event (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, supervisor text NOT NULL, child_id text NOT NULL, kind $schema$.supervision_kind NOT NULL, strategy $schema$.supervision_strategy NOT NULL, reason text NOT NULL DEFAULT '', at timestamptz NOT NULL
);

-- Newest first is the only way this table is ever read.
CREATE INDEX supervision_event_recent_idx ON $schema$.supervision_event (at DESC, id DESC);

