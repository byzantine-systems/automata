CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE SCHEMA fsm;

-- ---------------------------------------------------------------------------
-- machine
-- The logical machine name. The chart descriptor lives in
-- fsm.machine_chart_version, versioned by application time.
-- ---------------------------------------------------------------------------
CREATE TABLE fsm.machine (
    id text NOT NULL,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (id)
);

-- ---------------------------------------------------------------------------
-- machine_chart_version
-- One row per chart version, valid over a half-open [lower, upper) period.
-- The current version has an open upper bound of 'infinity' (never NULL).
-- PostgreSQL 18 WITHOUT OVERLAPS makes overlapping validity periods
-- impossible for a machine.
-- ---------------------------------------------------------------------------
CREATE TABLE fsm.machine_chart_version (
    machine_id text NOT NULL,
    version integer NOT NULL,
    chart jsonb NOT NULL,
    valid_during tstzrange NOT NULL,
    PRIMARY KEY (machine_id, valid_during WITHOUT OVERLAPS),
    CONSTRAINT fk_chart_version_machine FOREIGN KEY (machine_id) REFERENCES fsm.machine (id)
);

-- ---------------------------------------------------------------------------
-- instance
-- The materialised state of one entity: current state, gapless epoch, and
-- lifecycle status.
-- ---------------------------------------------------------------------------
CREATE TABLE fsm.instance (
    machine_id text NOT NULL,
    entity_id text NOT NULL,
    epoch bigint NOT NULL,
    state jsonb NOT NULL,
    state_path text[] NOT NULL DEFAULT '{}',
    status text NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (machine_id, entity_id)
);

-- ---------------------------------------------------------------------------
-- transition
-- The append-only history log. Epoch is gapless per entity; the idempotency
-- key is unique per machine/entity.
-- ---------------------------------------------------------------------------
CREATE TABLE fsm.transition (
    machine_id text NOT NULL,
    entity_id text NOT NULL,
    epoch bigint NOT NULL,
    occurred_at timestamptz NOT NULL,
    event jsonb NOT NULL,
    actions jsonb NOT NULL,
    from_state jsonb NOT NULL,
    to_state jsonb NOT NULL,
    status text NOT NULL,
    handled_by text NOT NULL,
    exited text[] NOT NULL DEFAULT '{}',
    entered text[] NOT NULL DEFAULT '{}',
    idempotency_key text NOT NULL,
    PRIMARY KEY (machine_id, entity_id, epoch),
    CONSTRAINT uq_transition_idem UNIQUE (machine_id, entity_id, idempotency_key),
    CONSTRAINT fk_transition_instance FOREIGN KEY (machine_id, entity_id) REFERENCES fsm.instance (machine_id, entity_id)
);

-- ---------------------------------------------------------------------------
-- outbox
-- One row per pending side effect, keyed by the full OutboxKey scope
-- (machine, entity, event idempotency key, action ordinal). locked_until
-- defaults to '-infinity' meaning "not leased".
-- ---------------------------------------------------------------------------
CREATE TABLE fsm.outbox (
    machine_id text NOT NULL,
    entity_id text NOT NULL,
    event_idempotency_key text NOT NULL,
    ordinal integer NOT NULL,
    action jsonb NOT NULL,
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL,
    locked_until timestamptz NOT NULL DEFAULT '-infinity',
    PRIMARY KEY (machine_id, entity_id, event_idempotency_key, ordinal)
);

-- ---------------------------------------------------------------------------
-- retry_queue
-- Durable long-horizon retry. locked_until = '-infinity' means "not leased";
-- last_error = '' means "no error recorded".
-- ---------------------------------------------------------------------------
CREATE TABLE fsm.retry_queue (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    machine_id text NOT NULL,
    entity_id text NOT NULL,
    idempotency_key text NOT NULL,
    event jsonb NOT NULL,
    attempts integer NOT NULL DEFAULT 0,
    next_retry_at timestamptz NOT NULL,
    locked_until timestamptz NOT NULL DEFAULT '-infinity',
    last_error text NOT NULL DEFAULT '',
    CONSTRAINT uq_retry_key UNIQUE (machine_id, entity_id, idempotency_key)
);

-- ---------------------------------------------------------------------------
-- dead_letter
-- Append-only record of events the machine permanently abandoned.
-- ---------------------------------------------------------------------------
CREATE TABLE fsm.dead_letter (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    machine_id text NOT NULL,
    entity_id text NOT NULL,
    idempotency_key text NOT NULL,
    event jsonb NOT NULL,
    final_error text NOT NULL,
    attempts integer NOT NULL,
    died_at timestamptz NOT NULL
);

-- ---------------------------------------------------------------------------
-- supervision_event
-- Auditable supervision facts (M5 writes these; the schema exists now).
-- ---------------------------------------------------------------------------
CREATE TABLE fsm.supervision_event (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    supervisor text NOT NULL,
    child_id text NOT NULL,
    kind text NOT NULL,
    strategy text NOT NULL,
    reason jsonb NOT NULL DEFAULT '{}',
    at timestamptz NOT NULL
);

