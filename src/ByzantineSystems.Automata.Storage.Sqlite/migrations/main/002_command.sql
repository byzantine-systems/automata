-- ---------------------------------------------------------------------------
-- fsm_command: the durable command inbox, and the unit of ordering.
--
-- At most one non-terminal command per entity is claimable, so claiming it
-- excludes the entity across every process that opens this file. That is the
-- same invariant as fsm.command, enforced the same way: a partial unique index.
--
-- What changes is how writers meet. PostgreSQL serialises submissions per
-- entity with an advisory lock; SQLite serialises every writer in the file, and
-- every write here begins IMMEDIATE, so a statement that reads and then writes
-- inside one transaction sees nothing move in between. seq, epoch and blocked
-- are computed by reading, and the constraints below are the backstop for that
-- reasoning rather than the mechanism behind it.
-- ---------------------------------------------------------------------------
-- Fencing tokens, monotone by construction, standing in for the fsm.lease_token
-- sequence. SQLite has no sequence to call once per row, so a claim advances
-- this counter by the size of its batch in the same transaction and numbers its
-- rows from the old value upward. A stale worker's token is strictly lower than
-- the current one, as it is in the other store.
--
-- One row per counter, seeded here. 0 is the "not leased" sentinel, so the first
-- token handed out is 1.
CREATE TABLE fsm_counter (
    name TEXT NOT NULL PRIMARY KEY,
    value INTEGER NOT NULL,
    CONSTRAINT counter_non_negative CHECK (value >= 0)
) STRICT;

INSERT INTO fsm_counter (name, value)
    VALUES ('lease_token', 0);

CREATE TABLE fsm_command (
    -- AUTOINCREMENT, not a bare rowid alias. Without it SQLite may hand the
    -- largest id out again once that row is deleted, and a command id that
    -- names two different commands over time breaks every reference an
    -- application kept to the first one. Retention deletes rows, so this is
    -- not hypothetical.
    command_id INTEGER PRIMARY KEY AUTOINCREMENT,
    machine_id TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    idempotency_key TEXT NOT NULL,
    chart_version INTEGER NOT NULL,
    event TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'event',
    status TEXT NOT NULL DEFAULT 'ready',
    blocked INTEGER NOT NULL DEFAULT 0,
    -- For a ready row, the earliest instant it may be claimed; for a leased
    -- row, the lease deadline. An expired lease is claimable again on its own.
    visible_at INTEGER NOT NULL,
    lease_token INTEGER NOT NULL DEFAULT 0,
    read_ct INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    received_at INTEGER NOT NULL,
    -- Audit context. The application's, never read by the claim path. Empty
    -- means not supplied.
    tenant TEXT NOT NULL DEFAULT '',
    principal TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL DEFAULT '',
    correlation_id TEXT NOT NULL DEFAULT '',
    causation_id TEXT NOT NULL DEFAULT '',
    CONSTRAINT command_unique_idem UNIQUE (machine_id, entity_id, idempotency_key),
    -- Gapless per entity, and the index that answers "what is the next
    -- sequence number" with one backward seek.
    CONSTRAINT command_unique_seq UNIQUE (machine_id, entity_id, seq),
    CONSTRAINT command_seq_positive CHECK (seq > 0),
    -- The checked vocabularies that PostgreSQL spells as domains.
    CONSTRAINT command_status_valid CHECK (status IN ('ready', 'leased', 'succeeded', 'rejected', 'dead_letter')),
    CONSTRAINT command_kind_valid CHECK (kind IN ('event', 'correction')),
    CONSTRAINT command_blocked_is_flag CHECK (blocked IN (0, 1)),
    CONSTRAINT command_event_is_json CHECK (JSON_VALID(event)),
    CONSTRAINT command_counters_non_negative CHECK (read_ct >= 0 AND attempts >= 0),
    CONSTRAINT command_lease_token_non_negative CHECK (lease_token >= 0),
    -- A ready row carries the 0 sentinel; a leased or terminal row carries the
    -- token that claimed it, which is what lets a repeated finalize answer
    -- "was this written by my lease".
    CONSTRAINT command_lease_token_matches_status CHECK ((lease_token = 0) = (status = 'ready')),
    -- blocked is derived state, and derived state drifts. A blocked command has
    -- never been claimed, so it is ready; anything else fails at the bad write.
    CONSTRAINT command_blocked_is_ready CHECK (blocked = 0 OR status = 'ready'),
    -- A command may only pin a version some chart declared. SQLite reports a
    -- violation here without naming it, so the inbox checks the version before
    -- inserting and this constraint only ever catches a writer that did not.
    CONSTRAINT command_chart_version_fkey FOREIGN KEY (machine_id, chart_version) REFERENCES fsm_machine_chart_version (machine_id, version)
) STRICT;

-- ---------------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------------
-- SQLite uses a partial index only when the query's WHERE clause contains the
-- index's predicate terms as written, so the claim statement repeats this
-- predicate verbatim, blocked = 0 included. Written as NOT blocked it would
-- still return the right rows and silently scan the table instead.
--
-- command_id trails visible_at so ORDER BY visible_at, command_id is answered
-- by the index, with ties broken by submission order.
CREATE INDEX fsm_command_claim_idx ON fsm_command (machine_id, visible_at, command_id)
WHERE
    status IN ('ready', 'leased') AND blocked = 0;

-- The per-entity head invariant, enforced rather than intended: at most one
-- non-terminal unblocked command per entity. This is the mutual exclusion the
-- design rests on, so a bug that elects two heads fails here instead of
-- producing an entity whose history interleaves.
CREATE UNIQUE INDEX fsm_command_entity_head_idx ON fsm_command (machine_id, entity_id)
WHERE
    status IN ('ready', 'leased') AND blocked = 0;

-- An entity's open commands in submission order. It answers two questions:
-- "does this entity already have an open command", for submission, and "which
-- open command is next", when a finalize promotes the next head. A superset of
-- the head index, which cannot answer either because it excludes exactly the
-- blocked rows being looked for.
--
-- seq is in the key for the second question. Without it the planner answers
-- ORDER BY seq LIMIT 1 by walking command_unique_seq from the entity's first
-- command, filtering out every terminal one on the way, so promotion slows
-- with the length of the entity's history. With it, the first entry is the
-- answer.
CREATE INDEX fsm_command_entity_open_idx ON fsm_command (machine_id, entity_id, seq)
WHERE
    status IN ('ready', 'leased');

-- ---------------------------------------------------------------------------
-- Guards
--
-- In the PostgreSQL store every mutation of fsm.command goes through an fsm.*
-- routine, and the routines are where the lifecycle is kept. SQLite has no
-- routines: the statements live in the application's embedded files, and any
-- connection to the file can write the table. These triggers put the two rules
-- a CHECK cannot state, because a CHECK sees only the new row, back beside the
-- data. A write that breaks one aborts its statement, and the transaction
-- around it rolls back.
-- ---------------------------------------------------------------------------
-- A terminal command is final. Finalize answers a repeat from the lease that
-- finished it by reading the status and token it left behind, and a terminal
-- row that could be rewritten would make that answer a guess. Deleting one is
-- still allowed, because retention does exactly that.
CREATE TRIGGER fsm_command_terminal_is_final
    BEFORE UPDATE ON fsm_command
    WHEN OLD.status IN ('succeeded', 'rejected', 'dead_letter')
BEGIN
    SELECT
        RAISE(ABORT, 'fsm_command: a terminal command is never updated');
END;

-- What was submitted is never rewritten: its identity, its place in the
-- entity's order, the event and the chart it pins, and the audit context.
-- UPDATE OF fires whenever a statement names one of these columns, whatever
-- the value, so no store statement names them at all.
CREATE TRIGGER fsm_command_submission_is_immutable
    BEFORE UPDATE OF command_id, machine_id, entity_id, seq, idempotency_key, chart_version, event, kind, received_at, tenant, principal, source, correlation_id, causation_id ON fsm_command
BEGIN
    SELECT
        RAISE(ABORT, 'fsm_command: a submitted command is never rewritten');
END;

-- ---------------------------------------------------------------------------
-- fsm_command_error: why a command was rejected or dead-lettered.
--
-- Separate from fsm_command for the reasons fsm.command_error gives: the claim
-- path never reads it, most commands never have one, and a succeeded command's
-- result lives in fsm_transition. The error is the tagged-JSON failure envelope
-- the PostgreSQL store writes, so the two stores read each other's shape.
-- ---------------------------------------------------------------------------
CREATE TABLE fsm_command_error (
    command_id INTEGER NOT NULL PRIMARY KEY,
    error TEXT NOT NULL,
    recorded_at INTEGER NOT NULL,
    CONSTRAINT command_error_is_json CHECK (JSON_VALID(error)),
    CONSTRAINT command_error_command_fkey FOREIGN KEY (command_id) REFERENCES fsm_command (command_id)
) STRICT;

-- ---------------------------------------------------------------------------
-- fsm_command_correction: what a correction command asks for.
--
-- This store cannot replay corrections, and the runtime dead-letters one when
-- its store offers no replay. It can still be submitted, though, because
-- submitting is on the inbox every store implements, and the inbox has to hand
-- it back intact for the runtime to recognise it. So the details are stored,
-- exactly as the other store stores them, and nothing here acts on them.
-- ---------------------------------------------------------------------------
CREATE TABLE fsm_command_correction (
    command_id INTEGER NOT NULL PRIMARY KEY,
    effective_at INTEGER NOT NULL,
    on_divergence TEXT NOT NULL,
    replay_limit INTEGER NOT NULL,
    CONSTRAINT command_correction_divergence_valid CHECK (on_divergence IN ('fail', 'truncate')),
    CONSTRAINT command_correction_replay_limit_positive CHECK (replay_limit > 0),
    CONSTRAINT command_correction_command_fkey FOREIGN KEY (command_id) REFERENCES fsm_command (command_id)
) STRICT;
