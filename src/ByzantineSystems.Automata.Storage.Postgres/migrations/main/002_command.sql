-- ---------------------------------------------------------------------------
-- fsm.command: the durable command inbox, and the unit of ordering.
--
-- Process-local actors give strict per-entity ordering inside one process and
-- nothing across processes. Two pods running the same machine each serialise
-- their own mailbox; between them, nothing does. This table is the distributed
-- replacement for that mailbox: at most one non-terminal command per entity is
-- claimable, so claiming it excludes the entity across every process.
--
-- Everything downstream assumes this. The transition log, corrections and the
-- command processor all take the inbox as the thing that decides order.
-- ---------------------------------------------------------------------------
-- A checked text domain, not an enum, and the difference is operational.
-- Migrations run one transaction per script, and a future script that does
-- ALTER TYPE ... ADD VALUE and then *uses* the new value in that same
-- transaction fails with "unsafe use of new value of enum type". A domain has
-- no such rule: adding a status is an ordinary constraint replacement.
--
-- 'ready' and 'leased' are the non-terminal states, in which a command still
-- owns its entity. The other three are terminal and release it, 'dead_letter'
-- included; fsm.ack_command explains why a poisonous command must not wedge an
-- entity forever.
CREATE DOMAIN fsm.command_status AS text CONSTRAINT command_status_valid CHECK (VALUE IN ('ready', 'leased', 'succeeded', 'rejected', 'dead_letter'));

-- The result of every fenced write. A domain rather than a bare text column, so
-- that a typo in a routine is a constraint violation rather than a value the
-- caller silently fails to match.
CREATE DOMAIN fsm.lease_outcome AS text CONSTRAINT lease_outcome_valid CHECK (VALUE IN ('updated', 'lease_lost'));

-- Fencing tokens, monotone by construction. A stale worker's token is not
-- merely different from the current one, it is strictly lower, which makes "did
-- this worker lose its lease" an ordering question rather than only an equality
-- miss. One sequence serves the whole schema: tokens need to be unique, not
-- dense, and gaps carry no meaning.
--
-- It starts at 1, which leaves 0 free as the "not leased" sentinel. No column
-- in this schema is nullable, so absence always has a value: '-infinity' for an
-- unset instant, 'infinity' for an open upper bound, the empty string for an
-- unsupplied text, 0 for no lease. Three-valued logic in a claim predicate is a
-- bug waiting for the one row where somebody writes = instead of
-- IS NOT DISTINCT FROM.
CREATE SEQUENCE fsm.lease_token
    AS bigint START WITH 1;

CREATE TABLE fsm.command (
    -- Identity, and only identity. Sequences are not transactional: values are
    -- allocated outside the transaction and a rollback does not return them, so
    -- command_id has gaps and those gaps mean nothing. The business numbering
    -- that must be gapless is seq, which is why it is computed rather than
    -- allocated.
    command_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    machine_id text NOT NULL,
    entity_id text NOT NULL,
    seq bigint NOT NULL,
    idempotency_key text NOT NULL,
    chart_version integer NOT NULL,
    event jsonb NOT NULL,
    status fsm.command_status NOT NULL DEFAULT 'ready',
    blocked boolean NOT NULL DEFAULT FALSE,
    visible_at timestamptz NOT NULL DEFAULT NOW(),
    lease_token bigint NOT NULL DEFAULT 0,
    read_ct integer NOT NULL DEFAULT 0,
    attempts integer NOT NULL DEFAULT 0,
    received_at timestamptz NOT NULL DEFAULT STATEMENT_TIMESTAMP(),
    -- Audit context. Every column here belongs to the application, never to the
    -- library: it is recorded and returned, and nothing in the claim path reads
    -- it.
    --
    -- The empty string means "not supplied". These are descriptive fields, and an
    -- empty principal is no principal, so there is no distinction between absent
    -- and blank worth carrying three-valued logic for. tenant becomes the
    -- row-level-security discriminator in a later step, and that policy already
    -- reads an unset tenant GUC as '' rather than as NULL, so the two agree.
    tenant text NOT NULL DEFAULT '',
    principal text NOT NULL DEFAULT '',
    source text NOT NULL DEFAULT '',
    correlation_id text NOT NULL DEFAULT '',
    causation_id text NOT NULL DEFAULT '',
    -- A resubmitted idempotency key must find the original command rather than
    -- create a second one.
    --
    -- This constraint and command_unique_seq below are the integrity boundary.
    -- fsm.submit_command takes a per-entity advisory lock so that concurrent
    -- submissions queue instead of colliding, but an advisory lock only binds
    -- writers who take it. These two constraints bind every writer, including one
    -- that bypasses the routine, so a bug there is a failed insert rather than a
    -- duplicated command or a reordered history.
    CONSTRAINT command_unique_idem UNIQUE (machine_id, entity_id, idempotency_key),
    -- Gapless per entity. Also the index that makes "what is the next sequence
    -- number" a single backward index scan rather than a scan of the entity's
    -- accumulated history.
    CONSTRAINT command_unique_seq UNIQUE (machine_id, entity_id, seq),
    CONSTRAINT command_seq_positive CHECK (seq > 0),
    CONSTRAINT command_counters_non_negative CHECK (read_ct >= 0 AND attempts >= 0),
    CONSTRAINT command_lease_token_non_negative CHECK (lease_token >= 0),
    -- A leased or terminal row carries the token that claimed it; a ready row
    -- carries the 0 sentinel. Keeping the token after a terminal write is what
    -- will later let finalize_command answer "was this result written by *my*
    -- lease", rather than only "is there a result".
    --
    -- Every operand is NOT NULL, so this is two-valued and cannot pass by being
    -- unknown. That is why no column here is nullable: a CHECK succeeds when its
    -- expression is TRUE *or* NULL, and a nullable column quietly turns a
    -- constraint into a suggestion.
    CONSTRAINT command_lease_token_matches_status CHECK ((lease_token = 0) = (status = 'ready')),
    -- The non-clamped check. blocked is derived state, and derived state drifts.
    -- This makes drift fail loudly at the moment of the bad write, rather than
    -- surfacing weeks later as an entity that mysteriously stopped progressing.
    -- A blocked command has by definition never been claimed, so it is 'ready'.
    CONSTRAINT command_blocked_is_ready CHECK (NOT blocked OR status = 'ready'),
    -- A command may only pin a chart version some chart declared. Without this,
    -- chart_version is an integer nobody validates, and the first time anyone
    -- finds out that a command references a version that never existed is
    -- during a replay that cannot proceed.
    --
    -- No ON DELETE clause, so NO ACTION: a version that commands still
    -- reference cannot be deleted. There is no routine for forgetting a
    -- registration either, because forgetting one disarms the fingerprint
    -- check. A developer iterating on a chart locally either bumps the version
    -- or runs make db-reset.
    --
    -- No index on the referencing side. PostgreSQL scans the child table when a
    -- parent key is deleted or updated, and neither happens here: versions are
    -- appended and then kept. An index for it would put a fourth write on every
    -- insert into the busiest table in the schema to serve an operation the
    -- constraint forbids.
    CONSTRAINT command_chart_version_fkey FOREIGN KEY (machine_id, chart_version) REFERENCES fsm.machine_chart_version (machine_id, version)
);

COMMENT ON COLUMN fsm.command.seq IS 'Gapless per-entity submission counter, computed under the advisory lock taken by fsm.submit_command and enforced by command_unique_seq. Not an identity column: identity values are allocated outside the transaction and would leave gaps on rollback.';

COMMENT ON COLUMN fsm.command.chart_version IS 'The chart version this command was resolved under, pinned at submission so that replay decides it the same way twice. Held to a version some chart declared by command_chart_version_fkey.';

COMMENT ON COLUMN fsm.command.blocked IS 'TRUE when an earlier non-terminal sibling exists for this entity. Derived, denormalised, and deliberately so: the alternative is an anti-join in the claim path. Measured on a comparable queue at 200k jobs with 50k blocked at the head, a live anti-join in checkout took 237 ms over 253k buffers, an anti-join against a materialised set took 42 ms over 102k buffers, and this column inside the index took 0.14 ms over 9 buffers. It ships with the discipline that buys: a non-clamped CHECK, a drift query that runs by default, and a repair that does not.';

COMMENT ON COLUMN fsm.command.visible_at IS 'Two readings, one column, one index. For a ready row it is the earliest instant the command may be claimed; for a leased row it is the lease deadline. An expired lease therefore becomes claimable again on its own, and the job that reaps them is an optimisation rather than a correctness requirement.';

COMMENT ON COLUMN fsm.command.lease_token IS 'Fences a worker that stalled past its lease: completing, rescheduling or extending requires the token the claim handed out, so a worker whose command was reclaimed cannot corrupt it. 0 means not leased, and fsm.lease_token starts at 1, so the sentinel can never collide with a live token.';

COMMENT ON COLUMN fsm.command.read_ct IS 'Deliveries. Incremented by every claim, including redelivery after a lease expires.';

COMMENT ON COLUMN fsm.command.attempts IS 'Processing attempts. Incremented by fsm.reschedule_command only, so it counts failures the application actually reported rather than deliveries.';

COMMENT ON COLUMN fsm.command.received_at IS 'When the submission reached the database. Distinct from the business time at which an event was effective, and from the instant its transition commits. Without all three you cannot separate "when did the user act" from "when did we process it".';

-- ---------------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------------
-- The claim path, and the reason the blocked column exists. Time lives in the
-- indexed columns and the booleans live in the predicate, because now() is not
-- IMMUTABLE and cannot appear in a partial index predicate.
--
-- command_id trails visible_at so that ORDER BY visible_at, command_id is
-- answered by the index itself: no sort node above the Limit, and ties broken
-- deterministically by submission order rather than by physical row order.
--
-- This predicate is repeated verbatim in fsm.claim_commands and in
-- fsm.command_metrics. If you change one, change all three. A claim whose WHERE
-- clause has drifted from its index still returns correct rows, it just stops
-- using the index, and nothing fails to tell you so.
CREATE INDEX command_claim_idx ON fsm.command (machine_id, visible_at, command_id)
WHERE
    status IN ('ready', 'leased') AND NOT blocked;

-- The per-entity head invariant, enforced rather than merely intended: at most
-- one non-terminal unblocked command per entity. This is the mutual exclusion
-- the whole design rests on, so it is a unique index rather than a convention.
-- Two racing submissions for a fresh entity, or an unblocking bug that elects
-- two heads, fail here instead of producing an entity whose history interleaves.
--
-- A partial unique index is an index and not a SQL constraint, so it can never
-- be a foreign-key target and ON CONFLICT cannot infer it without restating the
-- predicate. Neither matters here: nothing references a command by entity, and
-- every write goes through a routine.
CREATE UNIQUE INDEX command_entity_head_idx ON fsm.command (machine_id, entity_id)
WHERE
    status IN ('ready', 'leased') AND NOT blocked;

-- Answers "does this entity already have an open command" for
-- fsm.submit_command. A superset of the head index, which cannot serve the
-- question because its predicate excludes exactly the blocked rows being looked
-- for. Both stay small: they index open commands only, never the accumulated
-- history.
CREATE INDEX command_entity_open_idx ON fsm.command (machine_id, entity_id)
WHERE
    status IN ('ready', 'leased');

-- ---------------------------------------------------------------------------
-- Storage
-- ---------------------------------------------------------------------------
-- TODO: no autovacuum or fillfactor settings. This table churns and will
-- probably want tuning, but nothing here has been measured yet. Set them with a
-- benchmark in hand, and record what it measured.
-- ---------------------------------------------------------------------------
