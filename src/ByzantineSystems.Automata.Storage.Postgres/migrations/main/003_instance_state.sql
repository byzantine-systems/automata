-- ---------------------------------------------------------------------------
-- fsm.instance_state: what an entity's state was, and when we believed it.
--
-- Two independent time axes, kept apart:
--
--   * valid_during is business time. When the state was true in the world.
--   * system_time is belief time. When this database held that opinion.
--
-- An operator asking "it looked settled at 14:02 and shows refunded now" is
-- asking about both at once. Valid time alone cannot answer it, because a
-- back-dated correction changes what was true without changing what we thought
-- at the time, and a table that records only the current answer cannot tell the
-- two apart. Superseded beliefs move to fsm.instance_state_history rather than
-- being overwritten, so the earlier opinion survives its own correction.
--
-- system_time is maintained entirely by the fsm.temporal_versioning trigger.
-- Nothing else should write it.
-- ---------------------------------------------------------------------------
-- What a valid-time mutation did. A checked text domain, for the reason
-- fsm.command_status is one: adding an outcome must not be an ALTER TYPE whose
-- new value cannot be used in the transaction that adds it.
CREATE DOMAIN fsm.belief_outcome AS text CONSTRAINT belief_outcome_valid CHECK (VALUE IN ('opened', 'split', 'replaced'));

-- An entity's lifecycle, as of a belief. Declared here rather than beside
-- fsm.transition because this is the first table to use it.
CREATE DOMAIN fsm.instance_status AS text CONSTRAINT instance_status_valid CHECK (VALUE IN ('running', 'suspended', 'terminated'));

CREATE TABLE fsm.instance_state (
    machine_id text NOT NULL,
    entity_id text NOT NULL,
    state jsonb NOT NULL,
    status fsm.instance_status NOT NULL,
    -- Which transition produced this belief, and under which chart.
    --
    -- epoch is denormalised from fsm.transition. Unlike blocked in
    -- fsm.command it has no independent writer to drift against: the belief and
    -- its transition are written by the same two statements of the same routine.
    -- Carrying it here is what makes reading an entity's current state a single
    -- lookup on instance_state_live_idx rather than a join against a log that
    -- grows forever.
    --
    -- No foreign key to fsm.transition. A correction writes a belief that no
    -- single transition produced, and that arrives in a later step.
    epoch bigint NOT NULL,
    command_id bigint NOT NULL,
    chart_version integer NOT NULL,
    -- The two defaults deliberately use different clocks.
    --
    -- Belief time has to advance *within* a transaction, because two beliefs
    -- really did happen in sequence, so system_time uses statement_timestamp().
    -- Business time does not: every belief written by one transaction is
    -- effective at one business instant, so valid_during uses now(), which is
    -- frozen transaction-wide. The trigger's own header argues the first half
    -- at length.
    --
    -- The valid_during default is a convenience for fixtures and for a caller
    -- who genuinely means "from now". Real writers supply it, because business
    -- time is caller-supplied by design and a back-dated correction is exactly
    -- the case that matters.
    valid_during tstzrange NOT NULL DEFAULT TSTZRANGE(NOW(), 'infinity', '[)'),
    system_time tstzrange NOT NULL DEFAULT TSTZRANGE(STATEMENT_TIMESTAMP(), 'infinity', '[)'),
    -- The temporal key. An entity is unique at any instant, but the same entity
    -- owns as many non-overlapping historical valid-time beliefs as its life
    -- produced. This is the invariant corrections rest on: a split may not
    -- leave two versions claiming the same moment.
    PRIMARY KEY (machine_id, entity_id, valid_during WITHOUT OVERLAPS),
    -- Half-open on both axes: two spellings of one interval would make every
    -- as-of test ambiguous at its own boundary, which is where corrections land.
    --
    -- These also reject empty ranges, since 'empty' has lower_inc() = false. A
    -- zero-width belief is a row no containment test returns, so it must not be
    -- storable; a separate NOT isempty() constraint would never be the one to
    -- fire. The temporal key rejects an empty valid_during too, but CHECKs run
    -- first.
    --
    -- They matter most on fsm.instance_state_history, which has neither the
    -- temporal key nor the trigger. On this table the trigger overwrites
    -- system_time on every write, so a caller cannot reach the constraint with a
    -- bad value; on the twin, the constraint is all there is.
    CONSTRAINT instance_state_valid_half_open CHECK (LOWER_INC(valid_during) AND NOT UPPER_INC(valid_during)),
    CONSTRAINT instance_state_system_half_open CHECK (LOWER_INC(system_time) AND NOT UPPER_INC(system_time))
);

COMMENT ON COLUMN fsm.instance_state.valid_during IS 'Business time: when this state was true in the world. Half-open, and open-ended for the current belief. Supplied by the caller, because a back-dated correction is a legitimate thing to write and the system clock cannot know about it.';

COMMENT ON COLUMN fsm.instance_state.system_time IS 'Belief time: when this database held this opinion. Maintained only by fsm.temporal_versioning, stamped with statement_timestamp() so that one statement is one change of belief. The upper bound is the literal infinity, never NULL.';

-- The fetch path. "What is this entity's state now" is the question the runtime
-- asks most, and upper(valid_during) = 'infinity' is how a row says it is the
-- current belief.
--
-- The temporal key can answer it too, by scanning the entity's versions and
-- filtering, so this index earns its place only once valid-time history
-- accumulates: it stays proportional to the live frontier while the key grows
-- with every version. Measured at 2000 entities holding ten superseded versions
-- each, the read takes 3 buffers through this index against 33 through the key.
--
-- upper() on a range is IMMUTABLE and the comparison is against a literal, so
-- this is a legal partial-index predicate. now() would not be.
CREATE UNIQUE INDEX instance_state_live_idx ON fsm.instance_state (machine_id, entity_id)
WHERE
    UPPER(valid_during) = 'infinity';

-- ---------------------------------------------------------------------------
-- The history twin.
--
-- Named by convention: fsm.temporal_versioning finds it as <table>_history, so
-- one trigger function serves every temporal table this schema grows.
--
-- LIKE ... INCLUDING DEFAULTS INCLUDING CONSTRAINTS copies the columns, their
-- defaults, NOT NULL and the CHECK constraints. The two omissions are the
-- interesting part:
--
--   * NOT INCLUDING INDEXES, so the temporal primary key does not follow.
--     History exists precisely to hold superseded, overlapping beliefs; a key
--     forbidding overlap would reject the very rows this table is for.
--   * NOT INCLUDING GENERATED, because the trigger writes rows verbatim through
--     INSERT ... SELECT ($1).* and a generated column cannot be written that
--     way. There is no generated column here today, and stating the reason is
--     what stops one being added without noticing.
-- ---------------------------------------------------------------------------
CREATE TABLE fsm.instance_state_history (
    LIKE fsm.instance_state INCLUDING DEFAULTS INCLUDING CONSTRAINTS
);

-- Time travel: what did we believe about this entity as of some instant. A GiST
-- index because the system_time predicate is a containment test, and the two
-- text columns ride along through btree_gist.
CREATE INDEX instance_state_history_as_of_idx ON fsm.instance_state_history USING gist (machine_id, entity_id, system_time);

-- ---------------------------------------------------------------------------
-- Storage
-- ---------------------------------------------------------------------------
-- TODO: no autovacuum settings, as in 002_command.sql. The live table churns
-- and the history twin is append-only, so they will likely want different
-- values, but nothing here has been measured.
-- ---------------------------------------------------------------------------
