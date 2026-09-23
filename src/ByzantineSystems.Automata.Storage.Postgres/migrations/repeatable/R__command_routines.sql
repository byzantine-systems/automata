-- ---------------------------------------------------------------------------
-- Command inbox routines.
--
-- Every mutation of fsm.command goes through one of these. The application
-- never writes the table directly, which is what lets the blocked invariant,
-- the gapless sequence and the lease fence be reasoned about in one place.
--
-- This script is repeatable: DbUp reapplies it whenever its content changes, so
-- editing a routine body is an edit to this file rather than a new migration,
-- and every routine is therefore CREATE OR REPLACE.
--
-- The line between the two languages is drawn in one place: plpgsql only where
-- the operation genuinely needs more than one statement, LANGUAGE sql
-- everywhere else.
--
--   * fsm.submit_command is plpgsql because it needs ordered statements.
--     Sibling data-modifying parts of a single SQL statement share one
--     snapshot, run in no defined order, and cannot see each other's effects,
--     and it depends on seeing the previous statement's effect.
--   * Everything else is one query, so it is LANGUAGE sql: plannable, with no
--     procedural call overhead on paths that run once per command. It also
--     means the claim's body can be read back out of the catalog and EXPLAINed
--     verbatim, which is how its plan is asserted by a test.
--
-- All of these are SECURITY INVOKER, the default. They hold no privilege of
-- their own, so the default EXECUTE grant to PUBLIC confers nothing: a caller
-- still needs its own rights on fsm.command, and row-level security, when it
-- arrives, still applies. Routines are the mutation surface for the sake of
-- invariants, not as a privilege boundary.
--
-- Every routine declares its volatility, its parallel safety and its
-- search_path. The first two are promises the planner acts on and must not be
-- guessed. The third matters because a routine runs under whatever search_path
-- the connecting role happens to have; pinning it means an unqualified operator
-- or function cannot be shadowed by an object in a schema earlier on that path.
--
-- No routine returns void. Npgsql has no codec claiming void's typsend, so the
-- decoder receives an unknown OID carrying an empty binary payload; every
-- routine here returns a value instead. No routine is overloaded either, since
-- overload resolution happens at prepare time and an unannotated parameter can
-- select the wrong candidate.
-- ---------------------------------------------------------------------------
-- ---------------------------------------------------------------------------
-- fsm.retry_backoff: capped exponential backoff with full jitter.
--
-- Computed by bit-shift rather than pow(), so the ceiling has no float rounding
-- near it, with the exponent clamped at 30 to stay well inside bigint.
--
-- Jitter is not optional. Without it, every command that fails in the same
-- second is rescheduled to the same instant, and past the knee an unjittered
-- exponential pinned at the cap keeps the whole batch aligned forever, so the
-- retries keep arriving as a thundering herd for as long as the backlog lasts.
--
-- VOLATILE PARALLEL RESTRICTED because random() is. Marking it otherwise would
-- let the planner fold a single draw across rows and undo the jitter.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fsm.retry_backoff (p_attempt integer, p_base_ms integer, p_cap_ms integer)
    RETURNS interval
    LANGUAGE sql
    STRICT VOLATILE PARALLEL RESTRICTED
    SET search_path = pg_catalog, fsm
    AS $$
    SELECT
        MAKE_INTERVAL(secs => (RANDOM() * least (greatest ($3, 1)::bigint, greatest ($2, 1)::bigint << least (greatest ($1, 1) - 1, 30))) / 1000.0);
$$;

-- ---------------------------------------------------------------------------
-- fsm.submit_command: the durable front door.
--
-- Returns the command and whether it was newly accepted. A repeated
-- idempotency key returns the original command id with accepted = false, so a
-- client retry after a lost response is recognised rather than duplicated.
--
-- One advisory transaction lock per entity serialises submissions, which is
-- what lets two things be computed by reading:
--
--   * seq, gapless per entity, from max(seq) + 1.
--   * blocked, from whether an open sibling exists. Without the lock, two
--     concurrent submissions for a fresh entity would both see no open sibling,
--     both insert unblocked, and collide on command_entity_head_idx.
--
-- The lock is not the integrity boundary and must not be mistaken for one. An
-- advisory lock binds only the writers that take it, so correctness rests on
-- command_unique_idem, command_unique_seq and command_entity_head_idx, which
-- bind every writer. The lock's job is to turn contention into queueing instead
-- of into unique violations and retries.
--
-- Its honest cost: when a submission is later enlisted in a host application's
-- transaction, the lock is held for that transaction's life. Contention is per
-- entity and never global, and a hash collision between two unrelated entities
-- costs serialisation rather than correctness.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fsm.submit_command (p_machine_id text, p_entity_id text, p_idempotency_key text, p_chart_version integer, p_event jsonb, p_visible_at timestamptz DEFAULT NULL, p_received_at timestamptz DEFAULT NULL, p_tenant text DEFAULT '', p_principal text DEFAULT '', p_source text DEFAULT '', p_correlation_id text DEFAULT '', p_causation_id text DEFAULT '')
    RETURNS TABLE (
        command_id bigint,
        seq bigint,
        accepted boolean)
    LANGUAGE plpgsql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog,
    fsm
    AS $$
DECLARE
    v_command_id bigint;
    v_seq bigint;
    v_blocked boolean;
BEGIN
    PERFORM
        PG_ADVISORY_XACT_LOCK(HASHTEXTEXTENDED(FORMAT('fsm.command/%s/%s', p_machine_id, p_entity_id), 0));
    -- The incumbent, if this key was already submitted. Under the lock this read
    -- is race-free, and command_unique_idem remains the backstop.
    --
    -- Every reference to the table is alias-qualified throughout this routine.
    -- The RETURNS TABLE column names are in scope as variables, and plpgsql's
    -- default variable_conflict setting raises on an ambiguous reference rather
    -- than silently picking one.
    SELECT
        c.command_id,
        c.seq
    INTO
        v_command_id,
        v_seq
    FROM
        fsm.command c
    WHERE
        c.machine_id = p_machine_id
        AND c.entity_id = p_entity_id
        AND c.idempotency_key = p_idempotency_key;
    IF FOUND THEN
        RETURN QUERY
        SELECT
            v_command_id,
            v_seq,
            FALSE;
        RETURN;
    END IF;
    -- One backward index scan on command_unique_seq, not a scan of the entity's
    -- history. coalesce supplies the first sequence number, since max() over an
    -- empty set is NULL rather than 0.
    SELECT
        COALESCE(MAX(c.seq), 0) + 1
    INTO
        v_seq
    FROM
        fsm.command c
    WHERE
        c.machine_id = p_machine_id
        AND c.entity_id = p_entity_id;
    -- An earlier non-terminal sibling means this command waits its turn. EXISTS
    -- states semi-join intent and stops at the first match; served by
    -- command_entity_open_idx, which indexes open commands only.
    v_blocked := EXISTS (
        SELECT
            1
        FROM
            fsm.command c
        WHERE
            c.machine_id = p_machine_id
            AND c.entity_id = p_entity_id
            AND c.status IN ('ready', 'leased'));
    INSERT INTO fsm.command (machine_id, entity_id, seq, idempotency_key, chart_version, event, blocked, visible_at, received_at, tenant, principal, source, correlation_id, causation_id)
        VALUES (p_machine_id, p_entity_id, v_seq, p_idempotency_key, p_chart_version, p_event, v_blocked, COALESCE(p_visible_at, NOW()), COALESCE(p_received_at, STATEMENT_TIMESTAMP()), COALESCE(p_tenant, ''), coalesce(p_principal, ''), COALESCE(p_source, ''), coalesce(p_correlation_id, ''), COALESCE(p_causation_id, ''))
    RETURNING
        fsm.command.command_id
    INTO
        v_command_id;
    RETURN QUERY
    SELECT
        v_command_id,
        v_seq,
        TRUE;
END;
$$;

-- ---------------------------------------------------------------------------
-- fsm.claim_commands: lease the next runnable commands for a machine.
--
-- THE CANDIDATE SET MUST BE A MATERIALISED CTE. Written the obvious way, as
--
--     UPDATE fsm.command SET ... WHERE command_id IN (SELECT ... LIMIT $2)
--
-- the planner puts the candidate subquery on the inner side of a nested-loop
-- semi join and rescans it once per outer row. Each rescan sees the rows this
-- same statement has already flipped to 'leased', so it returns the next
-- available command every time. LIMIT then bounds each rescan rather than the
-- statement, and a worker asking for one command checks out the entire queue.
-- The failure is silent: every row returned is a legitimately claimed command,
-- there is simply no bound on how many. A CTE containing a locking clause
-- cannot be inlined, which is what makes the candidate set evaluate exactly
-- once. MATERIALIZED is therefore redundant, and written anyway for the reader.
--
-- The WHERE clause is command_claim_idx's predicate plus the visibility test.
-- If you change one, change the index, this, and fsm.command_metrics together.
-- Drift does not break correctness, it silently stops using the index.
--
-- Per-entity mutual exclusion is not enforced here and does not need to be.
-- command_entity_head_idx guarantees at most one unblocked non-terminal command
-- per entity, so a batch can never contain two commands for the same entity.
--
-- now(), not clock_timestamp(). An index qualifier may not contain a volatile
-- function, so clock_timestamp() would demote the visibility test to a filter
-- applied per row after the index scan. now() is STABLE and folds in like a
-- parameter. If a claim ever runs inside a longer transaction, its staleness
-- errs safely twice over: fewer rows pass the visibility test, and the lease
-- granted is shorter than asked for, which redelivery and the fence handle.
--
-- FOR UPDATE SKIP LOCKED is what lets many workers claim concurrently instead
-- of queueing behind each other on the same head row. The claim transaction
-- deliberately ends here: the lease, not a held row lock, is what protects the
-- command while the application processes it outside any transaction.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fsm.claim_commands (p_machine_id text, p_batch integer, p_lease interval)
    RETURNS SETOF fsm.command
    LANGUAGE sql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog, fsm
    AS $$
    WITH candidate AS MATERIALIZED (
        SELECT
            c.command_id
        FROM
            fsm.command c
        WHERE
            c.machine_id = $1
            AND c.status IN (
                'ready', 'leased'
)
            AND NOT c.blocked
            AND c.visible_at <= NOW(
)
        ORDER BY
            c.visible_at,
            c.command_id
        LIMIT $2
        FOR UPDATE
            SKIP LOCKED)
UPDATE
    fsm.command c
SET
    status = 'leased',
    visible_at = NOW() + $3,
    lease_token = NEXTVAL('fsm.lease_token'),
    read_ct = c.read_ct + 1
FROM
    candidate
WHERE
    c.command_id = candidate.command_id
RETURNING
    c.*;
$$;

-- ---------------------------------------------------------------------------
-- fsm.reschedule_command: hand a failed command back for a later attempt.
--
-- The command keeps its place. It stays non-terminal, stays the entity's head,
-- and nothing else for that entity runs before it. This is what replaces a
-- separate retry queue: a retry is the same command, visible later.
--
-- Durable retry timing belongs to the database, so the delay is computed here
-- from the attempt count rather than passed in by a caller whose clock may be
-- skewed and whose process may not exist by the time the retry falls due.
--
-- Returns '-infinity' as the new visibility when the lease was lost, because
-- nothing in this schema is nullable and absence always has a value.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fsm.reschedule_command (p_command_id bigint, p_lease_token bigint, p_base_ms integer, p_cap_ms integer)
    RETURNS TABLE (
        outcome fsm.lease_outcome,
        visible_at timestamptz)
    LANGUAGE sql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog,
    fsm
    AS $$
    -- c.attempts on the right-hand side is the pre-update value, so the backoff
    -- is computed for the attempt that has just failed.
    --
    -- The CTE is referenced twice and runs exactly once regardless: a
    -- data-modifying CTE executes to completion even if its output goes unread.
    -- Zero rows back means the fence rejected the caller.
    WITH fenced AS (
        UPDATE
            fsm.command c
        SET
            status = 'ready',
            lease_token = 0,
            attempts = c.attempts + 1,
            visible_at = NOW() + fsm.retry_backoff (c.attempts + 1, $3, $4)
        WHERE
            c.command_id = $1
            AND c.lease_token = $2
            AND c.status = 'leased'
        RETURNING
            c.visible_at
)
    SELECT
        CASE WHEN EXISTS (
            SELECT
                1
            FROM
                fenced) THEN
            'updated'
        ELSE
            'lease_lost'
        END::fsm.lease_outcome AS outcome,
        COALESCE((
            SELECT
                f.visible_at
            FROM fenced f), '-infinity'::timestamptz) AS visible_at;
$$;

-- ---------------------------------------------------------------------------
-- fsm.extend_lease: keep a long-running command claimed.
--
-- Deliberately does not check whether the lease has already expired. If it has
-- and nobody reclaimed the command, extending is exactly the right outcome; if
-- somebody did reclaim it, the token no longer matches and the answer is
-- 'lease_lost'. The token, not the clock, decides ownership.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fsm.extend_lease (p_command_id bigint, p_lease_token bigint, p_lease interval)
    RETURNS fsm.lease_outcome
    LANGUAGE sql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog, fsm
    AS $$
    WITH fenced AS (
        UPDATE
            fsm.command c
        SET
            visible_at = NOW() + $3
        WHERE
            c.command_id = $1
            AND c.lease_token = $2
            AND c.status = 'leased'
        RETURNING
            1
)
    SELECT
        CASE WHEN EXISTS (
            SELECT
                1
            FROM
                fenced) THEN
            'updated'
        ELSE
            'lease_lost'
        END::fsm.lease_outcome;
$$;

-- ---------------------------------------------------------------------------
-- fsm.repair_blocked: opt-in reconciliation of the derived blocked column.
--
-- Detection runs by default on the maintenance tick, from
-- sql/command/blocked_drift.sql. Repair does not, and must be invoked
-- deliberately. Silently rewriting derived state hides whatever caused it to
-- drift: a wrong blocked flag is a symptom of a bug in submission or
-- acknowledgement, not something that happens on its own.
--
-- The expected CTE reads the table this statement also updates. That is safe
-- and intended: a data-modifying statement and its CTEs share one snapshot, so
-- the comparison is against the pre-update state throughout.
--
-- Grain: one row per command actually changed, empty when there is no drift.
--
-- If drift claims a leased row ought to be blocked, this raises
-- command_blocked_is_ready rather than writing it. That combination is
-- impossible by construction, so failing is the correct outcome.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fsm.repair_blocked ()
    RETURNS TABLE (
        command_id bigint,
        blocked boolean)
    LANGUAGE sql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog,
    fsm
    AS $$
    WITH expected AS (
        SELECT
            c.command_id,
            c.blocked,
            c.seq > MIN(c.seq) OVER (PARTITION BY c.machine_id,
                c.entity_id) AS expected_blocked
        FROM
            fsm.command c
        WHERE
            c.status IN ('ready', 'leased'))
    UPDATE
        fsm.command c
    SET
        blocked = e.expected_blocked
    FROM
        expected e
    WHERE
        c.command_id = e.command_id
        AND e.blocked IS DISTINCT FROM e.expected_blocked
    RETURNING
        c.command_id,
        c.blocked;
$$;

-- ---------------------------------------------------------------------------
-- fsm.command_metrics: "is this machine healthy", in one row per machine.
--
-- Grain: one row per machine_id that has at least one command. A machine with
-- no rows at all does not appear, which is the honest answer to "how is it
-- doing" for a machine that has never been used.
--
-- blocked is counted separately from runnable on purpose. A machine full of
-- blocked commands looks idle by every other measure while holding work that
-- may never run.
--
-- The runnable filter is fsm.claim_commands' WHERE clause verbatim, so the
-- number reported and the number claimable cannot drift apart. An expired lease
-- counts as runnable because it is claimable; in_flight counts only leases that
-- are still live.
--
-- count(*) FILTER, not count(column), so the counts are of rows and not of
-- non-null values. The two age columns coalesce to zero rather than returning
-- NULL over an empty set, which is the right reading here: no backlog is an age
-- of zero. Treat zero as "nothing waiting", not as "something arrived just now".
--
-- A sequential scan, and meant to be: this is a maintenance-tick query and not
-- a hot path. Its cost grows with retained history, which is what retention is
-- for.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fsm.command_metrics ()
    RETURNS TABLE (
        machine_id text,
        runnable bigint,
        in_flight bigint,
        scheduled bigint,
        blocked bigint,
        succeeded bigint,
        rejected bigint,
        dead_lettered bigint,
        oldest_runnable_age interval,
        oldest_unacked_age interval)
    LANGUAGE sql
    STABLE PARALLEL SAFE
    SET search_path = pg_catalog,
    fsm
    AS $$
    SELECT
        c.machine_id AS machine_id,
        COUNT(*) FILTER (WHERE c.status IN ('ready', 'leased')
            AND NOT c.blocked
            AND c.visible_at <= NOW()) AS runnable,
        COUNT(*) FILTER (WHERE c.status = 'leased'
            AND c.visible_at > NOW()) AS in_flight,
        COUNT(*) FILTER (WHERE c.status = 'ready'
            AND NOT c.blocked
            AND c.visible_at > NOW()) AS scheduled,
        COUNT(*) FILTER (WHERE c.status IN ('ready', 'leased')
            AND c.blocked) AS blocked,
        COUNT(*) FILTER (WHERE c.status = 'succeeded') AS succeeded,
        COUNT(*) FILTER (WHERE c.status = 'rejected') AS rejected,
        COUNT(*) FILTER (WHERE c.status = 'dead_letter') AS dead_lettered,
        COALESCE(MAX(NOW() - c.received_at) FILTER (WHERE c.status IN ('ready', 'leased')
        AND NOT c.blocked
        AND c.visible_at <= NOW()), interval '0') AS oldest_runnable_age,
        COALESCE(MAX(NOW() - c.received_at) FILTER (WHERE c.status IN ('ready', 'leased')), interval '0') AS oldest_unacked_age
    FROM
        fsm.command c
    GROUP BY
        c.machine_id;
$$;

