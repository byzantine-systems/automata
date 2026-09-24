-- ---------------------------------------------------------------------------
-- Maintenance: registration, reaping, retention, notification and scheduling.
--
-- Every routine here is called the same way whoever calls it. pg_cron, when it
-- is installed, and the application's MaintenanceService, when it is not, both
-- call fsm.notify_pending and fsm.run_maintenance and nothing else, so a
-- deployment without pg_cron behaves identically, only on the application's
-- schedule. That is the whole reason maintenance is SQL rather than F#.
--
-- Nothing here repairs derived state. Drift is counted and reported; the repair
-- is fsm.repair_blocked, and something has to decide to call it.
--
-- Each routine declares its volatility, parallel safety and search_path, and
-- returns a value, for the reasons given at the top of R__command_routines.sql.
--
-- LANGUAGE sql unless a statement cannot be written without plpgsql, and here
-- there are exactly two such reasons:
--
--   * pgmq names a table after each queue, so reaching one takes EXECUTE on a
--     formatted identifier, which only plpgsql has. fsm.has_visible_actions
--     and fsm.purge_action_archive are that and nothing more.
--   * A SQL body is parsed as a whole, so it cannot name cron.* in a database
--     where pg_cron is not installed, even behind a guard. plpgsql resolves a
--     statement only when it reaches it, which is what lets
--     fsm.schedule_maintenance and fsm.unschedule_maintenance check for the
--     extension first.
--
-- Everything else is one statement the planner sees whole and a test can
-- EXPLAIN. Inlining is not the reason: SET search_path alone prevents a
-- function from being inlined, and these run once per tick at the top level
-- rather than per row of some larger query.
-- ---------------------------------------------------------------------------
-- ---------------------------------------------------------------------------
-- fsm.register_maintenance: a machine announcing itself on boot.
--
-- An upsert, so booting again is how a changed policy takes effect, and a
-- restored database whose registry is stale heals on the next start. Returns
-- whether the row was new.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fsm.register_maintenance (p_machine_id text, p_action_queue text, p_keep_commands interval, p_keep_belief_history interval, p_keep_action_archive interval, p_reap_after interval)
    RETURNS boolean
    LANGUAGE sql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog, fsm
    AS $$
    INSERT INTO fsm.machine_maintenance AS m (machine_id, action_queue, keep_commands, keep_belief_history, keep_action_archive, reap_after)
        VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT (machine_id)
        DO UPDATE SET
            action_queue = EXCLUDED.action_queue,
            keep_commands = EXCLUDED.keep_commands,
            keep_belief_history = EXCLUDED.keep_belief_history,
            keep_action_archive = EXCLUDED.keep_action_archive,
            reap_after = EXCLUDED.reap_after,
            registered_at = STATEMENT_TIMESTAMP()
        RETURNING (xmax = 0);
$$;

-- ---------------------------------------------------------------------------
-- fsm.reap_leases: hand long-expired leases back to ready.
--
-- Not needed for progress, and worth being honest about that. An expired lease
-- is already claimable, because the claim tests visible_at and not status.
-- What reaping buys is a status that tells the truth: a command whose worker
-- died reads 'ready' rather than 'leased' for as long as nobody claims it.
-- Poison is not handled here; the processor refuses a command claimed more
-- often than it may be attempted, which is where a crash loop is visible.
--
-- The grace period is deliberate. fsm.extend_lease accepts a lease that
-- expired and was not reclaimed, and reaping at the instant of expiry would
-- break that. A worker still holding the old token after a reap is fenced
-- exactly as it would be after a reclaim: its token no longer matches.
--
-- NOT blocked is always true of a leased row (command_blocked_is_ready), and is
-- written anyway so the predicate is command_claim_idx's and the range on
-- visible_at is an index scan.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fsm.reap_leases (p_machine_id text, p_reap_after interval)
    RETURNS bigint
    LANGUAGE sql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog, fsm
    AS $$
    WITH reaped AS (
        UPDATE
            fsm.command c
        SET
            status = 'ready',
            lease_token = 0
        WHERE
            c.machine_id = $1
            AND c.status IN ('ready', 'leased')
            AND NOT c.blocked
            AND c.status = 'leased'
            AND c.visible_at < NOW() - $2
        RETURNING
            1
)
    SELECT
        COUNT(*)
    FROM
        reaped;
$$;

-- ---------------------------------------------------------------------------
-- fsm.purge_commands: retention for a command's whole record.
--
-- A command, its transition, its error and its undelivered actions are deleted
-- together, because together they are one account of one decision and every
-- one of those children references the command. The foreign keys are NO
-- ACTION, which is checked at the end of the statement rather than per row, so
-- deleting parent and children in sibling CTEs of one statement satisfies them.
--
-- Three things are never purged:
--
--   * An open command. Retention is for history, not for work.
--   * An entity's newest command. max(seq) + 1 over an emptied entity would
--     restart at 1, and a sequence that goes backwards is not a sequence.
--   * Anything received after p_before. received_at is the clock because it is
--     the only one every command has: a succeeded command that changed nothing
--     has no transition, and only a failed one has an error.
--
-- A purged idempotency key is a forgotten key. A client retrying after the
-- retention period is accepted as new, so retention must exceed any client's
-- retry horizon. That is documented rather than enforced, because nothing here
-- knows the horizon.
--
-- Bounded by p_batch, so one call is one short transaction and the next tick
-- continues where this one stopped. SKIP LOCKED so a purge never waits on a
-- command something else is looking at.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fsm.purge_commands (p_machine_id text, p_before timestamptz, p_batch integer)
    RETURNS bigint
    LANGUAGE sql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog, fsm
    AS $$
    WITH doomed AS MATERIALIZED (
        SELECT
            c.command_id
        FROM
            fsm.command c
        WHERE
            c.machine_id = $1
            AND c.status IN (
                'succeeded', 'rejected', 'dead_letter'
)
            AND c.received_at < $2
            AND EXISTS (
                SELECT
                    1
                FROM
                    fsm.command later
                WHERE
                    later.machine_id = c.machine_id
                    AND later.entity_id = c.entity_id
                    AND later.seq > c.seq
)
            ORDER BY
                c.command_id
            LIMIT $3
            FOR UPDATE
                SKIP LOCKED
),
        transitions AS (
            DELETE FROM fsm.transition t USING doomed d
WHERE t.command_id = d.command_id), errors AS (
    DELETE FROM fsm.command_error e USING doomed d
WHERE e.command_id = d.command_id), undelivered AS (
    DELETE FROM fsm.action_dead_letter a USING doomed d
WHERE a.command_id = d.command_id), purged AS (
    DELETE FROM fsm.command c USING doomed d
WHERE c.command_id = d.command_id
RETURNING
    1
)
SELECT
    COUNT(*)
FROM
    purged;
$$;

-- ---------------------------------------------------------------------------
-- fsm.purge_belief_history: retention for superseded beliefs.
--
-- Deletes by upper(system_time), which is when the database stopped holding the
-- belief. The live table is never touched, so what is believed now is never
-- affected; what is lost is the ability to ask what was believed before the
-- cutoff. An as-of read with an older known_at answers as though the belief
-- had never been held, which is exactly what retention means and worth saying
-- where somebody configures it.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fsm.purge_belief_history (p_machine_id text, p_before timestamptz, p_batch integer)
    RETURNS bigint
    LANGUAGE sql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog, fsm
    AS $$
    WITH doomed AS MATERIALIZED (
        SELECT
            h.ctid
        FROM
            fsm.instance_state_history h
        WHERE
            h.machine_id = $1
            AND UPPER(
                h.system_time
) < $2
        LIMIT $3
),
purged AS (
    DELETE FROM fsm.instance_state_history h USING doomed d
WHERE h.ctid = d.ctid
RETURNING
    1
)
SELECT
    COUNT(*)
FROM
    purged;
$$;

-- ---------------------------------------------------------------------------
-- fsm.purge_action_archive: retention for delivered actions.
--
-- pgmq archives every message fsm.complete_action and fsm.abandon_action
-- acknowledge, and nothing reads the archive back. It is operational rather
-- than business data, so this deletes it by archived_at without any of the
-- care the command purge takes.
--
-- The queue reaches the statement as an identifier, so it is asserted first,
-- here, where the interpolation happens. A registered queue whose table is gone
-- purges nothing rather than failing the whole maintenance run.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fsm.purge_action_archive (p_queue text, p_before timestamptz, p_batch integer)
    RETURNS bigint
    LANGUAGE plpgsql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog, pg_temp
    AS $$
DECLARE
    v_purged bigint;
BEGIN
    PERFORM
        fsm.assert_queue_name (p_queue);
    IF TO_REGCLASS(FORMAT('pgmq.%I', 'a_' || p_queue)) IS NULL THEN
        RETURN 0;
    END IF;
    EXECUTE FORMAT('WITH doomed AS MATERIALIZED (SELECT a.msg_id FROM pgmq.%1$I a WHERE a.archived_at < $1 ORDER BY a.msg_id LIMIT $2) DELETE FROM pgmq.%1$I a USING doomed d WHERE a.msg_id = d.msg_id', 'a_' || p_queue)
    USING p_before, p_batch;
    GET DIAGNOSTICS v_purged = ROW_COUNT;
    RETURN v_purged;
END
$$;

-- ---------------------------------------------------------------------------
-- fsm.has_visible_actions: does this queue hold a message a worker could read.
--
-- pgmq's own visibility test, against its (vt) index. plpgsql only because the
-- table is named after the queue. A registered queue whose table is gone has
-- nothing visible, rather than failing the tick for every other machine.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fsm.has_visible_actions (p_queue text)
    RETURNS boolean
    LANGUAGE plpgsql
    STABLE PARALLEL SAFE
    SET search_path = pg_catalog, pg_temp
    AS $$
DECLARE
    v_visible boolean := FALSE;
BEGIN
    PERFORM
        fsm.assert_queue_name (p_queue);
    IF TO_REGCLASS(FORMAT('pgmq.%I', 'q_' || p_queue)) IS NOT NULL THEN
        EXECUTE FORMAT('SELECT EXISTS (SELECT 1 FROM pgmq.%I q WHERE q.vt <= clock_timestamp())', 'q_' || p_queue) INTO v_visible;
    END IF;
    RETURN v_visible;
END
$$;

-- ---------------------------------------------------------------------------
-- fsm.notify_pending: one tick of wake-ups, never one per write.
--
-- Called from a tick and never from a trigger. Every transaction with a pending
-- NOTIFY takes the global notification-queue lock at commit, which serialises
-- every notifying commit in the cluster, so notifying from the write path
-- caps write throughput at whatever that lock allows. One statement per tick
-- takes it once.
--
-- The command probe is fsm.claim_commands' WHERE clause verbatim, which makes
-- four places stating the claim predicate: command_claim_idx,
-- fsm.claim_commands, fsm.command_metrics and this. If you change one, change
-- all four. A drifted copy here breaks nothing loudly: it wakes workers for
-- commands they cannot claim, or fails to wake them for ones they can, and a
-- machine holding only blocked commands wakes every listener every tick.
-- SchemaContractTests asserts it still uses the index.
--
-- Channels are fsm_command and fsm_action, and payloads are the machine id or
-- the queue name, never data. NOTIFY bypasses row-level security, so anything
-- in a payload is visible to every listener on the channel, and listeners have
-- to re-read the source of truth regardless.
--
-- The CTEs are MATERIALIZED and counted, which is what runs them to
-- completion; a CTE nothing consumes is a notification nobody sends. A lost
-- notification costs the polling interval and nothing else. Returns how many
-- were sent.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fsm.notify_pending ()
    RETURNS integer
    LANGUAGE sql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog, fsm
    AS $$
    WITH commands AS MATERIALIZED (
        SELECT
            PG_NOTIFY(
                'fsm_command', m.machine_id
)
        FROM
            fsm.machine_maintenance m
        WHERE
            EXISTS (
                SELECT
                    1
                FROM
                    fsm.command c
                WHERE
                    c.machine_id = m.machine_id
                    AND c.status IN (
                        'ready', 'leased'
)
                    AND NOT c.blocked
                    AND c.visible_at <= NOW(
)
)
),
        actions AS MATERIALIZED (
            SELECT
                PG_NOTIFY(
                    'fsm_action', m.action_queue
)
            FROM
                fsm.machine_maintenance m
            WHERE
                fsm.has_visible_actions (
                    m.action_queue
))
    SELECT
        ((
                SELECT
                    COUNT(*)
                FROM
                    commands) + (
                    SELECT
                        COUNT(*)
                    FROM
                        actions))::integer;
$$;

-- ---------------------------------------------------------------------------
-- fsm.run_maintenance: one maintenance pass over every registered machine.
--
-- Single-flight. The advisory lock is tried rather than awaited, so when
-- another host or pg_cron is already running a pass this returns no rows at
-- once instead of queueing a duplicate behind it. It is taken in a
-- MATERIALIZED CTE so it is tried exactly once, and it is transaction-scoped,
-- so a crashed pass cannot leave it held. The target list is evaluated only
-- for rows that pass WHERE, so no task runs without the lock.
--
-- Each task is bounded by p_batch, so a large backlog is worked off over
-- several ticks rather than in one long transaction holding locks on the
-- busiest tables in the schema. A non-positive batch is refused by LIMIT
-- itself, or does nothing, and the application validates it before sending.
--
-- Drift in the derived blocked column is sql/command/blocked_drift.sql
-- narrowed to one machine, counted and never repaired. It is measured against
-- the statement's snapshot, before this pass reaped anything, and reaping does
-- not change which open commands are blocked, so the count is the same either
-- way.
--
-- One row per registered machine.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fsm.run_maintenance (p_batch integer)
    RETURNS TABLE (
        machine_id text,
        reaped bigint,
        purged_commands bigint,
        purged_beliefs bigint,
        purged_actions bigint,
        drifted bigint)
    LANGUAGE sql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog,
    fsm
    AS $$
    WITH turn AS MATERIALIZED (
        SELECT
            PG_TRY_ADVISORY_XACT_LOCK(
                HASHTEXTEXTENDED(
                    'fsm.run_maintenance', 0
)
) AS held
)
SELECT
    m.machine_id,
    fsm.reap_leases (m.machine_id, m.reap_after),
    fsm.purge_commands (m.machine_id, NOW() - m.keep_commands, $1),
    fsm.purge_belief_history (m.machine_id, NOW() - m.keep_belief_history, $1),
    fsm.purge_action_archive (m.action_queue, NOW() - m.keep_action_archive, $1),
    (
        SELECT
            COUNT(*)
        FROM (
            SELECT
                c.blocked,
                c.seq > MIN(c.seq) OVER (PARTITION BY c.entity_id) AS expected_blocked
            FROM
                fsm.command c
            WHERE
                c.machine_id = m.machine_id
                AND c.status IN ('ready', 'leased')) o
        WHERE
            o.blocked IS DISTINCT FROM o.expected_blocked)
FROM
    turn,
    fsm.machine_maintenance m
WHERE
    turn.held
ORDER BY
    m.machine_id;
$$;

-- ---------------------------------------------------------------------------
-- fsm.cron_schedule_of: an interval, as pg_cron would write it.
--
-- pg_cron takes either '<n> seconds', for 1 to 59 seconds, or a cron
-- expression. Anything else answers the empty string rather than a rounded
-- schedule, because a tick that silently runs at a different rate than
-- configured is worse than one that fails to schedule; the caller refuses it.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fsm.cron_schedule_of (p_every interval)
    RETURNS text
    LANGUAGE sql
    IMMUTABLE PARALLEL SAFE
    SET search_path = pg_catalog, fsm
    AS $$
    SELECT
        CASE WHEN e.s >= 1
            AND e.s < 60
            AND e.s = TRUNC(e.s) THEN
            FORMAT('%s seconds', e.s::integer)
        WHEN e.s >= 60
            AND e.s < 3600
            AND MOD(e.s, 60) = 0
            AND MOD(3600, e.s) = 0 THEN
            FORMAT('*/%s * * * *', (e.s / 60)::integer)
        WHEN e.s = 3600 THEN
            '0 * * * *'
        ELSE
            ''
        END
    FROM (
        SELECT
            EXTRACT(epoch FROM $1) AS s) e;
$$;

-- ---------------------------------------------------------------------------
-- fsm.schedule_maintenance: put the two ticks into pg_cron, if it is here.
--
-- Called on every boot rather than by a migration. The intervals are the
-- application's configuration, which a migration cannot know, and asserting
-- them on every start means a restored dump or a cloned database, whose
-- cron.job rows did not come with it, heals the next time anything boots.
-- cron.schedule with an existing name updates that job rather than adding a
-- second one, which is what makes calling this repeatedly safe.
--
-- plpgsql so that it can name cron.* at all: its body is not resolved until it
-- runs, and it never reaches those names without the extension.
--
-- Answers 'unavailable' without pg_cron, 'not_permitted' when this role may not
-- use it, and 'scheduled' otherwise. The first two are not failures: pg_cron is
-- optional and the application's MaintenanceService does the same work.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fsm.schedule_maintenance (p_notify_every interval, p_run_every interval, p_batch integer)
    RETURNS text
    LANGUAGE plpgsql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog, pg_temp
    AS $$
DECLARE
    v_notify text := fsm.cron_schedule_of (p_notify_every);
    v_run text := fsm.cron_schedule_of (p_run_every);
BEGIN
    IF v_notify = '' OR v_run = '' THEN
        RAISE EXCEPTION 'fsm.schedule_maintenance: pg_cron cannot run every % or every %; use whole seconds under a minute, whole minutes dividing an hour, or one hour', p_notify_every, p_run_every
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_batch IS NULL OR p_batch < 1 THEN
        RAISE EXCEPTION 'fsm.schedule_maintenance: the batch must be positive, not %', p_batch
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF NOT EXISTS (
        SELECT
            1
        FROM
            pg_extension e
        WHERE
            e.extname = 'pg_cron') THEN
    RETURN 'unavailable';
END IF;
    BEGIN
        PERFORM
            cron.schedule ('automata-notify', v_notify, 'SELECT fsm.notify_pending()');
        PERFORM
            cron.schedule ('automata-maintenance', v_run, FORMAT('SELECT count(*) FROM fsm.run_maintenance(%s)', p_batch));
    EXCEPTION
        WHEN insufficient_privilege THEN
            RETURN 'not_permitted';
    END;
    RETURN 'scheduled';
END
$$;

-- ---------------------------------------------------------------------------
-- fsm.unschedule_maintenance: take both ticks out of pg_cron, by name.
--
-- cron.job rows live outside this schema, so DROP SCHEMA fsm CASCADE leaves
-- them behind, calling functions that no longer exist every second. Anything
-- tearing the schema down calls this first; make db-reset does.
--
-- Returns how many jobs were removed, and 0 without pg_cron or without the
-- right to use it.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fsm.unschedule_maintenance ()
    RETURNS integer
    LANGUAGE plpgsql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog, pg_temp
    AS $$
DECLARE
    v_removed integer := 0;
BEGIN
    IF NOT EXISTS (
        SELECT
            1
        FROM
            pg_extension e
        WHERE
            e.extname = 'pg_cron') THEN
    RETURN 0;
END IF;
    -- A role that may not use pg_cron cannot have scheduled anything in it, so
    -- there is nothing for it to remove. Jobs another role scheduled are that
    -- role's to remove, and cron.job's own row-level security says the same.
    BEGIN
        PERFORM
            cron.unschedule (j.jobid)
        FROM
            cron.job j
        WHERE
            j.jobname IN ('automata-notify', 'automata-maintenance');
        GET DIAGNOSTICS v_removed = ROW_COUNT;
    EXCEPTION
        WHEN insufficient_privilege THEN
            RETURN 0;
    END;
    RETURN v_removed;
END
$$;

