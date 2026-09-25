-- ---------------------------------------------------------------------------
-- Temporal routines: the system-time trigger, and the valid-time split.
--
-- Repeatable, like the other R__ scripts, so editing a body is an edit to this
-- file rather than a new migration. The conventions R__command_routines.sql
-- documents hold here: explicit volatility, parallel safety and search_path,
-- no routine returning void, none overloaded.
--
-- The trigger *attachment* lives here too, which is unusual enough to justify.
-- Migrator.fs applies main scripts first and repeatable scripts second, so a
-- CREATE TRIGGER inside 005_instance_state.sql would reference a function that
-- does not exist yet on a fresh database. PostgreSQL 14 added CREATE OR REPLACE
-- TRIGGER, so the attachment can be reapplied on every migration run alongside
-- the function it names, and the ordering problem disappears.
-- ---------------------------------------------------------------------------
-- ---------------------------------------------------------------------------
-- fsm.temporal_versioning: archive the superseded row, stamp the new one.
--
-- Column-agnostic. It reads tg_table_schema and tg_table_name at fire time and
-- writes OLD into <table>_history by naming convention, so one function serves
-- every temporal table rather than one per table. Attach it with:
--
--   CREATE OR REPLACE TRIGGER <table>_versioning_trigger
--     BEFORE INSERT OR UPDATE OR DELETE ON fsm.<table>
--     FOR EACH ROW EXECUTE FUNCTION fsm.temporal_versioning ();
--
-- ## The instant: statement_timestamp(), and why not the other two
--
-- Belief time has to advance *within* a transaction, because two beliefs really
-- did happen in sequence. That rules out now() and transaction_timestamp(),
-- which are frozen transaction-wide: an insert followed by an update inside one
-- transaction would close the archived row at its own lower bound and produce
-- an empty, and therefore invisible, system_time window.
--
-- clock_timestamp() fails the other way. It advances on every call, so an
-- UPDATE touching a thousand rows would stamp a thousand belief instants
-- microseconds apart, and the archived upper bound would not even meet the live
-- lower bound on the *same* row: two calls, two values, and a gap between the
-- versions where an as-of query finds nothing at all.
--
-- statement_timestamp() advances between statements while staying fixed within
-- one, which is exactly the granularity belief time wants. It is also STABLE
-- rather than VOLATILE, so it is evaluated once and behaves like a parameter
-- instead of being re-evaluated per row and blocking constant folding.
--
-- `at` is captured once into a local even though a STABLE function would agree
-- with itself across two calls. That is for the reader: the moment the old
-- belief ends is the same moment the new one begins, and the code should say
-- so rather than leave it to be inferred.
--
-- ## The open bound
--
-- Always the literal 'infinity', never NULL. Both spell "forever" to a range
-- containment test, but only one of them is a value: upper_inf() separates
-- them, `= 'infinity'` finds one and not the other, and a table mixing the two
-- cannot be queried consistently for liveness. instance_state_live_idx keys off
-- exactly that comparison.
--
-- Schema-qualified everywhere, and SET search_path, so a bare name cannot land
-- wherever the connecting role's search_path happens to point.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fsm.temporal_versioning ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    SET search_path = pg_catalog, pg_temp
    AS $$
DECLARE
    at timestamptz := STATEMENT_TIMESTAMP();
BEGIN
    IF tg_op IN ('UPDATE', 'DELETE') THEN
        OLD.system_time := TSTZRANGE(LOWER(OLD.system_time), at, '[)');
        EXECUTE FORMAT('INSERT INTO %I.%I SELECT ($1).*', tg_table_schema, tg_table_name || '_history')
        USING old;
    END IF;
        IF tg_op = 'DELETE' THEN
            RETURN old;
        END IF;
        NEW.system_time := TSTZRANGE(at, 'infinity', '[)');
        RETURN new;
END
$$;

CREATE OR REPLACE TRIGGER instance_state_versioning_trigger
    BEFORE INSERT OR UPDATE OR DELETE ON fsm.instance_state
    FOR EACH ROW
    EXECUTE FUNCTION fsm.temporal_versioning ();

-- ---------------------------------------------------------------------------
-- fsm.split_belief: the portion update, and why it is its own routine.
--
-- UPDATE ... FOR PORTION OF will not take a plpgsql variable as a bound. On
-- 19beta3 the bound expressions are parsed without plpgsql's variable
-- substitution, so a variable there is read as a column reference and the
-- statement fails with "cannot use column reference in FOR PORTION OF
-- expression". A prepared statement's parameters work, and so do a SQL
-- function's, which is what this is.
--
-- Positional $n rather than the parameter names, since names resolve through
-- the same path that fails inside plpgsql.
--
-- Returns the number of rows the portion update matched, because no routine
-- here returns void.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fsm.split_belief (p_machine_id text, p_entity_id text, p_at timestamptz, p_state jsonb, p_status fsm.instance_status, p_epoch bigint, p_command_id bigint, p_chart_version integer)
    RETURNS bigint
    LANGUAGE sql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog, pg_temp
    AS $$
    WITH updated AS (
        UPDATE
            fsm.instance_state FOR PORTION OF valid_during
        FROM
            $3 TO 'infinity'
        SET
            state = $4,
            status = $5,
            epoch = $6,
            command_id = $7,
            chart_version = $8
        WHERE
            machine_id = $1
            AND entity_id = $2
            AND UPPER(valid_during) = 'infinity'
        RETURNING
            1
)
    SELECT
        COUNT(*)
    FROM
        updated;
$$;

-- ---------------------------------------------------------------------------
-- fsm.close_and_open: the one valid-time mutation.
--
-- Every valid-time write routes through here, so the split is written and
-- tested once instead of being re-derived at each call site.
--
-- UPDATE ... FOR PORTION OF does the split itself: it rewrites the portion of
-- the matched rows covered by [p_at, infinity) and re-inserts the remainder as
-- its own row, keeping the temporal key satisfied throughout. Both the update
-- and the re-insert fire the versioning trigger, so the superseded belief is
-- archived rather than discarded.
--
-- Three cases remain, which is why this is still plpgsql:
--
--   no live belief          insert [p_at, infinity)   -> opened
--   live [t0, inf), p_at>t0 portion update splits it  -> split
--   live [t0, inf), p_at=t0 portion covers it whole   -> replaced
--   live [t0, inf), p_at<t0 raise                     -> no row
--
-- The read is what distinguishes them and what guards the fourth. Left to
-- itself, a portion update starting before the live belief would rewrite every
-- historical version it overlaps, silently rewriting settled history.
--
-- TODO: a p_at earlier than the live belief's start raises rather than
-- guessing. Reconciling it means replaying the affected suffix against the
-- chart version each command was originally resolved under, which is correction
-- replay and belongs to its own step.
--
-- VOLATILE and PARALLEL UNSAFE because it writes.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fsm.close_and_open (p_machine_id text, p_entity_id text, p_at timestamptz, p_state jsonb, p_status fsm.instance_status, p_epoch bigint, p_command_id bigint, p_chart_version integer)
    RETURNS TABLE (
        outcome fsm.belief_outcome,
        valid_from timestamptz)
    LANGUAGE plpgsql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog,
    pg_temp
    AS $$
DECLARE
    v_live_from timestamptz;
BEGIN
    SELECT
        LOWER(s.valid_during)
    INTO
        v_live_from
    FROM
        fsm.instance_state s
    WHERE
        s.machine_id = p_machine_id
        AND s.entity_id = p_entity_id
        AND UPPER(s.valid_during) = 'infinity';
    IF v_live_from IS NOT NULL AND p_at < v_live_from THEN
        RAISE EXCEPTION 'valid-time instant % precedes the live belief, which begins at %', p_at, v_live_from
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF v_live_from IS NULL THEN
        INSERT INTO fsm.instance_state (machine_id, entity_id, state, status, epoch, command_id, chart_version, valid_during)
            VALUES (p_machine_id, p_entity_id, p_state, p_status, p_epoch, p_command_id, p_chart_version, TSTZRANGE(p_at, 'infinity', '[)'));
        RETURN QUERY
        SELECT
            'opened'::fsm.belief_outcome,
            p_at;
        RETURN;
    END IF;
    PERFORM
        fsm.split_belief (p_machine_id, p_entity_id, p_at, p_state, p_status, p_epoch, p_command_id, p_chart_version);
    RETURN QUERY
    SELECT
        CASE WHEN p_at > v_live_from THEN
            'split'
        ELSE
            'replaced'
        END::fsm.belief_outcome,
        p_at;
END
$$;

