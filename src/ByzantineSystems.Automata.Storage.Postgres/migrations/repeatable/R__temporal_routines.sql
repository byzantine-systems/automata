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
-- fsm.close_and_open: the one valid-time split.
--
-- Every valid-time mutation routes through here, so the close-then-insert is
-- written and tested once instead of being re-derived at each call site.
--
-- TODO: PostgreSQL 19 has UPDATE ... FOR PORTION OF, which does this in one
-- statement. Our floor is 18. When the floor moves, this routine collapses and
-- its callers do not change.
--
-- Four cases, which is why this is plpgsql rather than one clever statement:
--
--   no live belief          insert [p_at, infinity)            -> opened
--   live [t0, inf), p_at>t0 close live at p_at, then insert    -> split
--   live [t0, inf), p_at=t0 delete live, then insert           -> replaced
--   live [t0, inf), p_at<t0 raise                              -> no row
--
-- The third case: closing [t0, infinity) at t0 would write [t0, t0), an empty
-- range that no query can return. instance_state_valid_nonempty and the
-- temporal key both reject it, so the alternative to replacing is a failed
-- write rather than a subtly wrong one. A correction effective from the exact
-- instant a belief began does not split that belief, it supersedes it whole.
--
-- The delete goes through the versioning trigger, so the superseded belief is
-- archived rather than discarded. Beliefs are never overwritten; that is the
-- rule the whole correction story rests on.
--
-- TODO: a p_at earlier than the live belief's start raises rather than
-- guessing. Reconciling it means replaying the affected suffix against the
-- chart version each command was originally resolved under, which is correction
-- replay and belongs to its own step. Guessing here would corrupt valid time
-- quietly.
--
-- VOLATILE and PARALLEL UNSAFE because it writes.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fsm.close_and_open (p_machine_id text, p_entity_id text, p_at timestamptz, p_state jsonb)
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
    v_outcome fsm.belief_outcome;
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
    IF v_live_from IS NULL THEN
        v_outcome := 'opened';
    ELSIF p_at > v_live_from THEN
        UPDATE
            fsm.instance_state s
        SET
            valid_during = TSTZRANGE(v_live_from, p_at, '[)')
        WHERE
            s.machine_id = p_machine_id
            AND s.entity_id = p_entity_id
            AND UPPER(s.valid_during) = 'infinity';
        v_outcome := 'split';
    ELSIF p_at = v_live_from THEN
        DELETE FROM fsm.instance_state s
        WHERE s.machine_id = p_machine_id
            AND s.entity_id = p_entity_id
            AND UPPER(s.valid_during) = 'infinity';
        v_outcome := 'replaced';
    ELSE
        RAISE EXCEPTION 'valid-time instant % precedes the live belief, which begins at %', p_at, v_live_from
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    INSERT INTO fsm.instance_state (machine_id, entity_id, state, valid_during)
        VALUES (p_machine_id, p_entity_id, p_state, TSTZRANGE(p_at, 'infinity', '[)'));
    RETURN QUERY
    SELECT
        v_outcome,
        p_at;
END
$$;

