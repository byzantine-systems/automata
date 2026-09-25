-- ---------------------------------------------------------------------------
-- Chart identity routines.
--
-- Repeatable, like R__command_routines.sql: DbUp reapplies this whenever its
-- content changes, so editing a body is an edit to this file rather than a new
-- migration. The conventions that file documents hold here too, in that every
-- routine declares its volatility, its parallel safety and its search_path,
-- returns a value rather than void, and is not overloaded.
-- ---------------------------------------------------------------------------
-- ---------------------------------------------------------------------------
-- fsm.register_chart_version: claim a version for a fingerprint, or report what
-- the stored one says.
--
-- Called once per process startup per machine, and by every process at once
-- after a deployment, so the concurrent path is the normal path rather than an
-- edge case.
--
-- Three ordered statements, therefore plpgsql. The re-read at the end is the
-- reason this is not one clever CTE. ON CONFLICT tests uniqueness against the
-- latest row version, while a sibling SELECT in the same statement reads the
-- statement's snapshot. A registration that lost a race would therefore find
-- the insert skipped and the SELECT still seeing nothing, and would report
-- 'mismatched' to a process whose chart is identical to the winner's. A second
-- statement takes a fresh snapshot under READ COMMITTED and sees the committed
-- row.
--
-- The two idioms that avoid a re-read both cost more than they save. DO UPDATE
-- SET fingerprint = excluded.fingerprint writes a new tuple on every startup of
-- every process, and reading xmax = 0 to tell an insert from an update reaches
-- into storage internals to recover something two plain statements already
-- know.
--
-- There is no routine for deleting a registration. Forgetting one is how this
-- check gets disarmed by habit, and the foreign key from fsm.command refuses it
-- anyway once a single command has referenced the version.
--
-- VOLATILE and PARALLEL UNSAFE because it writes.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fsm.register_chart_version (p_machine_id text, p_version integer, p_fingerprint text)
    RETURNS TABLE (
        registration fsm.chart_registration,
        stored_fingerprint text)
    LANGUAGE plpgsql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog,
    pg_temp
    AS $$
DECLARE
    v_stored text;
    v_inserted integer;
BEGIN
    SELECT
        c.fingerprint
    INTO
        v_stored
    FROM
        fsm.machine_chart_version c
    WHERE
        c.machine_id = p_machine_id
        AND c.version = p_version;
    IF v_stored IS NULL THEN
        INSERT INTO fsm.machine_chart_version (machine_id, version, fingerprint)
            VALUES (p_machine_id, p_version, p_fingerprint)
        ON CONFLICT ON CONSTRAINT machine_chart_version_pkey
            DO NOTHING;
        GET DIAGNOSTICS v_inserted = ROW_COUNT;
        IF v_inserted = 1 THEN
            RETURN QUERY
            SELECT
                'registered'::fsm.chart_registration,
                p_fingerprint;
            RETURN;
        END IF;
        -- Lost the race. The winner has committed by now, so read it.
        SELECT
            c.fingerprint
        INTO
            v_stored
        FROM
            fsm.machine_chart_version c
        WHERE
            c.machine_id = p_machine_id
            AND c.version = p_version;
    END IF;
    RETURN QUERY
    SELECT
        CASE WHEN v_stored = p_fingerprint THEN
            'matched'
        ELSE
            'mismatched'
        END::fsm.chart_registration,
        v_stored;
END;
$$;

