-- These operations run in the caller's transaction. Both bundled commit paths and custom
-- correction implementations use them, so lease fencing and head promotion have one meaning.
CREATE OR REPLACE FUNCTION $schema$.command_standing (p_command_id bigint, p_token bigint)
    RETURNS TABLE (
        outcome $schema$.finalize_outcome,
        epoch bigint,
        status $schema$.command_status,
        machine_id text,
        entity_id text,
        chart_version integer)
    LANGUAGE plpgsql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog,
    pg_temp
    AS $$
DECLARE
    c $schema$.command%ROWTYPE;
    current_epoch bigint;
BEGIN
    SELECT
        *
    INTO
        c
    FROM
        $schema$.command q
    WHERE
        q.command_id = p_command_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'no command %', p_command_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF c.status IN ('succeeded', 'rejected', 'dead_letter') THEN
        SELECT
            t.epoch
        INTO
            current_epoch
        FROM
            $schema$.transition t
        WHERE
            t.command_id = p_command_id;
        RETURN QUERY
        SELECT
            CASE WHEN c.lease_token = p_token THEN
                'already_finalized'
            ELSE
                'lease_lost' END::$schema$.finalize_outcome,
                current_epoch,
                c.status,
                c.machine_id,
                c.entity_id,
                c.chart_version;
                RETURN;
            END IF;
        IF c.status <> 'leased' OR c.lease_token <> p_token THEN
            RETURN QUERY
            SELECT
                'lease_lost'::$schema$.finalize_outcome,
                0::bigint,
                c.status,
                c.machine_id,
                c.entity_id,
                c.chart_version;
            RETURN;
        END IF;
        -- Serializes missing snapshots too, for which SELECT FOR UPDATE cannot lock a row.
        PERFORM
            PG_ADVISORY_XACT_LOCK(HASHTEXTEXTENDED(FORMAT('$schema$.command/%s/%s', c.machine_id, c.entity_id), 0));
        SELECT
            s.epoch
        INTO
            current_epoch
        FROM
            $schema$.entity_state s
        WHERE
            s.machine_id = c.machine_id
            AND s.entity_id = c.entity_id
        FOR UPDATE;
        RETURN QUERY
        SELECT
            'finalized'::$schema$.finalize_outcome,
            COALESCE(current_epoch, 0),
            c.status,
            c.machine_id,
            c.entity_id,
            c.chart_version;
END
$$;

CREATE OR REPLACE FUNCTION $schema$.set_entity_state (p_machine text, p_entity text, p_state jsonb, p_status $schema$.instance_status, p_epoch bigint, p_command bigint, p_version integer, p_at timestamptz)
    RETURNS bigint
    LANGUAGE sql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog, pg_temp
    AS $$
    INSERT INTO $schema$.entity_state (machine_id, entity_id, state, status, epoch, command_id, chart_version, effective_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    ON CONFLICT (machine_id, entity_id)
        DO UPDATE SET
            state = EXCLUDED.state,
            status = EXCLUDED.status,
            epoch = EXCLUDED.epoch,
            command_id = EXCLUDED.command_id,
            chart_version = EXCLUDED.chart_version,
            effective_at = EXCLUDED.effective_at,
            committed_at = STATEMENT_TIMESTAMP()
        RETURNING
            epoch;
$$;

CREATE OR REPLACE FUNCTION $schema$.append_transition (p_machine text, p_entity text, p_epoch bigint, p_command bigint, p_version integer, p_actions jsonb, p_from jsonb, p_to jsonb, p_handled text, p_exited text[], p_entered text[], p_status $schema$.instance_status, p_at timestamptz)
    RETURNS bigint
    LANGUAGE sql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog, pg_temp
    AS $$
    INSERT INTO $schema$.transition (machine_id, entity_id, epoch, command_id, chart_version, actions, from_state, to_state, handled_by, exited, entered, status, effective_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
    RETURNING
        epoch;
$$;

CREATE OR REPLACE FUNCTION $schema$.close_command (p_command bigint, p_status $schema$.command_status)
    RETURNS boolean
    LANGUAGE plpgsql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog, pg_temp
    AS $$
DECLARE
    m text;
    e text;
BEGIN
    IF p_status NOT IN ('succeeded', 'rejected', 'dead_letter') THEN
        RAISE EXCEPTION 'a command can close only with a terminal status'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    UPDATE
        $schema$.command c
    SET
        status = p_status
    WHERE
        c.command_id = p_command
    RETURNING
        c.machine_id,
        c.entity_id
    INTO
        m,
        e;
    UPDATE
        $schema$.command c
    SET
        blocked = FALSE
    WHERE
        c.command_id = (
            SELECT
                n.command_id
            FROM
                $schema$.command n
            WHERE
                n.machine_id = m
                AND n.entity_id = e
                AND n.status IN ('ready', 'leased')
            ORDER BY
                n.seq
            LIMIT 1);
    RETURN TRUE;
END
$$;
