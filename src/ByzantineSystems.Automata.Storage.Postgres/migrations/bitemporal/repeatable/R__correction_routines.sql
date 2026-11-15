-- Transactional timeline rewrite. Callers hold the entity lock and establish the fence first.
CREATE OR REPLACE FUNCTION $temporal_schema$.rewrite_beliefs (p_machine text, p_entity text, p_from timestamptz, p_states jsonb[], p_statuses text[], p_valid_froms timestamptz[], p_epochs bigint[], p_command_ids bigint[], p_chart_versions integer[])
    RETURNS integer
    LANGUAGE plpgsql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog, pg_temp
    AS $$
DECLARE
    previous_at timestamptz := '-infinity';
    b record;
    next_at timestamptz;
    changed integer;
    removed integer;
BEGIN
    IF p_states IS NULL OR p_statuses IS NULL OR p_valid_froms IS NULL OR p_epochs IS NULL OR p_command_ids IS NULL OR p_chart_versions IS NULL OR CARDINALITY(p_states) <> CARDINALITY(p_statuses) OR CARDINALITY(p_states) <> CARDINALITY(p_valid_froms) OR CARDINALITY(p_states) <> CARDINALITY(p_epochs) OR CARDINALITY(p_states) <> CARDINALITY(p_command_ids) OR CARDINALITY(p_states) <> CARDINALITY(p_chart_versions) OR COALESCE(ARRAY_NDIMS(p_states), 1) <> 1 OR COALESCE(ARRAY_NDIMS(p_statuses), 1) <> 1 OR COALESCE(ARRAY_NDIMS(p_valid_froms), 1) <> 1 OR COALESCE(ARRAY_NDIMS(p_epochs), 1) <> 1 OR COALESCE(ARRAY_NDIMS(p_command_ids), 1) <> 1 OR COALESCE(ARRAY_NDIMS(p_chart_versions), 1) <> 1 THEN
        RAISE EXCEPTION 'belief arrays must be non-null, one-dimensional and equal in cardinality'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF ARRAY_POSITION(p_states, NULL) IS NOT NULL OR ARRAY_POSITION(p_statuses, NULL) IS NOT NULL OR ARRAY_POSITION(p_valid_froms, NULL) IS NOT NULL OR ARRAY_POSITION(p_epochs, NULL) IS NOT NULL OR ARRAY_POSITION(p_command_ids, NULL) IS NOT NULL OR ARRAY_POSITION(p_chart_versions, NULL) IS NOT NULL THEN
        RAISE EXCEPTION 'belief arrays cannot contain SQL NULL'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    FOR b IN
    SELECT
        t.*
    FROM
        UNNEST(p_states, p_statuses, p_valid_froms, p_epochs, p_command_ids, p_chart_versions)
    WITH ORDINALITY AS t (state, status, valid_from, epoch, command_id, chart_version, ordinal)
ORDER BY
    t.ordinal LOOP
        IF b.ordinal = 1 THEN
            IF b.valid_from IS DISTINCT FROM p_from THEN
                RAISE EXCEPTION 'first belief must start at the corrected instant' USING ERRCODE = 'invalid_parameter_value';
    END IF;
ELSIF b.valid_from <= previous_at THEN
    RAISE EXCEPTION 'beliefs must have strictly ascending valid times'
        USING ERRCODE = 'invalid_parameter_value';
END IF;
    previous_at := b.valid_from;
    IF b.epoch <= 0 OR b.command_id <= 0 OR b.chart_version <= 0 OR b.status NOT IN ('running', 'suspended', 'terminated') THEN
        RAISE EXCEPTION 'invalid belief fields'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
END LOOP;
    UPDATE
        $temporal_schema$.instance_state s
    SET
        valid_during = TSTZRANGE(LOWER(s.valid_during), p_from, '[)')
    WHERE
        s.machine_id = p_machine
        AND s.entity_id = p_entity
        AND LOWER(s.valid_during) < p_from
        AND UPPER(s.valid_during) > p_from;
    GET DIAGNOSTICS changed = ROW_COUNT;
    DELETE FROM $temporal_schema$.instance_state s
    WHERE s.machine_id = p_machine
        AND s.entity_id = p_entity
        AND LOWER(s.valid_during) >= p_from;
    GET DIAGNOSTICS removed = ROW_COUNT;
    FOR b IN
    SELECT
        t.*,
        LEAD(t.valid_from) OVER (ORDER BY t.ordinal) AS next_at
    FROM
        UNNEST(p_states, p_statuses, p_valid_froms, p_epochs, p_command_ids, p_chart_versions)
    WITH ORDINALITY AS t (state, status, valid_from, epoch, command_id, chart_version, ordinal)
ORDER BY
    t.ordinal LOOP
        INSERT INTO $temporal_schema$.instance_state (machine_id, entity_id, state, status, epoch, command_id, chart_version, valid_during)
            VALUES (p_machine, p_entity, b.state, b.status::$schema$.instance_status, b.epoch, b.command_id, b.chart_version, TSTZRANGE(b.valid_from, COALESCE(b.next_at, 'infinity'), '[)'));
    END LOOP;
    RETURN changed + removed;
END
$$;

-- Operator correction: optimistic concurrency against the core epoch, without advancing it.
CREATE OR REPLACE FUNCTION $temporal_schema$.correct_beliefs (p_machine text, p_entity text, p_expected bigint, p_from timestamptz, p_states jsonb[], p_statuses text[], p_valid_froms timestamptz[], p_epochs bigint[], p_command_ids bigint[], p_chart_versions integer[])
    RETURNS TABLE (
        superseded integer,
        actual_epoch bigint,
        CONFLICT boolean)
    LANGUAGE plpgsql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog,
    pg_temp
    AS $$
DECLARE
    actual bigint;
BEGIN
    PERFORM
        PG_ADVISORY_XACT_LOCK(HASHTEXTEXTENDED(FORMAT('$schema$.command/%s/%s', p_machine, p_entity), 0));
    SELECT
        s.epoch
    INTO
        actual
    FROM
        $schema$.entity_state s
    WHERE
        s.machine_id = p_machine
        AND s.entity_id = p_entity
    FOR UPDATE;
    actual := COALESCE(actual, 0);
    IF actual <> p_expected THEN
        RETURN QUERY
        SELECT
            0,
            actual,
            TRUE;
        RETURN;
    END IF;
    RETURN QUERY
    SELECT
        $temporal_schema$.rewrite_beliefs (p_machine, p_entity, p_from, p_states, p_statuses, p_valid_froms, p_epochs, p_command_ids, p_chart_versions),
        actual,
        FALSE;
END
$$;

CREATE OR REPLACE FUNCTION $temporal_schema$.commit_correction (p_command_id bigint, p_lease_token bigint, p_expected_epoch bigint, p_valid_from timestamptz, p_states jsonb[], p_statuses text[], p_valid_froms timestamptz[], p_epochs bigint[], p_command_ids bigint[], p_chart_versions integer[], p_instance_status $schema$.instance_status, p_from_state jsonb, p_to_state jsonb, p_handled_by text, p_exited text[], p_entered text[])
    RETURNS TABLE (
        outcome $schema$.finalize_outcome,
        epoch bigint,
        status $schema$.command_status)
    LANGUAGE plpgsql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog,
    pg_temp
    AS $$
DECLARE
    standing record;
BEGIN
    IF (
        SELECT
            beliefs
        FROM
            $schema$.store_profile FOR SHARE) IS DISTINCT FROM 'bitemporal' THEN
        RAISE EXCEPTION 'bitemporal profile required' USING ERRCODE = 'invalid_parameter_value';
END IF;
    SELECT
        *
    INTO
        standing
    FROM
        $schema$.command_standing (p_command_id, p_lease_token);
    IF standing.outcome <> 'finalized' THEN
        RETURN QUERY
        SELECT
            standing.outcome,
            COALESCE(standing.epoch, p_expected_epoch),
            standing.status;
        RETURN;
    END IF;
    IF (
        SELECT
            kind
        FROM
            $schema$.command
        WHERE
            command_id = p_command_id) <> 'correction' THEN
        RAISE EXCEPTION 'command is not a correction'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF standing.epoch <> p_expected_epoch THEN
        RETURN QUERY
        SELECT
            'conflict'::$schema$.finalize_outcome,
            standing.epoch,
            standing.status;
        RETURN;
    END IF;
    IF p_states IS NULL OR CARDINALITY(p_states) = 0 THEN
        RAISE EXCEPTION 'a correction commit requires at least one belief'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    PERFORM
        $schema$.append_transition (standing.machine_id, standing.entity_id, p_expected_epoch + 1, p_command_id, standing.chart_version, '[]', p_from_state, p_to_state, p_handled_by, p_exited, p_entered, p_instance_status, p_valid_from);
    PERFORM
        $temporal_schema$.rewrite_beliefs (standing.machine_id, standing.entity_id, p_valid_from, p_states, p_statuses, p_valid_froms, p_epochs, p_command_ids, p_chart_versions);
    PERFORM
        $schema$.set_entity_state (standing.machine_id, standing.entity_id, p_to_state, p_instance_status, p_expected_epoch + 1, p_command_id, standing.chart_version, p_valid_from);
    PERFORM
        $schema$.close_command (p_command_id, 'succeeded');
    RETURN QUERY
    SELECT
        'finalized'::$schema$.finalize_outcome,
        p_expected_epoch + 1,
        'succeeded'::$schema$.command_status;
END
$$;

