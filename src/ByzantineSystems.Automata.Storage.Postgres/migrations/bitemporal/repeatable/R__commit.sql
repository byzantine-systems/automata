CREATE OR REPLACE FUNCTION $temporal_schema$.commit_command (p_command_id bigint, p_lease_token bigint, p_expected_epoch bigint, p_status $schema$.command_status, p_error jsonb DEFAULT NULL, p_state jsonb DEFAULT NULL, p_instance_status $schema$.instance_status DEFAULT NULL, p_effective_at timestamptz DEFAULT NULL, p_event jsonb DEFAULT NULL, p_actions jsonb DEFAULT NULL, p_from_state jsonb DEFAULT NULL, p_to_state jsonb DEFAULT NULL, p_handled_by text DEFAULT NULL, p_exited text[] DEFAULT NULL, p_entered text[] DEFAULT NULL)
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
    finished record;
    m text;
    e text;
    v integer;
BEGIN
    IF (
        SELECT
            beliefs
        FROM
            $schema$.store_profile FOR SHARE) IS DISTINCT FROM 'bitemporal' THEN
        RAISE EXCEPTION 'bitemporal commit requires the bitemporal profile' USING ERRCODE = 'invalid_parameter_value';
END IF;
    SELECT
        *
    INTO
        finished
    FROM
        $schema$.finish_command (p_command_id, p_lease_token, p_expected_epoch, p_status, p_error, p_state, p_instance_status, p_effective_at, p_event, p_actions, p_from_state, p_to_state, p_handled_by, p_exited, p_entered);
    IF finished.outcome = 'finalized' AND p_status = 'succeeded' THEN
        SELECT
            c.machine_id,
            c.entity_id,
            c.chart_version
        INTO
            m,
            e,
            v
        FROM
            $schema$.command c
        WHERE
            c.command_id = p_command_id;
        PERFORM
            $temporal_schema$.close_and_open (m, e, p_effective_at, p_state, p_instance_status, finished.epoch, p_command_id, v);
    END IF;
    RETURN QUERY
    SELECT
        finished.outcome,
        finished.epoch,
        finished.status;
END
$$;

-- An old snapshot client must get a deliberate refusal, including on a fresh bitemporal
-- installation that has never applied the snapshot migration set.
CREATE OR REPLACE FUNCTION $schema$.commit_command (p_command_id bigint, p_lease_token bigint, p_expected_epoch bigint, p_status $schema$.command_status, p_error jsonb DEFAULT NULL, p_state jsonb DEFAULT NULL, p_instance_status $schema$.instance_status DEFAULT NULL, p_effective_at timestamptz DEFAULT NULL, p_event jsonb DEFAULT NULL, p_actions jsonb DEFAULT NULL, p_from_state jsonb DEFAULT NULL, p_to_state jsonb DEFAULT NULL, p_handled_by text DEFAULT NULL, p_exited text[] DEFAULT NULL, p_entered text[] DEFAULT NULL)
    RETURNS TABLE (
        outcome $schema$.finalize_outcome,
        epoch bigint,
        status $schema$.command_status)
    LANGUAGE plpgsql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog,
    pg_temp
    AS $$
BEGIN
    RAISE EXCEPTION 'snapshot commit cannot bypass bitemporal history'
        USING ERRCODE = 'invalid_parameter_value';
END
$$;

