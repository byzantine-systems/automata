CREATE OR REPLACE FUNCTION $schema$.commit_command (p_command_id bigint, p_lease_token bigint, p_expected_epoch bigint, p_status $schema$.command_status, p_error jsonb DEFAULT NULL, p_state jsonb DEFAULT NULL, p_instance_status $schema$.instance_status DEFAULT NULL, p_effective_at timestamptz DEFAULT NULL, p_actions jsonb DEFAULT NULL, p_from_state jsonb DEFAULT NULL, p_to_state jsonb DEFAULT NULL, p_handled_by text DEFAULT NULL, p_exited text[] DEFAULT NULL, p_entered text[] DEFAULT NULL)
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
    IF (
        SELECT
            beliefs
        FROM
            $schema$.store_profile FOR SHARE) IS DISTINCT FROM 'snapshot' THEN
        RAISE EXCEPTION 'snapshot commit cannot bypass bitemporal history' USING ERRCODE = 'invalid_parameter_value';
END IF;
    RETURN QUERY
    SELECT
        *
    FROM
        $schema$.finish_command (p_command_id, p_lease_token, p_expected_epoch, p_status, p_error, p_state, p_instance_status, p_effective_at, p_actions, p_from_state, p_to_state, p_handled_by, p_exited, p_entered);
END
$$;
