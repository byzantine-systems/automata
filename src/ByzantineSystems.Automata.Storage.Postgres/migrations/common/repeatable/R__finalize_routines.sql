-- Atomic command completion. Expected fencing outcomes return before any write.
CREATE OR REPLACE FUNCTION $schema$.finish_command (p_command_id bigint, p_lease_token bigint, p_expected_epoch bigint, p_status $schema$.command_status, p_error jsonb DEFAULT NULL, p_instance_status $schema$.instance_status DEFAULT NULL, p_effective_at timestamptz DEFAULT NULL, p_actions jsonb DEFAULT NULL, p_from_state jsonb DEFAULT NULL, p_to_state jsonb DEFAULT NULL, p_handled_by text DEFAULT NULL, p_exited text[] DEFAULT NULL, p_entered text[] DEFAULT NULL)
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
    queue_name text;
BEGIN
    IF p_status NOT IN ('succeeded', 'rejected', 'dead_letter') THEN
        RAISE EXCEPTION 'not a terminal status: %', p_status
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_status = 'succeeded' AND (p_to_state IS NULL OR p_effective_at IS NULL) THEN
        RAISE EXCEPTION 'a succeeded command requires its transition'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_status <> 'succeeded' AND p_error IS NULL THEN
        RAISE EXCEPTION 'a failed command requires an error'
            USING ERRCODE = 'invalid_parameter_value';
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
    IF p_status = 'succeeded' THEN
        IF standing.epoch <> p_expected_epoch THEN
            RETURN QUERY
            SELECT
                'conflict'::$schema$.finalize_outcome,
                standing.epoch,
                standing.status;
            RETURN;
        END IF;
        PERFORM
            $schema$.append_transition (standing.machine_id, standing.entity_id, p_expected_epoch + 1, p_command_id, standing.chart_version, p_actions, p_from_state, p_to_state, p_handled_by, p_exited, p_entered, p_instance_status, p_effective_at);
        PERFORM
            $schema$.set_entity_state (standing.machine_id, standing.entity_id, p_to_state, p_instance_status, p_expected_epoch + 1, p_command_id, standing.chart_version, p_effective_at);
        IF p_actions IS NOT NULL AND JSONB_ARRAY_LENGTH(p_actions) > 0 THEN
            SELECT
                m.action_queue
            INTO
                queue_name
            FROM
                $schema$.machine_maintenance m
            WHERE
                m.machine_id = standing.machine_id;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'action delivery is not registered for machine %', standing.machine_id
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
            IF queue_name <> '' THEN
                PERFORM
                    $schema$.assert_queue_name (queue_name);
                PERFORM
                    pgmq.send (queue_name::text, JSONB_BUILD_OBJECT('machine_id', standing.machine_id, 'entity_id', standing.entity_id, 'command_id', p_command_id, 'epoch', p_expected_epoch + 1, 'ordinal', a.ordinal - 1, 'action', a.value), 0::integer)
                FROM
                    JSONB_ARRAY_ELEMENTS(p_actions)
    WITH ORDINALITY AS a (value, ordinal);
            END IF;
        END IF;
    ELSE
        RAISE EXCEPTION 'failures use fail_command' USING ERRCODE = 'invalid_parameter_value';
    END IF;
    PERFORM
        $schema$.close_command (p_command_id, p_status);
    RETURN QUERY
    SELECT
        'finalized'::$schema$.finalize_outcome,
        CASE WHEN p_status = 'succeeded' THEN
            p_expected_epoch + 1
        ELSE
            p_expected_epoch
        END,
        p_status;
END
$$;
