-- Header, selected child and command closure are one fenced transaction.
CREATE OR REPLACE FUNCTION $schema$.fail_command (
    p_command_id bigint, p_token bigint, p_status $schema$.command_status,
    p_kind $schema$.failure_kind, p_domain jsonb, p_reason text, p_replay text,
    p_version integer, p_epoch bigint, p_budget integer)
RETURNS TABLE (outcome $schema$.finalize_outcome, epoch bigint, status $schema$.command_status)
LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE SET search_path = pg_catalog, pg_temp AS $$
DECLARE standing record;
BEGIN
    SELECT * INTO standing FROM $schema$.command_standing(p_command_id, p_token);
    IF standing.outcome <> 'finalized' THEN
        RETURN QUERY SELECT standing.outcome, COALESCE(standing.epoch, 0::bigint), standing.status;
        RETURN;
    END IF;
    IF p_status IS NULL OR p_status NOT IN ('rejected', 'dead_letter') OR
       (CASE p_kind
        WHEN 'domain' THEN p_domain IS NOT NULL AND p_reason = '' AND p_replay = '' AND p_version = 0 AND p_epoch = 0 AND p_budget = 0
        WHEN 'machine' THEN p_domain = 'null'::jsonb AND p_reason IS NOT NULL AND p_replay = '' AND p_version = 0 AND p_epoch = 0 AND p_budget = 0
        WHEN 'replay' THEN p_domain = 'null'::jsonb AND p_reason IS NOT NULL AND p_replay <> '' AND p_version IS NOT NULL AND p_epoch IS NOT NULL AND p_budget IS NOT NULL
        ELSE FALSE END) IS DISTINCT FROM TRUE THEN
        RAISE EXCEPTION 'invalid failure fields' USING ERRCODE = 'invalid_parameter_value';
    END IF;
    INSERT INTO $schema$.command_error(command_id, kind) VALUES (p_command_id, p_kind);
    CASE p_kind
    WHEN 'domain' THEN INSERT INTO $schema$.command_error_domain(command_id, error) VALUES (p_command_id, p_domain);
    WHEN 'machine' THEN INSERT INTO $schema$.command_error_machine(command_id, reason) VALUES (p_command_id, p_reason);
    WHEN 'replay' THEN INSERT INTO $schema$.command_error_replay(command_id, replay_kind, version, epoch, reason, budget)
        VALUES (p_command_id, p_replay::$schema$.replay_failure, p_version, p_epoch, p_reason, p_budget);
    ELSE RAISE EXCEPTION 'unknown failure';
    END CASE;
    PERFORM $schema$.close_command(p_command_id, p_status);
    RETURN QUERY SELECT 'finalized'::$schema$.finalize_outcome, 0::bigint, p_status;
END $$;
