-- Submission is core: an inbox records requests independently of the worker's capabilities.
CREATE OR REPLACE FUNCTION $schema$.submit_correction (p_machine_id text, p_entity_id text, p_idempotency_key text, p_chart_version integer, p_event jsonb, p_effective_at timestamptz, p_on_divergence $schema$.divergence, p_replay_limit integer, p_tenant text DEFAULT '', p_principal text DEFAULT '', p_source text DEFAULT '', p_correlation_id text DEFAULT '', p_causation_id text DEFAULT '')
    RETURNS TABLE (
        command_id bigint,
        seq bigint,
        accepted boolean,
        key_reused boolean,
        predecessor_received_at timestamptz)
    LANGUAGE plpgsql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog,
    pg_temp
    AS $$
DECLARE
    s record;
BEGIN
    SELECT
        *
    INTO
        s
    FROM
        $schema$.submit_command (p_machine_id, p_entity_id, p_idempotency_key, p_chart_version, p_event, NULL, NULL, p_tenant, p_principal, p_source, p_correlation_id, p_causation_id, 'correction', JSONB_BUILD_ARRAY(EXTRACT(epoch FROM p_effective_at), p_on_divergence, p_replay_limit));
    IF s.accepted THEN
        INSERT INTO $schema$.command_correction (command_id, effective_at, on_divergence, replay_limit)
            VALUES (s.command_id, p_effective_at, p_on_divergence, p_replay_limit);
    END IF;
    RETURN QUERY
    SELECT
        s.command_id,
        s.seq,
        s.accepted,
        s.key_reused,
        s.predecessor_received_at;
END
$$;

