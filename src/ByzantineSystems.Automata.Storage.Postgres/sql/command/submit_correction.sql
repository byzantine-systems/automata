-- A correction entering the inbox. See $schema$.submit_correction.
SELECT
    s.command_id,
    s.seq,
    s.accepted,
    s.key_reused,
    s.predecessor_received_at
FROM
    $schema$.submit_correction (@machine_id, @entity_id, @idempotency_key, @chart_version, @event::jsonb, @effective_at, @on_divergence::$schema$.divergence, @replay_limit, @tenant, @principal, @source, @correlation_id, @causation_id) s;

