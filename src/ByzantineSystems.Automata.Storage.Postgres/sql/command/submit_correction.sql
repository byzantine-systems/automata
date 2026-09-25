-- A correction entering the inbox. See fsm.submit_correction.
SELECT
    s.command_id,
    s.seq,
    s.accepted
FROM
    fsm.submit_correction (@machine_id, @entity_id, @idempotency_key, @chart_version, @event::jsonb, @effective_at, @on_divergence::fsm.divergence, @replay_limit, @tenant, @principal, @source, @correlation_id, @causation_id) s;

