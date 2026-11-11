-- Leases up to @batch runnable commands for one machine.
--
-- The column list is written out rather than using a star. This result is the
-- contract between the routine and the reader, which maps by name, and an
-- explicit list means adding a column to fsm.command cannot silently change
-- what this query returns.
SELECT
    c.command_id,
    c.machine_id,
    c.entity_id,
    c.seq,
    c.idempotency_key,
    c.chart_version,
    c.event,
    c.request_digest,
    c.kind,
    c.status,
    c.blocked,
    c.visible_at,
    c.lease_token,
    c.read_ct,
    c.attempts,
    c.received_at,
    c.tenant,
    c.principal,
    c.source,
    c.correlation_id,
    c.causation_id,
    d.command_id AS correction_command_id,
    d.effective_at AS correction_effective_at,
    d.on_divergence AS correction_on_divergence,
    d.replay_limit AS correction_replay_limit
FROM
    fsm.claim_commands (@machine_id, @batch, @lease::interval) c
    LEFT JOIN fsm.command_correction d ON d.command_id = c.command_id;

