-- Reads one command by its identity, for inspecting a submitted command's
-- progress without waiting for it.
SELECT
    c.command_id,
    c.machine_id,
    c.entity_id,
    c.seq,
    c.idempotency_key,
    c.chart_version,
    c.event,
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
    c.causation_id
FROM
    fsm.command c
WHERE
    c.command_id = @command_id;

