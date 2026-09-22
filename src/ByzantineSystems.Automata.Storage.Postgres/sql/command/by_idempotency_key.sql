-- Reads one command by the key the caller submitted it under.
--
-- Served by command_unique_idem. This is the lookup a caller makes when it
-- has lost the command id but still holds the idempotency key.
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
    c.machine_id = @machine_id
    AND c.entity_id = @entity_id
    AND c.idempotency_key = @idempotency_key;

