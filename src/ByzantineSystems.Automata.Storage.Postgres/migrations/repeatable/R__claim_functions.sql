-- ---------------------------------------------------------------------------
-- Leased claim functions. Repeatable so the lease logic versions with the code
-- rather than living in a string literal. FOR UPDATE SKIP LOCKED lets any number
-- of pump instances poll without double-claiming. locked_until = '-infinity'
-- means "not leased"; a finite value is the lease deadline.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fsm.claim_retries (p_batch integer, p_lease interval, p_now timestamptz)
    RETURNS SETOF fsm.retry_queue
    LANGUAGE sql
    AS $$
    WITH candidate AS MATERIALIZED (
        SELECT
            id
        FROM
            fsm.retry_queue
        WHERE
            next_retry_at <= p_now
            AND locked_until < p_now
        ORDER BY
            next_retry_at,
            id
        LIMIT p_batch
        FOR UPDATE
            SKIP LOCKED)
UPDATE
    fsm.retry_queue AS q
SET
    locked_until = p_now + p_lease,
    attempts = attempts + 1
FROM
    candidate AS c
WHERE
    q.id = c.id
RETURNING
    q.*;
$$;

CREATE OR REPLACE FUNCTION fsm.claim_outbox (p_batch integer, p_lease interval, p_now timestamptz)
    RETURNS SETOF fsm.outbox
    LANGUAGE sql
    AS $$
    WITH candidate AS MATERIALIZED (
        SELECT
            machine_id,
            entity_id,
            event_idempotency_key,
            ordinal
        FROM
            fsm.outbox
        WHERE
            next_attempt_at <= p_now
            AND locked_until < p_now
        ORDER BY
            next_attempt_at,
            ordinal
        LIMIT p_batch
        FOR UPDATE
            SKIP LOCKED)
UPDATE
    fsm.outbox AS o
SET
    locked_until = p_now + p_lease,
    attempts = attempts + 1
FROM
    candidate AS c
WHERE
    o.machine_id = c.machine_id
    AND o.entity_id = c.entity_id
    AND o.event_idempotency_key = c.event_idempotency_key
    AND o.ordinal = c.ordinal
RETURNING
    o.*;
$$;

