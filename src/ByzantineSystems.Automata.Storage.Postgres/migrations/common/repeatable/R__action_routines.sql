-- ---------------------------------------------------------------------------
-- Action delivery: fenced wrappers around pgmq.
--
-- pgmq gives us the right delivery semantics and no fencing at all. Its
-- archive() and set_vt() take a msg_id and ignore read_ct, so a worker that
-- stalled past its visibility timeout, woke up, and archived a message another
-- worker had already reclaimed is indistinguishable from the legitimate owner.
-- The effect is delivered twice and acknowledged once.
--
-- read_ct is the fencing token this design needs and pgmq already maintains:
-- every read increments it, so the value a worker was handed at claim time is
-- stale the moment anyone else reads the message. These routines compare it
-- before acting, which is the whole reason they exist. Nothing in F# calls
-- pgmq.archive or pgmq.set_vt directly.
--
-- Each takes the row lock first and acts second, in one transaction, so the
-- comparison cannot be overtaken between checking and acting.
-- ---------------------------------------------------------------------------
-- ---------------------------------------------------------------------------
-- $schema$.assert_queue_name: the allowlist, enforced where the interpolation is.
--
-- A queue name reaches dynamic SQL as an identifier rather than a parameter,
-- because pgmq's tables are named after the queue. The F# side checks the same
-- pattern before it ever sends one, and this checks again: the layer that does
-- the interpolating is the layer that cannot afford to assume.
--
-- Stricter than PostgreSQL's own identifier rules on purpose. This allows only
-- what never needs quoting, so there is no case where the quoting is the thing
-- keeping it safe.
--
-- IMMUTABLE and PARALLEL SAFE: it reads nothing.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION $schema$.assert_queue_name (p_queue text)
    RETURNS boolean
    LANGUAGE plpgsql
    IMMUTABLE PARALLEL SAFE
    SET search_path = pg_catalog, pg_temp
    AS $$
BEGIN
    IF p_queue IS NULL OR LENGTH(p_queue) > 47 OR p_queue !~ '^[a-z_][a-z0-9_]*$' THEN
        RAISE EXCEPTION '$schema$: % is not an acceptable queue name', p_queue
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    RETURN TRUE;
END
$$;

-- ---------------------------------------------------------------------------
-- $schema$.ensure_action_queue: create the queue if this is its first sighting.
--
-- pgmq.create is not idempotent, so the registry is consulted first rather than
-- catching duplicate_table, which would also swallow a genuine collision.
--
-- VOLATILE and PARALLEL UNSAFE because it creates tables.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION $schema$.ensure_action_queue (p_queue text)
    RETURNS boolean
    LANGUAGE plpgsql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog, pg_temp
    AS $$
DECLARE
    v_created boolean := FALSE;
    v_owner text;
    v_expected_owner constant text := 'automata:$schema$';
BEGIN
    PERFORM
        $schema$.assert_queue_name (p_queue);
    IF NOT EXISTS (
        SELECT
            1
        FROM
            pgmq.meta m
        WHERE
            m.queue_name = p_queue) THEN
    PERFORM
        pgmq.create (p_queue);
    v_created := TRUE;
END IF;
    SELECT
        OBJ_DESCRIPTION(TO_REGCLASS(FORMAT('pgmq.%I', 'q_' || p_queue)), 'pg_class')
    INTO
        v_owner;
    IF v_owner IS NOT NULL AND v_owner <> v_expected_owner THEN
        RAISE EXCEPTION 'queue % belongs to %, not $schema$', p_queue, v_owner
            USING ERRCODE = 'unique_violation', CONSTRAINT = 'automata_action_queue_owner';
    END IF;
    -- An existing uncommented queue predates schema ownership. Claiming it preserves its
    -- messages while making every later cross-schema use fail closed.
    EXECUTE FORMAT('COMMENT ON TABLE pgmq.%I IS %L', 'q_' || p_queue, v_expected_owner);
    RETURN v_created;
END
$$;

-- ---------------------------------------------------------------------------
-- $schema$.claim_actions: lease a batch for delivery.
--
-- A thin pass-through, because pgmq.read is already the right shape: ORDER BY
-- msg_id, WHERE vt <= clock_timestamp(), FOR UPDATE SKIP LOCKED against a (vt)
-- index. Contention-free and at-least-once.
--
-- No machine predicate, and none is needed: a queue belongs to one machine, so
-- everything in it is that machine's. pgmq could not express the predicate
-- anyway without scanning, which is why the queue is per machine.
--
-- read_ct in the result is the caller's fencing token for everything below.
--
-- VOLATILE and PARALLEL UNSAFE: reading a message leases it.
-- ---------------------------------------------------------------------------
-- A transport integer is allowed to fail without aborting the already claimed batch.
CREATE OR REPLACE FUNCTION $schema$.action_integer (p_value jsonb)
    RETURNS bigint
    LANGUAGE plpgsql
    IMMUTABLE PARALLEL SAFE
    SET search_path = pg_catalog, pg_temp
    AS $$
BEGIN
    IF JSONB_TYPEOF(p_value) IS DISTINCT FROM 'number' OR p_value::text !~ '^-?[0-9]+$' THEN
        RETURN NULL;
    END IF;
    RETURN p_value::text::bigint;
EXCEPTION
    WHEN numeric_value_out_of_range
        OR invalid_text_representation THEN
        RETURN NULL;
END
$$;

CREATE OR REPLACE FUNCTION $schema$.claim_actions (p_queue text, p_batch integer, p_lease interval)
    RETURNS TABLE (
        msg_id bigint,
        read_ct integer,
        vt timestamptz,
        machine_id text,
        entity_id text,
        command_id bigint,
        epoch bigint,
        ordinal integer,
        action jsonb)
    LANGUAGE plpgsql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog,
    pg_temp
    AS $$
BEGIN
    PERFORM
        $schema$.assert_queue_name (p_queue);
    RETURN QUERY
    SELECT
        r.msg_id,
        r.read_ct,
        r.vt,
        CASE WHEN JSONB_TYPEOF(r.message -> 'machine_id') = 'string' THEN
            r.message ->> 'machine_id'
        END,
        CASE WHEN JSONB_TYPEOF(r.message -> 'entity_id') = 'string' THEN
            r.message ->> 'entity_id'
        END,
        CASE WHEN $schema$.action_integer (r.message -> 'command_id') > 0 THEN
            $schema$.action_integer (r.message -> 'command_id')
        END,
        CASE WHEN $schema$.action_integer (r.message -> 'epoch') > 0 THEN
            $schema$.action_integer (r.message -> 'epoch')
        END,
        CASE WHEN $schema$.action_integer (r.message -> 'ordinal') BETWEEN 0 AND 2147483647 THEN
            $schema$.action_integer (r.message -> 'ordinal')::integer
        END,
        r.message -> 'action'
    FROM
        -- Explicit casts: pgmq.read is overloaded and resolution happens at
        -- prepare time, so an untyped argument picks the wrong one.
        pgmq.read (p_queue::text, EXTRACT(epoch FROM p_lease)::integer, p_batch::integer) r;
END
$$;

-- ---------------------------------------------------------------------------
-- $schema$.complete_action: archive a delivered message, fenced.
--
-- The SELECT ... FOR UPDATE is what makes this safe rather than merely checked.
-- It holds the row for the rest of the transaction, so no other worker can read
-- the message, increment read_ct and act on it between our comparison and our
-- archive.
--
-- VOLATILE and PARALLEL UNSAFE because it writes.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION $schema$.complete_action (p_queue text, p_msg_id bigint, p_read_ct integer)
    RETURNS $schema$.lease_outcome
    LANGUAGE plpgsql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog, pg_temp
    AS $$
DECLARE
    v_read_ct integer;
BEGIN
    PERFORM
        $schema$.assert_queue_name (p_queue);
    EXECUTE FORMAT('SELECT read_ct FROM pgmq.%I WHERE msg_id = $1 FOR UPDATE', 'q_' || p_queue) INTO v_read_ct
    USING p_msg_id;
    -- Absent means somebody already archived it; a different count means
    -- somebody else holds the lease now. Neither is ours to finish.
    IF v_read_ct IS NULL OR v_read_ct <> p_read_ct THEN
        RETURN 'lease_lost';
    END IF;
    PERFORM
        pgmq.archive (p_queue::text, p_msg_id::bigint);
    RETURN 'updated';
END
$$;

-- ---------------------------------------------------------------------------
-- $schema$.reschedule_action: push a failed delivery out, fenced.
--
-- The delay is computed by the same capped-exponential-with-jitter routine the
-- inbox uses, so a queue full of failures spreads rather than retrying in
-- lockstep.
--
-- VOLATILE and PARALLEL UNSAFE because it writes.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION $schema$.reschedule_action (p_queue text, p_msg_id bigint, p_read_ct integer, p_base_ms integer, p_cap_ms integer)
    RETURNS TABLE (
        outcome $schema$.lease_outcome,
        visible_at timestamptz)
    LANGUAGE plpgsql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog,
    pg_temp
    AS $$
DECLARE
    v_read_ct integer;
    v_delay interval;
BEGIN
    PERFORM
        $schema$.assert_queue_name (p_queue);
    EXECUTE FORMAT('SELECT read_ct FROM pgmq.%I WHERE msg_id = $1 FOR UPDATE', 'q_' || p_queue) INTO v_read_ct
    USING p_msg_id;
    IF v_read_ct IS NULL OR v_read_ct <> p_read_ct THEN
        RETURN QUERY
        SELECT
            'lease_lost'::$schema$.lease_outcome,
            '-infinity'::timestamptz;
        RETURN;
    END IF;
    -- read_ct counts deliveries, so it is the attempt number the backoff wants.
    v_delay := $schema$.retry_backoff (v_read_ct, p_base_ms, p_cap_ms);
    PERFORM
        pgmq.set_vt (p_queue::text, p_msg_id::bigint, EXTRACT(epoch FROM v_delay)::integer);
    RETURN QUERY
    SELECT
        'updated'::$schema$.lease_outcome,
        CLOCK_TIMESTAMP() + v_delay;
END
$$;

-- ---------------------------------------------------------------------------
-- $schema$.abandon_action: record the effect that never happened, then archive.
--
-- Fenced like the others, and the record is written before the archive so a
-- failure between them leaves the message in the queue rather than leaving the
-- fact unrecorded. A redelivery after that writes the same (command_id,
-- ordinal), which the primary key turns into a no-op rather than a duplicate.
--
-- VOLATILE and PARALLEL UNSAFE because it writes.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION $schema$.abandon_action (p_queue text, p_msg_id bigint, p_read_ct integer, p_reason text)
    RETURNS $schema$.lease_outcome
    LANGUAGE plpgsql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog, pg_temp
    AS $$
DECLARE
    v_read_ct integer;
    v_message jsonb;
    v_command bigint;
    v_epoch bigint;
    v_ordinal bigint;
BEGIN
    PERFORM
        $schema$.assert_queue_name (p_queue);
    EXECUTE FORMAT('SELECT read_ct, message FROM pgmq.%I WHERE msg_id = $1 FOR UPDATE', 'q_' || p_queue) INTO v_read_ct,
    v_message
    USING p_msg_id;
    IF v_read_ct IS NULL OR v_read_ct <> p_read_ct THEN
        RETURN 'lease_lost';
    END IF;
    v_command := $schema$.action_integer (v_message -> 'command_id');
    v_epoch := $schema$.action_integer (v_message -> 'epoch');
    v_ordinal := $schema$.action_integer (v_message -> 'ordinal');
    IF JSONB_TYPEOF(v_message -> 'machine_id') = 'string' AND JSONB_TYPEOF(v_message -> 'entity_id') = 'string' AND v_epoch > 0 AND v_ordinal BETWEEN 0 AND 2147483647 AND v_message -> 'action' IS NOT NULL AND EXISTS (
        SELECT
            1
        FROM
            $schema$.command c
        WHERE
            c.command_id = v_command AND c.machine_id = v_message ->> 'machine_id' AND c.entity_id = v_message ->> 'entity_id') THEN
        INSERT INTO $schema$.action_dead_letter (machine_id, entity_id, command_id, epoch, ordinal, action, reason, deliveries)
        VALUES
            ((v_message ->> 'machine_id'),
                (v_message ->> 'entity_id'), v_command, v_epoch, v_ordinal::integer, (v_message -> 'action'), p_reason, v_read_ct)
        ON CONFLICT (command_id, ordinal)
            DO NOTHING;
    ELSE
        INSERT INTO $schema$.action_quarantine (queue, msg_id, message, reason, deliveries)
            VALUES (p_queue, p_msg_id, v_message, p_reason, v_read_ct)
        ON CONFLICT (queue, msg_id)
            DO NOTHING;
    END IF;
    PERFORM
        pgmq.archive (p_queue::text, p_msg_id::bigint);
    RETURN 'updated';
END
$$;

