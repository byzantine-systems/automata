-- ---------------------------------------------------------------------------
-- fsm.finalize_command: the one atomic step.
--
-- A worker that loses its connection mid-finalize does not know whether it
-- committed. Without a defined rule it retries, is told the lease is lost, and
-- the command is redelivered and applied a second time. This routine gives that
-- retry a defined answer:
--
--   stored result | lease token           | outcome
--   none          | current               | finalized
--   exists        | this one              | already_finalized
--   exists        | another               | lease_lost
--   none          | stale                 | lease_lost
--
-- The table is already in the schema. fsm.command.status records the terminal
-- states, so "a result exists" is a column; command_lease_token_matches_status
-- forces a terminal row to carry a non-zero token, so "who wrote it" is a
-- column too. The four rows are two comparisons.
--
-- This is what makes an ambiguous outcome safe to retry, and why a cancelled
-- Npgsql command or a dropped socket can be treated as "unknown, try again".
--
-- It replaces fsm.ack_command, which got exactly this wrong: its update
-- required status = 'leased', so a second acknowledgement by the same token
-- reported lease_lost rather than the work already done.
--
-- Everything happens in the caller's transaction, so all of it commits or none
-- does. The order matters: the transition is appended before the belief moves,
-- so a belief that cannot be written leaves no transition claiming it did.
--
-- The actions are enqueued between the belief write and the inbox close, in
-- this same transaction. That is the whole transactional-outbox property with
-- no outbox table: pgmq.send is an ordinary insert into an ordinary table, so
-- it commits or rolls back with the transition it belongs to. A commit whose
-- effects were not queued, or queued effects for a commit that did not happen,
-- are both unrepresentable.
--
-- VOLATILE and PARALLEL UNSAFE because it writes.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fsm.finalize_command (p_command_id bigint, p_lease_token bigint, p_expected_epoch bigint, p_status fsm.command_status, p_action_queue text DEFAULT '', p_error jsonb DEFAULT NULL, p_state jsonb DEFAULT NULL, p_instance_status fsm.instance_status DEFAULT NULL, p_effective_at timestamptz DEFAULT NULL, p_event jsonb DEFAULT NULL, p_actions jsonb DEFAULT NULL, p_from_state jsonb DEFAULT NULL, p_to_state jsonb DEFAULT NULL, p_handled_by text DEFAULT NULL, p_exited text[] DEFAULT NULL, p_entered text[] DEFAULT NULL)
    RETURNS TABLE (
        outcome fsm.finalize_outcome,
        epoch bigint)
    LANGUAGE plpgsql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog,
    pg_temp
    AS $$
DECLARE
    v_machine_id text;
    v_entity_id text;
    v_status fsm.command_status;
    v_token bigint;
    v_chart_version integer;
    v_current_epoch bigint;
BEGIN
    IF p_status NOT IN ('succeeded', 'rejected', 'dead_letter') THEN
        RAISE EXCEPTION 'fsm.finalize_command: % is not a terminal status', p_status
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    -- A succeeded command carries a transition; the other two carry an error.
    -- Checking here keeps a caller from half-filling either shape.
    IF p_status = 'succeeded' AND (p_state IS NULL OR p_event IS NULL OR p_effective_at IS NULL) THEN
        RAISE EXCEPTION 'fsm.finalize_command: a succeeded command requires its transition'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_status <> 'succeeded' AND p_error IS NULL THEN
        RAISE EXCEPTION 'fsm.finalize_command: a % command requires an error', p_status
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    -- FOR UPDATE because everything below decides from these values and then
    -- writes. Without the lock, two finalizers could both read 'leased'.
    SELECT
        c.machine_id,
        c.entity_id,
        c.status,
        c.lease_token,
        c.chart_version
    INTO
        v_machine_id,
        v_entity_id,
        v_status,
        v_token,
        v_chart_version
    FROM
        fsm.command c
    WHERE
        c.command_id = p_command_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'fsm.finalize_command: no command %', p_command_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    -- Already finished. Whether this caller wrote it is the whole question: if
    -- it did, its retry is answered with the work it already did.
    IF v_status IN ('succeeded', 'rejected', 'dead_letter') THEN
        IF v_token = p_lease_token THEN
            RETURN QUERY
            SELECT
                'already_finalized'::fsm.finalize_outcome,
                COALESCE((
                    SELECT
                        t.epoch
                    FROM fsm.transition t
                    WHERE
                        t.command_id = p_command_id), p_expected_epoch);
        ELSE
            RETURN QUERY
            SELECT
                'lease_lost'::fsm.finalize_outcome,
                p_expected_epoch;
        END IF;
        RETURN;
    END IF;
    -- Still open, but held by somebody else. Note that an expired lease is not
    -- checked here: the token is the fence. A worker past its deadline whose
    -- command nobody has reclaimed still holds the only token, and rejecting it
    -- would discard work that is safe to commit. Once another worker claims the
    -- command, the token differs and this test catches it.
    IF v_token <> p_lease_token THEN
        RETURN QUERY
        SELECT
            'lease_lost'::fsm.finalize_outcome,
            p_expected_epoch;
        RETURN;
    END IF;
    -- The epoch check guards the state write, so it applies only when there is
    -- one. A rejection records that the chart refused the event; it does not
    -- depend on the state having stayed still, and failing it for that reason
    -- would report a conflict about something the call never touches.
    --
    -- It is a backstop rather than the mechanism. The head invariant already
    -- stops a second command for this entity being processed while this one is
    -- leased, so under the inbox path the epoch cannot move underneath a
    -- finalizer. What this catches is a writer outside that path.
    IF p_status = 'succeeded' THEN
        SELECT
            s.epoch
        INTO
            v_current_epoch
        FROM
            fsm.instance_state s
        WHERE
            s.machine_id = v_machine_id
            AND s.entity_id = v_entity_id
            AND UPPER(s.valid_during) = 'infinity';
        v_current_epoch := COALESCE(v_current_epoch, 0);
        IF v_current_epoch <> p_expected_epoch THEN
            RETURN QUERY
            SELECT
                'conflict'::fsm.finalize_outcome,
                v_current_epoch;
            RETURN;
        END IF;
        INSERT INTO fsm.transition (machine_id, entity_id, epoch, command_id, chart_version, event, actions, from_state, to_state, handled_by, exited, entered, status, effective_at)
            VALUES (v_machine_id, v_entity_id, p_expected_epoch + 1, p_command_id, v_chart_version, p_event, p_actions, p_from_state, p_to_state, p_handled_by, p_exited, p_entered, p_instance_status, p_effective_at);
        PERFORM
            fsm.close_and_open (v_machine_id, v_entity_id, p_effective_at, p_state, p_instance_status, p_expected_epoch + 1, p_command_id, v_chart_version);
        -- The actions this transition asked for, enqueued in this transaction.
        -- Ordinal is the position in the chart's action list, and together with
        -- the command id it is the identity a destination deduplicates on: a
        -- redelivery carries a new msg_id and the same pair.
        --
        -- An empty queue name means the caller has no delivery configured,
        -- which is how a machine whose chart emits nothing avoids requiring a
        -- queue to exist. Explicit casts throughout, because pgmq.send is
        -- overloaded and overload resolution happens at prepare time.
        IF p_action_queue <> '' AND p_actions IS NOT NULL AND jsonb_array_length(p_actions) > 0 THEN
            PERFORM
                fsm.assert_queue_name (p_action_queue);
            PERFORM
                pgmq.send (p_action_queue::text, jsonb_build_object('machine_id', v_machine_id, 'entity_id', v_entity_id, 'command_id', p_command_id, 'epoch', p_expected_epoch + 1, 'ordinal', a.ordinal - 1, 'action', a.value), 0::integer)
            FROM
                jsonb_array_elements(p_actions) WITH ORDINALITY AS a (value,
                    ordinal);
        END IF;
    ELSE
        INSERT INTO fsm.command_error (command_id, error)
            VALUES (p_command_id, p_error);
    END IF;
    UPDATE
        fsm.command c
    SET
        status = p_status
    WHERE
        c.command_id = p_command_id;
    -- Promote the entity's next command to head. This statement sees the
    -- terminal status written above, so the row just finished cannot be
    -- re-elected. A dead letter unblocks too: a command we have given up on
    -- must not wedge its entity forever.
    UPDATE
        fsm.command c
    SET
        blocked = FALSE
    WHERE
        c.command_id = (
            SELECT
                n.command_id
            FROM
                fsm.command n
            WHERE
                n.machine_id = v_machine_id
                AND n.entity_id = v_entity_id
                AND n.status IN ('ready', 'leased')
            ORDER BY
                n.seq
            LIMIT 1);
    RETURN QUERY
    SELECT
        'finalized'::fsm.finalize_outcome,
        CASE WHEN p_status = 'succeeded' THEN
            p_expected_epoch + 1
        ELSE
            p_expected_epoch
        END;
END
$$;

