-- ---------------------------------------------------------------------------
-- fsm.correct_beliefs: rewrite a belief timeline from one instant onward.
--
-- This is the only routine permitted to write behind the live belief.
-- fsm.close_and_open refuses that case and keeps refusing it: a forward commit
-- landing before the current belief is a bug, not a change of mind, and a
-- routine that quietly accepted both could not tell them apart. A correction
-- says so explicitly by calling this instead.
--
-- Nothing here decides anything. Which chart would have decided a state,
-- whether replaying a command against it still resolves, and what to do when it
-- does not are all questions the caller has already answered; this writes the
-- answer down. Keeping the policy out is what let this routine be written at
-- all, because those questions have no settled answers yet and this one does.
--
-- Superseded beliefs are archived rather than overwritten, and that falls out of
-- the trigger rather than being arranged here: fsm.temporal_versioning closes
-- OLD.system_time and copies the row into the history twin on UPDATE *and* on
-- DELETE. So a belief this routine removes is still answerable through an as-of
-- read at a known_at before the correction, which is the entire point. A
-- correction that erased what it replaced would leave an audit record claiming
-- the new answer had always been the answer.
--
-- Returns the number of beliefs superseded, which is what an operator checks
-- against what they expected to change. Zero is not a failure: correcting an
-- entity that had no belief over that range is how one is created out of order.
--
-- VOLATILE and PARALLEL UNSAFE because it writes.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fsm.correct_beliefs (p_machine_id text, p_entity_id text, p_valid_from timestamptz, p_beliefs jsonb)
    RETURNS integer
    LANGUAGE plpgsql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog, pg_temp
    AS $$
DECLARE
    v_truncated integer;
    v_deleted integer;
    v_previous timestamptz;
    v_belief jsonb;
    v_next timestamptz;
BEGIN
    IF JSONB_TYPEOF(p_beliefs) <> 'array' THEN
        RAISE EXCEPTION 'fsm.correct_beliefs: the beliefs must be a json array, not %', JSONB_TYPEOF(p_beliefs)
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    -- Ordering is checked before anything is written. A timeline that is not
    -- ascending cannot be given contiguous windows, and discovering that
    -- half way through would leave the entity with a partly rewritten history
    -- and no way to tell which half.
    v_previous := '-infinity'::timestamptz;
    FOR v_belief IN
    SELECT
        value
    FROM
        JSONB_ARRAY_ELEMENTS(p_beliefs)
        LOOP
            IF (v_belief ->> 'valid_from')::timestamptz < p_valid_from THEN
                RAISE EXCEPTION 'fsm.correct_beliefs: a belief at % precedes the corrected instant %', (v_belief ->> 'valid_from')::timestamptz, p_valid_from USING ERRCODE = 'invalid_parameter_value';
END IF;
    IF (v_belief ->> 'valid_from')::timestamptz <= v_previous THEN
        RAISE EXCEPTION 'fsm.correct_beliefs: the beliefs are not in ascending valid-time order at %', (v_belief ->> 'valid_from')::timestamptz
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    v_previous := (v_belief ->> 'valid_from')::timestamptz;
END LOOP;
    -- A belief that starts before the corrected instant and runs past it is
    -- truncated to end there rather than removed: the part of it before the
    -- correction was never in question. The trigger archives the untruncated
    -- version, so what it used to say is still readable.
    --
    -- One starting exactly at the corrected instant has nothing left to keep and
    -- is deleted with the rest; truncating it would produce an empty range, which
    -- instance_state_valid_half_open refuses.
    UPDATE
        fsm.instance_state s
    SET
        valid_during = TSTZRANGE(LOWER(s.valid_during), p_valid_from, '[)')
    WHERE
        s.machine_id = p_machine_id
        AND s.entity_id = p_entity_id
        AND LOWER(s.valid_during) < p_valid_from
        AND UPPER(s.valid_during) > p_valid_from;
    GET DIAGNOSTICS v_truncated = ROW_COUNT;
    DELETE FROM fsm.instance_state s
    WHERE s.machine_id = p_machine_id
        AND s.entity_id = p_entity_id
        AND LOWER(s.valid_during) >= p_valid_from;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    -- Each belief runs until the next one begins, and the last is left open.
    -- Deriving the upper bounds rather than accepting them is what makes a gap
    -- or an overlap in a corrected timeline unrepresentable: a caller cannot
    -- describe one, so no as-of query can ever land in it.
    FOR v_belief,
    v_next IN
    SELECT
        t.value,
        (LEAD(t.value) OVER (ORDER BY t.ordinal) ->> 'valid_from')::timestamptz
    FROM
        JSONB_ARRAY_ELEMENTS(p_beliefs)
    WITH ORDINALITY AS t (value, ordinal)
    LOOP
        INSERT INTO fsm.instance_state (machine_id, entity_id, state, status, epoch, command_id, chart_version, valid_during)
        VALUES
            (p_machine_id, p_entity_id, (v_belief -> 'state'),
                (v_belief ->> 'status')::fsm.instance_status, (v_belief ->> 'epoch')::bigint, (v_belief ->> 'command_id')::bigint, (v_belief ->> 'chart_version')::integer, TSTZRANGE((v_belief ->> 'valid_from')::timestamptz, COALESCE(v_next, 'infinity'::timestamptz), '[)'));
    END LOOP;
    RETURN v_truncated + v_deleted;
END
$$;

-- ---------------------------------------------------------------------------
-- fsm.submit_correction: a correction enters the inbox like any command.
--
-- Through fsm.submit_command, so idempotency, the gapless sequence and the
-- head invariant are exactly an ordinary command's: a correction waits behind
-- work already queued for its entity, and nothing queued after it runs first.
-- Only a newly accepted command is marked a correction and given its details;
-- a repeated key returns the original, whatever it was.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fsm.submit_correction (p_machine_id text, p_entity_id text, p_idempotency_key text, p_chart_version integer, p_event jsonb, p_effective_at timestamptz, p_on_divergence fsm.divergence, p_replay_limit integer, p_tenant text DEFAULT '', p_principal text DEFAULT '', p_source text DEFAULT '', p_correlation_id text DEFAULT '', p_causation_id text DEFAULT '')
    RETURNS TABLE (
        command_id bigint,
        seq bigint,
        accepted boolean)
    LANGUAGE plpgsql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog,
    pg_temp
    AS $$
DECLARE
    v_command_id bigint;
    v_seq bigint;
    v_accepted boolean;
BEGIN
    SELECT
        s.command_id,
        s.seq,
        s.accepted
    INTO
        v_command_id,
        v_seq,
        v_accepted
    FROM
        fsm.submit_command (p_machine_id, p_entity_id, p_idempotency_key, p_chart_version, p_event, NULL, NULL, p_tenant, p_principal, p_source, p_correlation_id, p_causation_id) s;
    IF v_accepted THEN
        UPDATE
            fsm.command c
        SET
            kind = 'correction'
        WHERE
            c.command_id = v_command_id;
        INSERT INTO fsm.command_correction (command_id, effective_at, on_divergence, replay_limit)
            VALUES (v_command_id, p_effective_at, p_on_divergence, p_replay_limit);
    END IF;
    RETURN QUERY
    SELECT
        v_command_id,
        v_seq,
        v_accepted;
END
$$;

-- ---------------------------------------------------------------------------
-- fsm.finalize_correction: commit a replayed correction, all or nothing.
--
-- The same fence and epoch check as fsm.finalize_command, then the three
-- writes a correction is: its own transition, with no actions, because no
-- historical action is re-emitted; the rewritten belief timeline, through
-- fsm.correct_beliefs, which archives what it replaces; and the closed command,
-- with the entity's next command promoted.
--
-- The replay ran outside this transaction. That is sound because the command
-- was the entity's head while it ran, so nothing else for the entity could
-- commit, and the epoch check here is the backstop that proves it.
--
-- The last belief carries the new epoch. fsm.finalize_command reads the live
-- belief's epoch as the entity's, so any other attribution would leave the next
-- ordinary command expecting an epoch that is already taken.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fsm.finalize_correction (p_command_id bigint, p_lease_token bigint, p_expected_epoch bigint, p_valid_from timestamptz, p_beliefs jsonb, p_instance_status fsm.instance_status, p_event jsonb, p_from_state jsonb, p_to_state jsonb, p_handled_by text, p_exited text[], p_entered text[])
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
    v_kind fsm.command_kind;
    v_token bigint;
    v_chart_version integer;
    v_current_epoch bigint;
BEGIN
    SELECT
        c.machine_id,
        c.entity_id,
        c.status,
        c.kind,
        c.lease_token,
        c.chart_version
    INTO
        v_machine_id,
        v_entity_id,
        v_status,
        v_kind,
        v_token,
        v_chart_version
    FROM
        fsm.command c
    WHERE
        c.command_id = p_command_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'fsm.finalize_correction: no command %', p_command_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF v_kind <> 'correction' THEN
        RAISE EXCEPTION 'fsm.finalize_correction: command % is not a correction', p_command_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
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
    IF v_token <> p_lease_token THEN
        RETURN QUERY
        SELECT
            'lease_lost'::fsm.finalize_outcome,
            p_expected_epoch;
        RETURN;
    END IF;
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
        VALUES (v_machine_id, v_entity_id, p_expected_epoch + 1, p_command_id, v_chart_version, p_event, '[]'::jsonb, p_from_state, p_to_state, p_handled_by, p_exited, p_entered, p_instance_status, p_valid_from);
    PERFORM
        fsm.correct_beliefs (v_machine_id, v_entity_id, p_valid_from, p_beliefs);
    UPDATE
        fsm.command c
    SET
        status = 'succeeded'
    WHERE
        c.command_id = p_command_id;
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
        p_expected_epoch + 1;
END
$$;

