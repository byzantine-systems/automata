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

