-- Appends one committed transition. Written before the snapshot moves, so a
-- snapshot that cannot be written leaves no transition claiming it did; the
-- transaction rolls both back either way.
-- This ordering is load-bearing: the origin is read from the OLD snapshot, before upsert.
INSERT INTO fsm_transition (machine_id, entity_id, epoch, command_id, chart_version, actions, from_state, to_state, handled_by, exited, entered, status, effective_at, committed_at)
    VALUES (@machine_id, @entity_id, @epoch, @command_id, @chart_version, @actions,
        CASE WHEN (@epoch = 1 AND @from_state IS NOT NULL) OR (@epoch > 1 AND @from_state IS NULL)
            THEN COALESCE((SELECT state FROM fsm_entity_snapshot WHERE machine_id = @machine_id AND entity_id = @entity_id), @from_state) END,
        @to_state, @handled_by, @exited, @entered, @status, @effective_at, @now);
