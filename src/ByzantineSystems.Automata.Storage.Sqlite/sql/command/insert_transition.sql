-- Appends one committed transition. Written before the snapshot moves, so a
-- snapshot that cannot be written leaves no transition claiming it did; the
-- transaction rolls both back either way.
INSERT INTO fsm_transition (machine_id, entity_id, epoch, command_id, chart_version, event, actions, from_state, to_state, handled_by, exited, entered, status, effective_at, committed_at)
    VALUES (@machine_id, @entity_id, @epoch, @command_id, @chart_version, @event, @actions, @from_state, @to_state, @handled_by, @exited, @entered, @status, @effective_at, @now);
