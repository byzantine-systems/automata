-- Moves the entity's current state to the commit's.
--
-- An UPSERT, never INSERT OR REPLACE. REPLACE deletes the old row and inserts a
-- new one, which fires delete-side foreign key actions and hands the row a new
-- rowid; an update in place does neither.
INSERT INTO fsm_entity_snapshot (machine_id, entity_id, state, status, epoch, command_id, chart_version, effective_at, committed_at)
    VALUES (@machine_id, @entity_id, @state, @status, @epoch, @command_id, @chart_version, @effective_at, @now)
ON CONFLICT (machine_id, entity_id)
    DO UPDATE SET
        state = excluded.state,
        status = excluded.status,
        epoch = excluded.epoch,
        command_id = excluded.command_id,
        chart_version = excluded.chart_version,
        effective_at = excluded.effective_at,
        committed_at = excluded.committed_at;
