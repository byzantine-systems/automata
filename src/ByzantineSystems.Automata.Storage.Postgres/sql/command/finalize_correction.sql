-- Commits a replayed correction. See $schema$.finalize_correction.
SELECT
    f.outcome,
    f.epoch,
    f.status
FROM
    $temporal_schema$.commit_correction (@command_id, @lease_token, @expected_epoch, @valid_from, @states, @statuses, @valid_froms, @epochs, @command_ids, @chart_versions, @instance_status::$schema$.instance_status, @event::jsonb, @from_state::jsonb, @to_state::jsonb, @handled_by, @exited, @entered) f;
