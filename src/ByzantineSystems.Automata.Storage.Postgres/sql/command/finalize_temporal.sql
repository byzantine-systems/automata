SELECT
    f.outcome,
    f.epoch,
    f.status
FROM
    $temporal_schema$.commit_command (@command_id, @lease_token, @expected_epoch, @status::$schema$.command_status, @error::jsonb, @instance_status::$schema$.instance_status, @effective_at, @actions::jsonb, @from_state::jsonb, @to_state::jsonb, @handled_by, @exited, @entered) f;
