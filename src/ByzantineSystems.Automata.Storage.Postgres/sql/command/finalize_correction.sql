-- Commits a replayed correction. See $schema$.finalize_correction.
SELECT
    f.outcome,
    f.epoch,
    f.status
FROM
    $schema$.finalize_correction (@command_id, @lease_token, @expected_epoch, @valid_from, @beliefs::jsonb, @instance_status::$schema$.instance_status, @event::jsonb, @from_state::jsonb, @to_state::jsonb, @handled_by, @exited, @entered) f;

