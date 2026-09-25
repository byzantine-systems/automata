-- Commits a replayed correction. See fsm.finalize_correction.
SELECT
    f.outcome,
    f.epoch
FROM
    fsm.finalize_correction (@command_id, @lease_token, @expected_epoch, @valid_from, @beliefs::jsonb, @instance_status::fsm.instance_status, @event::jsonb, @from_state::jsonb, @to_state::jsonb, @handled_by, @exited, @entered) f;

