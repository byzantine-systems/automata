-- Finishes a leased command: verify the lease and the expected epoch, append
-- the transition, advance the belief, record the result, close the inbox row.
-- All of it in one transaction, or none of it.
--
-- Idempotent on command_id. A retry after a dropped connection is answered with
-- what the first attempt did rather than with a lost lease.
--
-- The domain casts are explicit because Npgsql sends these as text, and leaving
-- the coercion to the server works until an overload appears beside it.
SELECT
    f.outcome,
    f.epoch
FROM
    fsm.finalize_command (@command_id, @lease_token, @expected_epoch, @status::fsm.command_status, @action_queue, @error::jsonb, @state::jsonb, @instance_status::fsm.instance_status, @effective_at, @event::jsonb, @actions::jsonb, @from_state::jsonb, @to_state::jsonb, @handled_by, @exited, @entered) f;

