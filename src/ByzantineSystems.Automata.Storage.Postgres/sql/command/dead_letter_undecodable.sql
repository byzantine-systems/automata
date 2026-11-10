-- The raw id and token suffice; no domain payload or entity id needs decoding.
SELECT f.outcome
FROM fsm.finalize_command(p_command_id => @command_id, p_lease_token => @lease_token,
    p_expected_epoch => 0, p_status => 'dead_letter',
    p_error => jsonb_build_object('machine', @reason::text)) f;
