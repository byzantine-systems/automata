-- Fenced close before recording the reason and promoting the next head in this transaction.
UPDATE fsm_command SET status = 'dead_letter'
WHERE command_id = @command_id AND lease_token = @lease_token AND status = 'leased';
