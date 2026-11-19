-- The raw fence was checked and the complete failure recorded in this write
-- transaction. Close under the same token before promoting the next head.
UPDATE fsm_command SET status = 'dead_letter'
WHERE command_id = @command_id AND lease_token = @lease_token AND status = 'leased';
