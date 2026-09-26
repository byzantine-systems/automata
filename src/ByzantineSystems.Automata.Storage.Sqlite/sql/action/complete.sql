-- Removes a delivered action. Fenced: the row's key and the claim's token must
-- both match, so a worker whose lease was taken over, or whose delivery was
-- rescheduled, removes nothing. Zero rows deleted is lease_lost.
DELETE FROM fsm_action
WHERE command_id = @command_id
    AND ordinal = @ordinal
    AND lease_token = @lease_token;
