-- The previous sequence's arrival, read inside the write transaction.
SELECT c.received_at
FROM fsm_command c
WHERE c.machine_id = @machine_id AND c.entity_id = @entity_id
ORDER BY c.seq DESC
LIMIT 1;
