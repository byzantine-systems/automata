-- Records a delivery given up on, the business fact that an effect the machine
-- asked for never happened. Fenced: the copy is taken only from a row the
-- caller still holds, and zero rows inserted is lease_lost. complete.sql then
-- removes the queued row in the same transaction.
INSERT INTO fsm_action_dead_letter (machine_id, entity_id, command_id, epoch, ordinal, action, reason, deliveries, recorded_at)
SELECT
    a.machine_id,
    a.entity_id,
    a.command_id,
    a.epoch,
    a.ordinal,
    a.action,
    @reason,
    a.read_ct,
    @now
FROM
    fsm_action a
WHERE
    a.command_id = @command_id
    AND a.ordinal = @ordinal
    AND a.lease_token = @lease_token;
