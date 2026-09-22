-- The current belief about an entity.
--
-- upper(valid_during) = 'infinity' is how a row says it is current, and it is
-- written as that comparison rather than as upper_inf(valid_during) so that
-- instance_state_live_idx can serve it: a partial index is only usable when the
-- query's predicate matches the index's own.
SELECT
    s.machine_id,
    s.entity_id,
    s.state,
    s.valid_during,
    s.system_time
FROM
    fsm.instance_state s
WHERE
    s.machine_id = @machine_id
    AND s.entity_id = @entity_id
    AND UPPER(s.valid_during) = 'infinity';

