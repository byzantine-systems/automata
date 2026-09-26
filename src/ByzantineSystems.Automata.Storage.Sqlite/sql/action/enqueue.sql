-- The actions a commit asked for, queued in the commit's own transaction. That
-- is the whole transactional-outbox property: the transition and its effects
-- commit together or not at all.
--
-- One row per element of @actions, a JSON array. The ordinal is the element's
-- position, and with the command id it is the identity a destination
-- deduplicates on.
--
-- The element is taken with @actions -> j.key, never json_each's value column.
-- value unquotes a JSON string and turns JSON null into SQL NULL, so an action
-- codec that emits a bare string would store text that is no longer JSON. The
-- -> operator always answers the element's JSON text. An empty array inserts
-- nothing.
INSERT INTO fsm_action (command_id, ordinal, machine_id, entity_id, epoch, action, visible_at, enqueued_at)
SELECT
    @command_id,
    j.key,
    @machine_id,
    @entity_id,
    @epoch,
    @actions -> j.key,
    @now,
    @now
FROM
    JSON_EACH(@actions) j;
