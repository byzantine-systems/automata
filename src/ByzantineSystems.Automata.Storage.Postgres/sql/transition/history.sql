-- One ascending page of an entity's transition log.
--
-- Ascending because this is a log rather than a feed: it is read to follow what
-- happened in the order it happened, and the epoch that orders it is gapless by
-- the primary key of fsm.transition rather than by convention.
--
-- The cursor is an exclusive epoch, not an offset. An offset shifts as rows
-- arrive behind it, so paging a growing log with one silently skips or repeats;
-- an epoch does not move, so a page pinned to one returns the same rows however
-- much has been committed since.
--
-- @after_epoch is 0 for a first page, which is below every stored epoch because
-- transition_epoch_positive forbids zero. That is what lets one query shape
-- serve both the first page and every later one, with no null and no branch.
--
-- (machine_id, entity_id, epoch) is transition_history_idx, so the predicate and
-- the ordering are both served by the index and neither needs a sort node.
--
-- The column list is written out rather than starred. This result is the
-- contract between the log and a reader that maps by name, and it is deliberately
-- the same shape sql/command/result.sql returns for a committed command, so one
-- decoder serves both.
SELECT
    t.machine_id,
    t.entity_id,
    t.epoch,
    t.command_id,
    t.chart_version,
    t.event,
    t.actions,
    t.from_state,
    t.to_state,
    t.handled_by,
    t.exited,
    t.entered,
    -- Aliased to match sql/command/result.sql, where the unqualified status is the
    -- command's rather than the instance's. One decoder serves both.
    t.status AS instance_status,
    t.effective_at,
    t.committed_at
FROM
    fsm.transition t
WHERE
    t.machine_id = @machine_id
    AND t.entity_id = @entity_id
    AND t.epoch > @after_epoch
ORDER BY
    t.epoch
LIMIT @limit;

