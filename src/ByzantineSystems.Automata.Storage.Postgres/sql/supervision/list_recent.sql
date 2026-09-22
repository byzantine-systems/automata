-- Newest supervision facts first. id breaks ties so that two records written in
-- the same instant still have a deterministic order.
SELECT
    e.supervisor,
    e.child_id,
    e.kind,
    e.strategy,
    e.reason,
    e.at
FROM
    fsm.supervision_event e
ORDER BY
    e.at DESC,
    e.id DESC
LIMIT @limit;

