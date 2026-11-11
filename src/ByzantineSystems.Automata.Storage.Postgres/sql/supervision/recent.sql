-- Newest supervision audit facts first, matching supervision_event_recent_idx.
SELECT
    supervisor,
    child_id,
    kind,
    strategy,
    reason,
    at
FROM
    fsm.supervision_event
ORDER BY
    at DESC,
    id DESC
LIMIT @limit;

