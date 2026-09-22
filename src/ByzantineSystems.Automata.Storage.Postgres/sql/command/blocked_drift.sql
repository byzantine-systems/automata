-- Detects drift in the derived blocked column. Empty means healthy.
--
-- This runs on the maintenance tick and alerts on any non-empty result. The
-- repair is a separate, opt-in call (fsm.repair_blocked), because silently
-- rewriting derived state hides whatever caused it to drift.
--
-- The truth is recomputed from first principles rather than from the column
-- being checked: within each entity, exactly the earliest open command should
-- be unblocked. The window runs after WHERE, so min(seq) is the minimum over
-- open siblings only, which is the definition wanted.
--
-- Cost is proportional to open commands, not to retained history.
WITH open_command AS (
    SELECT
        c.command_id,
        c.machine_id,
        c.entity_id,
        c.seq,
        c.blocked,
        c.seq > MIN(c.seq) OVER (PARTITION BY c.machine_id,
            c.entity_id) AS expected_blocked
    FROM
        fsm.command c
    WHERE
        c.status IN ('ready', 'leased'))
SELECT
    o.command_id,
    o.machine_id,
    o.entity_id,
    o.seq,
    o.blocked,
    o.expected_blocked
FROM
    open_command o
WHERE
    o.blocked IS DISTINCT FROM o.expected_blocked
ORDER BY
    o.machine_id,
    o.entity_id,
    o.seq;

