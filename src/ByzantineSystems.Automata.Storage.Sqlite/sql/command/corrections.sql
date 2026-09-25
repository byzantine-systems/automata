-- The correction details of the claimed commands that are corrections. @ids is
-- a JSON array of command ids, so one statement serves any batch size.
SELECT
    d.command_id,
    d.effective_at,
    d.on_divergence,
    d.replay_limit
FROM
    fsm_command_correction d
WHERE
    d.command_id IN (
        SELECT
            j.value
        FROM
            JSON_EACH(@ids) j);
