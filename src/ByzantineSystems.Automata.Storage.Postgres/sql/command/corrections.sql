-- Correction details for the claimed command ids, fetched in the claim's session.
SELECT
    d.command_id,
    d.effective_at,
    d.on_divergence,
    d.replay_limit
FROM
    fsm.command_correction d
WHERE
    d.command_id = ANY (@ids);

