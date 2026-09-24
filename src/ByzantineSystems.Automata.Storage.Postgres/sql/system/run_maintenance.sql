-- One maintenance pass. No rows back means another host, or pg_cron, is
-- already running one, which is the single-flight answer rather than a
-- failure. See fsm.run_maintenance.
SELECT
    m.machine_id,
    m.reaped,
    m.purged_commands,
    m.purged_beliefs,
    m.purged_actions,
    m.drifted
FROM
    fsm.run_maintenance (@batch) m;

