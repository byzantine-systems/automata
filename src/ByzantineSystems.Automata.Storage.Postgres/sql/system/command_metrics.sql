-- One health row per machine. See fsm.command_metrics for what each bucket
-- means and why blocked is counted apart from runnable.
SELECT
    m.machine_id,
    m.runnable,
    m.in_flight,
    m.scheduled,
    m.blocked,
    m.succeeded,
    m.rejected,
    m.dead_lettered,
    m.oldest_runnable_age,
    m.oldest_unacked_age
FROM
    fsm.command_metrics () m;

