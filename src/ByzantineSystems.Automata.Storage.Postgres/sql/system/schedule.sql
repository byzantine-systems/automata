-- Asserts the two pg_cron jobs, when pg_cron is here and this role may use it.
-- Answers 'scheduled', 'unavailable' or 'not_permitted'; the last two mean the
-- application runs maintenance itself. See $schema$.schedule_maintenance.
SELECT
    $schema$.schedule_maintenance (@notify_every, @run_every, @batch) AS scheduling;

