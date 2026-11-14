-- Removes both pg_cron jobs by name. Anything tearing the schema down calls
-- this first, because cron.job rows outlive DROP SCHEMA.
SELECT
    $schema$.unschedule_maintenance () AS removed;

