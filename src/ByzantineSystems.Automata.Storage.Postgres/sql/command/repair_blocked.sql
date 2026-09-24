-- The opt-in repair for the drift sql/command/blocked_drift.sql detects.
--
-- Never run by the maintenance pass. A wrong blocked flag is a symptom of a bug
-- in submission or acknowledgement, and repairing it silently would hide the
-- bug; something has to decide to call this, and should log what it changed.
-- Returns one row per command it changed.
SELECT
    r.command_id,
    r.blocked
FROM
    fsm.repair_blocked () r;

