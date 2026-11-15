-- One tick of wake-ups. See $schema$.notify_pending for why this is a tick and
-- never a trigger.
--
-- The queue's usage rides along because this is the statement that fills it.
-- A full notification queue fails every commit that notifies, and the server
-- only warns about it in its own log, at half full; reporting it here lets the
-- application warn at the same point, where somebody will see it.
SELECT
    $schema$.notify_pending () AS sent,
    PG_NOTIFICATION_QUEUE_USAGE () AS queue_usage;

