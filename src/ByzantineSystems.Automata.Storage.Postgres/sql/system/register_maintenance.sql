-- A machine announcing itself, and its retention, on boot. See
-- fsm.register_maintenance: an upsert, so a changed policy takes effect on the
-- next start and a stale registry heals on it.
--
-- The three retention periods arrive as text and are cast here, because
-- "keep forever" is the interval 'infinity' and a TimeSpan has no way to say
-- that. reap_after is always finite, so it binds as an interval directly.
SELECT
    fsm.register_maintenance (@machine_id, @action_queue, @keep_commands::interval, @keep_belief_history::interval, @keep_action_archive::interval, @reap_after) AS created;

