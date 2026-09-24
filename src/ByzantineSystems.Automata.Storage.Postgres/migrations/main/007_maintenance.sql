-- ---------------------------------------------------------------------------
-- fsm.machine_maintenance: which machines this database maintains, and how.
--
-- Notification, retention and lease reaping all need the same answer to
-- "which machines and queues live here", and a registry is cheaper to ask than
-- the tables it describes. fsm.notify_pending probes one index per registered
-- machine rather than grouping the whole inbox; fsm.run_maintenance knows each
-- machine's retention without the application being present to say it. That
-- second property is what lets pg_cron run maintenance on its own.
--
-- Each machine writes its own row when it boots, so the policy lives with the
-- machine and the execution lives with the database. The last machine to boot
-- decides its own row, and nobody else's.
--
-- No column is nullable. 'infinity' is how "keep forever" is written, which an
-- interval can say from PostgreSQL 17 onward, and a comparison against
-- now() - 'infinity' is simply never true.
-- ---------------------------------------------------------------------------
CREATE TABLE fsm.machine_maintenance (
    machine_id text PRIMARY KEY,
    action_queue text NOT NULL,
    keep_commands interval NOT NULL DEFAULT 'infinity',
    keep_belief_history interval NOT NULL DEFAULT 'infinity',
    keep_action_archive interval NOT NULL DEFAULT 'infinity',
    -- How long past its expiry a lease is left alone before it is reaped. Not
    -- zero, because fsm.extend_lease deliberately accepts a lease that expired
    -- and was not reclaimed, and reaping at the instant of expiry would take
    -- that promise back.
    reap_after interval NOT NULL DEFAULT '5 minutes',
    registered_at timestamptz NOT NULL DEFAULT STATEMENT_TIMESTAMP(),
    -- The same allowlist as fsm.assert_queue_name, stated inline because a
    -- CHECK cannot call a routine the repeatable scripts have not created yet.
    -- The queue reaches dynamic SQL as an identifier, so it is checked on the
    -- way in as well as on the way out.
    CONSTRAINT machine_maintenance_queue_name CHECK (action_queue ~ '^[a-z_][a-z0-9_]*$'),
    CONSTRAINT machine_maintenance_keep_positive CHECK (keep_commands > interval '0' AND keep_belief_history > interval '0' AND keep_action_archive > interval '0'),
    CONSTRAINT machine_maintenance_reap_after_positive CHECK (reap_after > interval '0')
);

COMMENT ON TABLE fsm.machine_maintenance IS 'One row per machine this database maintains, written by the machine on boot. Read by fsm.notify_pending and fsm.run_maintenance, so that pg_cron can maintain a machine whose application is not running.';

