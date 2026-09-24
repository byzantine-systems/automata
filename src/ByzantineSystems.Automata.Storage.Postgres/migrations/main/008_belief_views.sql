-- ---------------------------------------------------------------------------
-- Belief reads as views, so they have a shape a code generator can type.
--
-- The tables keep their ranges; the views expose the bounds as scalar columns,
-- because SqlHydra drops tstzrange columns rather than mapping them. Because
-- both windows are half-open by CHECK (instance_state_valid_half_open,
-- instance_state_system_half_open), "lower <= t AND upper > t" means exactly
-- what "range @> t" meant, with no boundary case between them.
-- ---------------------------------------------------------------------------
-- ---------------------------------------------------------------------------
-- fsm.belief_cold: superseded beliefs old enough to never change again.
--
-- A history row is written once, when its belief is superseded, and only ever
-- deleted afterwards, by retention. Rows older than the cutoff are therefore
-- frozen, which is what lets them be materialized.
--
-- The cutoff is stored in every row rather than computed by the reader. The
-- boundary between this view and the live history must move exactly when this
-- view's content does, or rows fall between the two: a reader using the wall
-- clock would see midnight pass before the refresh landed, and lose a day of
-- beliefs for as long as the refresh took or kept failing. Stored here, the
-- boundary moves only when a refresh commits, so a late or failed refresh
-- makes reads slower and never wrong.
--
-- The cutoff trails midnight by a day. A row is archived with the
-- statement_timestamp() of the statement that superseded it, and that
-- statement's transaction can commit after a refresh has already run; a row
-- stamped before the cutoff but committed after the refresh would be in
-- neither layer. A day of margin covers any transaction shorter than a day.
--
-- Refreshed CONCURRENTLY, which needs the unique index below and cannot run
-- inside a function, so it is issued as its own statement: by a daily pg_cron
-- job when pg_cron runs maintenance, and by the application's maintenance pass
-- otherwise.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW fsm.belief_cold AS
SELECT
    h.machine_id,
    h.entity_id,
    h.state,
    h.status,
    h.epoch,
    h.command_id,
    h.chart_version,
    h.valid_during,
    h.system_time,
    c.cutoff
FROM
    fsm.instance_state_history h
    CROSS JOIN (
        SELECT
            DATE_TRUNC('day', NOW()) - interval '1 day' AS cutoff) c
WHERE
    UPPER(h.system_time) < c.cutoff;

-- What REFRESH ... CONCURRENTLY diffs by. One entity never holds two versions
-- with the same valid window over the same belief window.
CREATE UNIQUE INDEX belief_cold_key ON fsm.belief_cold (machine_id, entity_id, valid_during, system_time);

-- max(cutoff) is read on every belief query, so it has to be an index probe
-- rather than a scan of everything this view holds.
CREATE INDEX belief_cold_cutoff_idx ON fsm.belief_cold (cutoff);

-- Which cutoff the last refresh used, so the maintenance pass refreshes once a
-- day rather than on every tick. A hint and nothing more: a stale value costs
-- a refresh that finds nothing new, never a wrong read, because readers take
-- the boundary from fsm.belief_cold itself.
CREATE TABLE fsm.belief_cold_state (
    singleton boolean PRIMARY KEY DEFAULT TRUE,
    refreshed_for timestamptz NOT NULL,
    CONSTRAINT belief_cold_state_singleton CHECK (singleton)
);

-- ---------------------------------------------------------------------------
-- fsm.belief: every belief ever held, on both time axes.
--
-- Three branches, which never overlap:
--
--   * the live table, holding beliefs still held;
--   * the history newer than the cold cutoff, read live;
--   * the cold layer, holding history older than the cutoff.
--
-- The live history branch starts at max(cutoff), which is '-infinity' when the
-- cold layer is empty, so the two history branches meet exactly whatever the
-- refresh has or has not done yet.
--
-- Retention is applied here as well as by fsm.purge_belief_history. A purge is
-- batched and the cold layer refreshes once a day, so without this a belief
-- past its retention would stay visible until both caught up. Filtering by the
-- machine's keep_belief_history makes it invisible at the same instant for
-- every reader. A machine with no registration keeps everything.
-- ---------------------------------------------------------------------------
CREATE VIEW fsm.belief AS
SELECT
    b.machine_id,
    b.entity_id,
    b.state,
    b.status,
    b.epoch,
    b.command_id,
    b.chart_version,
    LOWER(b.valid_during) AS valid_from,
    UPPER(b.valid_during) AS valid_to,
    LOWER(b.system_time) AS known_from,
    UPPER(b.system_time) AS known_to
FROM (
    SELECT
        s.machine_id,
        s.entity_id,
        s.state,
        s.status,
        s.epoch,
        s.command_id,
        s.chart_version,
        s.valid_during,
        s.system_time
    FROM
        fsm.instance_state s
    UNION ALL
    SELECT
        h.machine_id,
        h.entity_id,
        h.state,
        h.status,
        h.epoch,
        h.command_id,
        h.chart_version,
        h.valid_during,
        h.system_time
    FROM
        fsm.instance_state_history h
    WHERE
        UPPER(h.system_time) >= (
            SELECT
                COALESCE(MAX(c.cutoff), '-infinity')
            FROM
                fsm.belief_cold c)
            AND UPPER(h.system_time) >= NOW() - COALESCE((
                SELECT
                    m.keep_belief_history
                FROM fsm.machine_maintenance m
            WHERE
                m.machine_id = h.machine_id), 'infinity')
        UNION ALL
        SELECT
            c.machine_id,
            c.entity_id,
            c.state,
            c.status,
            c.epoch,
            c.command_id,
            c.chart_version,
            c.valid_during,
            c.system_time
        FROM
            fsm.belief_cold c
        WHERE
            UPPER(c.system_time) >= NOW() - COALESCE((
                SELECT
                    m.keep_belief_history
                FROM fsm.machine_maintenance m
            WHERE
                m.machine_id = c.machine_id), 'infinity')) b;

-- ---------------------------------------------------------------------------
-- fsm.current_belief: the belief held now about what is true now.
--
-- The predicate is instance_state_live_idx's own, written the same way, so the
-- partial index still serves the runtime's most frequent read.
-- ---------------------------------------------------------------------------
CREATE VIEW fsm.current_belief AS
SELECT
    s.machine_id,
    s.entity_id,
    s.state,
    s.status,
    s.epoch,
    s.command_id,
    s.chart_version,
    LOWER(s.valid_during) AS valid_from,
    LOWER(s.system_time) AS known_from
FROM
    fsm.instance_state s
WHERE
    UPPER(s.valid_during) = 'infinity';

-- ---------------------------------------------------------------------------
-- fsm.blocked_drift: open commands whose derived blocked flag disagrees with
-- their siblings. Empty means healthy. See fsm.repair_blocked for the opt-in
-- repair, which is never run for you.
--
-- The truth is recomputed from first principles rather than from the column
-- being checked: within each entity exactly the earliest open command should
-- be unblocked. The window runs after WHERE, so min(seq) is the minimum over
-- open siblings only. Cost is proportional to open commands, not to history.
-- ---------------------------------------------------------------------------
CREATE VIEW fsm.blocked_drift AS
SELECT
    o.command_id,
    o.machine_id,
    o.entity_id,
    o.seq,
    o.blocked,
    o.expected_blocked
FROM (
    SELECT
        c.command_id,
        c.machine_id,
        c.entity_id,
        c.seq,
        c.blocked,
        c.seq > MIN(c.seq) OVER (PARTITION BY c.machine_id, c.entity_id) AS expected_blocked
    FROM
        fsm.command c
    WHERE
        c.status IN ('ready', 'leased')) o
WHERE
    o.blocked IS DISTINCT FROM o.expected_blocked;

