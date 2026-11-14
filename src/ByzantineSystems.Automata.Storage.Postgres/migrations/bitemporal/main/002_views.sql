CREATE MATERIALIZED VIEW $temporal_schema$.belief_cold AS
SELECT
    h.*,
    c.cutoff
FROM
    $temporal_schema$.instance_state_history h
    CROSS JOIN (
        SELECT
            (DATE_TRUNC('day', NOW() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC') - interval '1 day' AS cutoff) c
WHERE
    UPPER(h.system_time) < c.cutoff;

CREATE UNIQUE INDEX belief_cold_key ON $temporal_schema$.belief_cold (machine_id, entity_id, valid_during, system_time);

CREATE INDEX belief_cold_cutoff_idx ON $temporal_schema$.belief_cold (cutoff);

CREATE VIEW $temporal_schema$.belief AS
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
        s.*
    FROM
        $temporal_schema$.instance_state s
    UNION ALL
    SELECT
        h.*
    FROM
        $temporal_schema$.instance_state_history h
    WHERE
        UPPER(h.system_time) >= (
            SELECT
                COALESCE(MAX(c.cutoff), '-infinity')
            FROM
                $temporal_schema$.belief_cold c)
            AND UPPER(h.system_time) >= NOW() - COALESCE((
                SELECT
                    m.keep_belief_history
                FROM $schema$.machine_maintenance m
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
            $temporal_schema$.belief_cold c
        WHERE
            UPPER(c.system_time) >= NOW() - COALESCE((
                SELECT
                    m.keep_belief_history
                FROM $schema$.machine_maintenance m
            WHERE
                m.machine_id = c.machine_id), 'infinity')) b;

CREATE VIEW $temporal_schema$.current_belief AS
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
    $temporal_schema$.instance_state s
WHERE
    UPPER(s.valid_during) = 'infinity';

