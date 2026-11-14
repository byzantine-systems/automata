CREATE OR REPLACE FUNCTION $temporal_schema$.purge_belief_history (p_machine text, p_before timestamptz, p_batch integer)
    RETURNS bigint
    LANGUAGE sql
    VOLATILE PARALLEL UNSAFE
    SET search_path = pg_catalog, pg_temp
    AS $$
    WITH doomed AS MATERIALIZED (
        SELECT
            h.ctid
        FROM
            $temporal_schema$.instance_state_history h
        WHERE
            h.machine_id = $1
            AND UPPER(
                h.system_time
) < $2
        ORDER BY
            UPPER(
                h.system_time
),
            h.ctid
        LIMIT $3
        FOR UPDATE
            SKIP LOCKED
),
    purged AS (
        DELETE FROM $temporal_schema$.instance_state_history h USING doomed d
WHERE h.ctid = d.ctid
RETURNING
    1
)
SELECT
    COUNT(*)
FROM
    purged;
$$;

