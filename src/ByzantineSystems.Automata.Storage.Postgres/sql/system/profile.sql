SELECT
    beliefs,
    TO_REGCLASS('$schema$.snapshot_schemaversions') IS NOT NULL
    AND TO_REGPROC('$schema$.commit_command') IS NOT NULL AS snapshot_installed,
    TO_REGCLASS('$temporal_schema$.schemaversions') IS NOT NULL
    AND TO_REGPROCEDURE('$temporal_schema$.close_and_open(text,text,timestamptz,jsonb,$schema$.instance_status,bigint,bigint,integer)') IS NOT NULL
    AND TO_REGPROC('$temporal_schema$.commit_command') IS NOT NULL
    AND TO_REGPROC('$temporal_schema$.commit_correction') IS NOT NULL AS temporal_installed
FROM
    $schema$.store_profile
WHERE
    singleton;

