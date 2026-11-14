-- What a machine needs before it may serve, read from the catalog alone.
--
-- Nothing here references an object that might not exist. to_regnamespace,
-- to_regclass and to_regproc answer NULL for a missing name rather than
-- raising, and pg_extension always exists, so this runs, and answers, against
-- a database where nothing has been installed at all. A boot check that fails
-- with "relation does not exist" has told the operator less than one that says
-- which of these is missing.
--
-- The routine probe is a sentinel rather than an inventory. The repeatable
-- scripts are reapplied on every migration, so if the newest of them is present
-- the rest are too; a database with the schema but without it is one whose
-- migrations stopped part way. has_schema also refuses a name newly reserved
-- by the live server, while preserving the catalog result's existing shape.
--
-- The journal is only reported here, not read: reading it when it does not
-- exist would be the error this statement exists to avoid. system/journal.sql
-- reads it once this says it is there.
SELECT
    TO_REGNAMESPACE('$schema$') IS NOT NULL
    AND NOT EXISTS (
        SELECT
            1
        FROM
            PG_GET_KEYWORDS() k
        WHERE
            k.word = '$schema$'
            AND k.catcode = 'R') AS has_schema,
    TO_REGCLASS('$schema$.schemaversions') IS NOT NULL AS has_journal,
    TO_REGPROC('$schema$.run_maintenance') IS NOT NULL AS has_routines,
    EXISTS (
        SELECT
            1
        FROM
            pg_extension e
        WHERE
            e.extname = 'btree_gist') AS has_btree_gist,
    EXISTS (
        SELECT
            1
        FROM
            pg_extension e
        WHERE
            e.extname = 'pgmq') AS has_pgmq;
