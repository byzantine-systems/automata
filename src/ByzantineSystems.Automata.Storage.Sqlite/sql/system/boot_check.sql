-- What the file holds, before anything in it is trusted. Never fails for a
-- missing object: each answer is an EXISTS over the schema table.
--
-- The journal mode is read separately, with PRAGMA journal_mode, because it is
-- not available as a table-valued pragma function.
SELECT
    sqlite_version() AS version,
    EXISTS (
        SELECT
            1
        FROM
            sqlite_schema s
        WHERE
            s.type = 'table'
            AND s.name = 'SchemaVersions') AS has_journal,
    EXISTS (
        SELECT
            1
        FROM
            sqlite_schema s
        WHERE
            s.type = 'table'
            AND s.name LIKE 'fsm\_%' ESCAPE '\') AS has_schema;
