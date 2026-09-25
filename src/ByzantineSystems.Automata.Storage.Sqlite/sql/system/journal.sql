-- The migrations DbUp has applied, by the embedded resource name it journals.
-- Read only once boot_check.sql has said the journal exists.
SELECT
    j.ScriptName AS script_name
FROM
    SchemaVersions j
ORDER BY
    j.ScriptName;
