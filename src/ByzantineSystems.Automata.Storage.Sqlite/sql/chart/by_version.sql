-- The fingerprint a version is registered under, or no row.
--
-- Also how a submission checks that the version it pins exists before it
-- writes: SQLite reports a foreign key failure without naming the constraint,
-- so the store asks first rather than interpreting the error.
SELECT
    v.fingerprint
FROM
    fsm_machine_chart_version v
WHERE
    v.machine_id = @machine_id
    AND v.version = @version;
