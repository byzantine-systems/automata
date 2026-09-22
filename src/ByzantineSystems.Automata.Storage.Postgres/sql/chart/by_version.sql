-- Reads the fingerprint a version is registered under, without claiming it.
--
-- A plain SELECT rather than a routine: routines are the mutation surface here,
-- and asking what is registered must not be a way of registering something.
SELECT
    c.fingerprint
FROM
    fsm.machine_chart_version c
WHERE
    c.machine_id = @machine_id
    AND c.version = @version;

