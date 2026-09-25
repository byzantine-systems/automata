-- The migration scripts DbUp has applied, by their embedded resource name.
--
-- Compared at boot against the scripts this build embeds. A script embedded
-- and not applied means the database is behind the code; one applied and not
-- embedded means an older binary is pointed at a newer schema. Both refuse to
-- boot, because both end in a write the other side does not understand.
--
-- Only the numbered scripts are journaled. The repeatable ones run on every
-- migration and leave no row, which is why system/boot_check.sql probes a
-- routine for them instead.
SELECT
    j.scriptname
FROM
    public.schemaversions j
ORDER BY
    j.scriptname;

