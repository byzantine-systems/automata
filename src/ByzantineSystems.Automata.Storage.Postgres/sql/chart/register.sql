-- Claims a chart version for a fingerprint, or reports what the stored one
-- says. Idempotent, and every process running the machine calls it at startup.
SELECT
    r.registration,
    r.stored_fingerprint
FROM
    fsm.register_chart_version (@machine_id, @version, @fingerprint) r;

