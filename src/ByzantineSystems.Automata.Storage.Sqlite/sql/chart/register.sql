-- Claims a chart version for a fingerprint, if nobody has.
--
-- An insert that does nothing on conflict, so the row count is the answer:
-- one row means this call registered the version, none means it was already
-- there and the caller reads what is stored. Writers are serialised, so the
-- read that follows cannot race another registration.
INSERT INTO fsm_machine_chart_version (machine_id, version, fingerprint, registered_at)
    VALUES (@machine_id, @version, @fingerprint, @now)
ON CONFLICT (machine_id, version)
    DO NOTHING;
