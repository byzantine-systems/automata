-- Joined transition history and request identity depend on submitted fields
-- remaining immutable. Retain the existing trigger identity when extending it.
CREATE OR REPLACE FUNCTION $schema$.refuse_event_update ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    SET search_path = pg_catalog, pg_temp
    AS $$
BEGIN
    RAISE EXCEPTION 'a submitted command is immutable'
        USING ERRCODE = 'integrity_constraint_violation';
END
$$;

CREATE OR REPLACE TRIGGER command_event_is_immutable
    BEFORE UPDATE OF command_id,
    machine_id,
    entity_id,
    seq,
    idempotency_key,
    request_digest,
    chart_version,
    event,
    kind,
    received_at,
    tenant,
    principal,
    source,
    correlation_id,
    causation_id ON $schema$.command
    FOR EACH ROW
    EXECUTE FUNCTION $schema$.refuse_event_update ();

