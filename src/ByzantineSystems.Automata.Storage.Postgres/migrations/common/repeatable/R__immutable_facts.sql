CREATE OR REPLACE FUNCTION $schema$.refuse_fact_update ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    VOLATILE
    SET search_path = pg_catalog, pg_temp
    AS $$
BEGIN
    RAISE EXCEPTION 'an audit fact or submitted correction is immutable'
        USING ERRCODE = '23514', CONSTRAINT = 'fact_is_immutable';
END
$$;

DO $$
DECLARE
    fact text;
BEGIN
    FOREACH fact IN ARRAY ARRAY['transition', 'command_error', 'command_error_domain', 'command_error_machine', 'command_error_replay', 'command_correction', 'action_dead_letter', 'action_quarantine', 'supervision_event'] LOOP
        EXECUTE FORMAT('CREATE OR REPLACE TRIGGER %I BEFORE UPDATE ON %I.%I FOR EACH ROW EXECUTE FUNCTION %I.refuse_fact_update()', fact || '_is_append_only', '$schema$', fact, '$schema$');
    END LOOP;
END
$$;

CREATE OR REPLACE FUNCTION $schema$.guard_correction_owner ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    VOLATILE
    SET search_path = pg_catalog, $schema$, pg_temp
    AS $$
DECLARE
    owner_row $schema$.command;
BEGIN
    SELECT
        *
    INTO
        owner_row
    FROM
        $schema$.command
    WHERE
        command_id = NEW.command_id
    FOR NO KEY UPDATE;
    IF FOUND AND (owner_row.kind <> 'correction' OR owner_row.status <> 'ready') THEN
        RAISE EXCEPTION 'correction details belong to a ready correction'
            USING ERRCODE = '23514', CONSTRAINT = 'command_correction_ready_owner';
    END IF;
    RETURN NEW;
END
$$;

CREATE OR REPLACE TRIGGER command_correction_requires_ready_owner
    BEFORE INSERT ON $schema$.command_correction
    FOR EACH ROW
    EXECUTE FUNCTION $schema$.guard_correction_owner ();

