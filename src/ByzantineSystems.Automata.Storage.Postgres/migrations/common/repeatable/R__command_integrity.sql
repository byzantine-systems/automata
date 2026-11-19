-- Row constraints validate the new shape; these guards validate the lifecycle
-- and its cross-table evidence. Functions and attachments live together so
-- reapplying repeatables reinstalls the same integrity boundary.
CREATE OR REPLACE FUNCTION $schema$.guard_command_lifecycle ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    VOLATILE
    SET search_path = pg_catalog, $schema$, pg_temp
    AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.status <> 'ready' THEN
            RAISE EXCEPTION 'a new command must be ready'
                USING ERRCODE = '23514', CONSTRAINT = 'command_starts_ready';
        END IF;
    ELSE
        IF OLD.status IN ('succeeded', 'rejected', 'dead_letter') THEN
            RAISE EXCEPTION 'a terminal command is never updated'
                USING ERRCODE = '23514', CONSTRAINT = 'command_terminal_is_final';
        END IF;
        IF OLD.status IS DISTINCT FROM NEW.status AND NOT ((OLD.status = 'ready' AND NEW.status = 'leased') OR (OLD.status = 'leased' AND NEW.status = 'ready') OR (OLD.status IN ('ready', 'leased') AND NEW.status IN ('succeeded', 'rejected', 'dead_letter'))) THEN
            RAISE EXCEPTION 'illegal command lifecycle edge'
                USING ERRCODE = '23514', CONSTRAINT = 'command_lifecycle_edge';
        END IF;
    END IF;
    RETURN NEW;
END
$$;

CREATE OR REPLACE TRIGGER command_starts_ready
    AFTER INSERT ON $schema$.command
    FOR EACH ROW
    EXECUTE FUNCTION $schema$.guard_command_lifecycle ();

CREATE OR REPLACE TRIGGER command_terminal_is_final
    BEFORE UPDATE ON $schema$.command
    FOR EACH ROW
    EXECUTE FUNCTION $schema$.guard_command_lifecycle ();

CREATE OR REPLACE FUNCTION $schema$.guard_command_closure ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    VOLATILE
    SET search_path = pg_catalog, $schema$, pg_temp
    AS $$
BEGIN
    IF NEW.status = 'succeeded' AND EXISTS (
        SELECT
            1
        FROM
            $schema$.command_error
        WHERE
            command_id = NEW.command_id) THEN
        RAISE EXCEPTION 'success cannot carry failure evidence'
            USING ERRCODE = '23514', CONSTRAINT = 'command_success_has_no_error';
    END IF;
    IF NEW.status IN ('rejected', 'dead_letter') AND NOT EXISTS (
        SELECT
            1
        FROM
            $schema$.command_error h
        WHERE
            h.command_id = NEW.command_id AND CASE h.kind
            WHEN 'domain' THEN
                EXISTS (
                    SELECT
                        1
                    FROM
                        $schema$.command_error_domain d
                    WHERE
                        d.command_id = h.command_id
                        AND d.kind = h.kind)
            WHEN 'machine' THEN
                EXISTS (
                    SELECT
                        1
                    FROM
                        $schema$.command_error_machine m
                    WHERE
                        m.command_id = h.command_id
                        AND m.kind = h.kind)
            WHEN 'replay' THEN
                EXISTS (
                    SELECT
                        1
                    FROM
                        $schema$.command_error_replay r
                    WHERE
                        r.command_id = h.command_id
                        AND r.kind = h.kind)
            ELSE
                FALSE
            END) THEN
    RAISE EXCEPTION 'failure requires its header and selected child'
            USING ERRCODE = '23514', CONSTRAINT = 'command_failure_is_complete';
    END IF;
    RETURN NEW;
END
$$;

CREATE OR REPLACE TRIGGER command_closure_has_evidence
    AFTER UPDATE ON $schema$.command
    FOR EACH ROW
    WHEN (NEW.status IN ('succeeded', 'rejected', 'dead_letter'))
    EXECUTE FUNCTION $schema$.guard_command_closure ();

-- FK KEY SHARE alone does not serialize with an UPDATE of status. Take the
-- same row lock as closure before observing the status, otherwise a writer
-- could insert evidence after a concurrent successful closure checked it.
CREATE OR REPLACE FUNCTION $schema$.guard_error_owner ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    VOLATILE
    SET search_path = pg_catalog, $schema$, pg_temp
    AS $$
DECLARE
    owner_status text;
BEGIN
    SELECT
        status
    INTO
        owner_status
    FROM
        $schema$.command
    WHERE
        command_id = NEW.command_id
    FOR NO KEY UPDATE;
    IF FOUND AND owner_status NOT IN ('ready', 'leased') THEN
        RAISE EXCEPTION 'failure evidence requires a live command'
            USING ERRCODE = '23514', CONSTRAINT = 'command_error_live_owner';
    END IF;
    RETURN NEW;
END
$$;

CREATE OR REPLACE TRIGGER command_error_requires_live_owner
    BEFORE INSERT ON $schema$.command_error
    FOR EACH ROW
    EXECUTE FUNCTION $schema$.guard_error_owner ();

-- The root has already been removed when FK cascades visit these facts. Do
-- not acquire a parent lock here: every surviving owner forbids deletion,
-- irrespective of status, and child->parent locking would invert purge order.
CREATE OR REPLACE FUNCTION $schema$.refuse_owned_delete ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    VOLATILE
    SET search_path = pg_catalog, $schema$, pg_temp
    AS $$
BEGIN
    IF EXISTS (
        SELECT
            1
        FROM
            $schema$.command
        WHERE
            command_id = OLD.command_id) THEN
    RAISE EXCEPTION 'delete the owning command, not its evidence or request'
        USING ERRCODE = '23514', CONSTRAINT = 'command_owns_facts';
END IF;
    RETURN OLD;
END
$$;

DO $$
DECLARE
    fact text;
BEGIN
    FOREACH fact IN ARRAY ARRAY['command_error', 'command_error_domain', 'command_error_machine', 'command_error_replay', 'command_correction'] LOOP
        EXECUTE FORMAT('CREATE OR REPLACE TRIGGER %I BEFORE DELETE ON %I.%I FOR EACH ROW EXECUTE FUNCTION %I.refuse_owned_delete()', fact || '_owned_delete', '$schema$', fact, '$schema$');
    END LOOP;
END
$$;

