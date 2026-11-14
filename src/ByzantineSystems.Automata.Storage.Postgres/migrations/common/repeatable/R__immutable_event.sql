-- Joined transition history depends on the submitted event remaining immutable.
CREATE OR REPLACE FUNCTION $schema$.refuse_event_update()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    RAISE EXCEPTION 'a submitted command event is immutable' USING ERRCODE = 'integrity_constraint_violation';
END $$;
CREATE OR REPLACE TRIGGER command_event_is_immutable BEFORE UPDATE OF event ON $schema$.command
FOR EACH ROW EXECUTE FUNCTION $schema$.refuse_event_update();
