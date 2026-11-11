-- The entity's current belief: the view's one open row. The view cannot carry NOT NULL, so the
-- reader requires each column on the F# side rather than assuming it.
SELECT
    machine_id,
    entity_id,
    state,
    status,
    epoch,
    command_id,
    chart_version,
    valid_from,
    known_from
FROM
    fsm.current_belief
WHERE
    machine_id = @machine_id
    AND entity_id = @entity_id;

