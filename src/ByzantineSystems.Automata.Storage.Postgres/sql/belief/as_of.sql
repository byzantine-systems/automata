-- The belief valid at one business instant as it was known at one knowledge instant. Both
-- windows are half-open by constraint, so the boundary predicates match range containment.
SELECT
    machine_id,
    entity_id,
    state,
    status,
    epoch,
    command_id,
    chart_version,
    valid_from,
    valid_to,
    known_from,
    known_to
FROM
    fsm.belief
WHERE
    machine_id = @machine_id
    AND entity_id = @entity_id
    AND valid_from <= @valid_at
    AND valid_to > @valid_at
    AND known_from <= @known_at
    AND known_to > @known_at;

