-- The currently held opinion is a database fact, not an application clock reading.
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
    $temporal_schema$.belief
WHERE
    machine_id = @machine_id
    AND entity_id = @entity_id
    AND valid_from <= @valid_at
    AND valid_to > @valid_at
    AND known_to = 'infinity';

