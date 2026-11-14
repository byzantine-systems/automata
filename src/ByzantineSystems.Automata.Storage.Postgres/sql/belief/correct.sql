SELECT
    superseded,
    actual_epoch,
    CONFLICT
FROM
    $temporal_schema$.correct_beliefs (@machine_id, @entity_id, @expected_epoch, @valid_from, @beliefs::jsonb);

