SELECT
    outcome,
    epoch,
    status
FROM
    $schema$.fail_command (@command_id, @lease_token, @status::$schema$.command_status, @kind::$schema$.failure_kind, @domain, @reason, @replay_kind, @version, @failure_epoch, @budget);

