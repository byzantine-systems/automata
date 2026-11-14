-- The raw id and token suffice; no domain payload or entity id needs decoding.
SELECT
    f.outcome
FROM
    $schema$.fail_command (@command_id, @lease_token, 'dead_letter', 'machine', 'null'::jsonb, @reason, '', 0, 0::bigint, 0) f;
