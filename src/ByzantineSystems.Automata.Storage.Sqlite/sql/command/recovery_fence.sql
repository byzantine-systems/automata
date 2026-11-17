-- The raw fence for poison recovery: whether the claimed row is still this
-- claim's leased command. Recovery runs precisely because the payload cannot
-- be decoded, so nothing here may depend on the domain identity being
-- readable; the row's stored columns are the whole decision.
SELECT
    status,
    lease_token
FROM
    fsm_command
WHERE
    command_id = @command_id;
