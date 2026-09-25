-- Leases up to @batch deliverable actions of one machine.
--
-- Served by fsm_action_claim_idx (machine_id, visible_at). A queued action is
-- visible from the moment its commit wrote it, and a leased one again once its
-- deadline passes, so one predicate claims both new and abandoned-by-crash
-- deliveries. The new token fences whoever held the expired lease.
--
-- Unordered by contract; ordering by the deadline is only what the index
-- gives for free, and it serves the longest-waiting first.
UPDATE
    fsm_action
SET
    visible_at = @deadline,
    lease_token = @lease_token,
    read_ct = read_ct + 1
WHERE
    rowid IN (
        SELECT
            a.rowid
        FROM
            fsm_action a
        WHERE
            a.machine_id = @machine_id
            AND a.visible_at <= @now
        ORDER BY
            a.visible_at
        LIMIT @batch)
RETURNING
    command_id,
    ordinal,
    machine_id,
    entity_id,
    epoch,
    action,
    visible_at,
    lease_token,
    read_ct,
    enqueued_at;
