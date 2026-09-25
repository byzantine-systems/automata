-- Leases up to @batch runnable commands for one machine.
--
-- The WHERE clause is fsm_command_claim_idx's predicate plus the visibility
-- test, and blocked = 0 is spelled exactly as the index spells it. Written as
-- NOT blocked, the planner no longer sees the index's predicate implied, loses
-- the index, and sorts the whole queue in a temp B-tree instead.
--
-- UPDATE ... WHERE command_id IN (subquery) evaluates the subquery once, so
-- LIMIT bounds the statement. PostgreSQL needs a materialised CTE for the same
-- guarantee; SQLite does not rescan an uncorrelated IN list.
--
-- An expired lease is claimable again: status 'leased' with a deadline in the
-- past passes the visibility test, and the new token fences the old holder.
--
-- Per-entity mutual exclusion is fsm_command_entity_head_idx's job: at most one
-- unblocked open command per entity, so a batch never holds two for one
-- entity.
--
-- The column list is written out, as in the PostgreSQL store, so the reader's
-- contract does not move when a column is added.
UPDATE
    fsm_command
SET
    status = 'leased',
    visible_at = @deadline,
    lease_token = @lease_token,
    read_ct = read_ct + 1
WHERE
    command_id IN (
        SELECT
            c.command_id
        FROM
            fsm_command c
        WHERE
            c.machine_id = @machine_id
            AND c.status IN ('ready', 'leased')
            AND c.blocked = 0
            AND c.visible_at <= @now
        ORDER BY
            c.visible_at,
            c.command_id
        LIMIT @batch)
RETURNING
    command_id,
    machine_id,
    entity_id,
    seq,
    idempotency_key,
    chart_version,
    event,
    kind,
    status,
    blocked,
    visible_at,
    lease_token,
    read_ct,
    attempts,
    received_at,
    tenant,
    principal,
    source,
    correlation_id,
    causation_id;
