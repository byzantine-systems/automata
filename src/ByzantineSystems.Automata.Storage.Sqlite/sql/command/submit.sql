-- Appends one command to its entity's queue.
--
-- Runs inside a BEGIN IMMEDIATE transaction, after the idempotency lookup and
-- the chart-version check. SQLite has one writer per file, so both values
-- computed here are race-free without the advisory lock PostgreSQL needs:
--
--   * seq, gapless per entity, from max(seq) + 1: one backward step on
--     command_unique_seq's index.
--   * blocked, from whether an open sibling exists, served by
--     fsm_command_entity_open_idx. A command with an open sibling waits its
--     turn, which is what keeps fsm_command_entity_head_idx satisfied.
--
-- The constraints, not the serialisation, remain the integrity boundary.
INSERT INTO fsm_command (machine_id, entity_id, seq, idempotency_key, chart_version, event, kind, blocked, visible_at, received_at, tenant, principal, source, correlation_id, causation_id)
    VALUES (@machine_id, @entity_id, COALESCE((
            SELECT
                MAX(c.seq)
            FROM fsm_command c
            WHERE
                c.machine_id = @machine_id AND c.entity_id = @entity_id), 0) + 1, @idempotency_key, @chart_version, @event, @kind, EXISTS (
            SELECT
                1
            FROM
                fsm_command c
            WHERE
                c.machine_id = @machine_id
                AND c.entity_id = @entity_id
                AND c.status IN ('ready', 'leased')), @visible_at, @received_at, @tenant, @principal, @source, @correlation_id, @causation_id)
RETURNING
    command_id;
