-- Time travel: what did this database believe about an entity, for one instant
-- of business time, as of one instant of belief time.
--
-- Two containment tests against two different columns, and they answer two
-- different questions. @valid_at asks what was true in the world; @known_at
-- asks what we thought at the time. Holding @valid_at still and moving
-- @known_at is how a correction becomes visible as a correction: the same
-- moment in the world, two different opinions about it, and the difference is
-- the explanation an operator is asking for.
--
-- Both tables are needed. The live table holds beliefs still held; the history
-- twin holds beliefs since superseded. A row that was current at @known_at and
-- has since been corrected lives only in history, and a query reading just the
-- live table would answer with today's opinion and call it the past.
--
-- The temporal key guarantees at most one live belief covers @valid_at, and the
-- system_time windows of one entity's versions are disjoint by construction, so
-- this returns at most one row without needing DISTINCT or a LIMIT to say so.
--
-- epoch, command_id and chart_version are selected, which an earlier version of
-- this query deliberately omitted on the grounds that a superseded belief's
-- epoch is not the entity's current one. That was the right caution while the
-- only consumer wanted a snapshot. It is the wrong one for a Belief, which
-- carries its own two windows and exists to say which transition produced this
-- belief and under which chart. Read with the windows beside them, they are
-- unambiguous; read as if they were the entity's current epoch, they never were.
SELECT
    s.machine_id,
    s.entity_id,
    s.state,
    s.status,
    s.epoch,
    s.command_id,
    s.chart_version,
    s.valid_during,
    s.system_time
FROM (
    SELECT
        *
    FROM
        fsm.instance_state
    UNION ALL
    SELECT
        *
    FROM
        fsm.instance_state_history) s
WHERE
    s.machine_id = @machine_id
    AND s.entity_id = @entity_id
    AND s.valid_during @> @valid_at::timestamptz
    AND s.system_time @> @known_at::timestamptz;

