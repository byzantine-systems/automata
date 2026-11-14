-- Derived inbox consistency, independent of the belief model.
CREATE VIEW $schema$.blocked_drift AS
SELECT
    o.command_id,
    o.machine_id,
    o.entity_id,
    o.seq,
    o.blocked,
    o.expected_blocked
FROM (
    SELECT
        c.command_id,
        c.machine_id,
        c.entity_id,
        c.seq,
        c.blocked,
        c.seq > MIN(c.seq) OVER (PARTITION BY c.machine_id, c.entity_id) AS expected_blocked
    FROM
        $schema$.command c
    WHERE
        c.status IN ('ready', 'leased')) o
WHERE
    o.blocked IS DISTINCT FROM o.expected_blocked;

