-- Whether the materialized cold layer of $schema$.belief is a day behind. See
-- $schema$.belief_cold_is_stale; the refresh it calls for is its own statement.
SELECT
    $schema$.belief_cold_is_stale () AS stale;

