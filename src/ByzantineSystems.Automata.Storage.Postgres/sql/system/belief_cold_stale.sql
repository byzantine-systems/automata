-- Whether the materialized cold layer of fsm.belief is a day behind. See
-- fsm.belief_cold_is_stale; the refresh it calls for is its own statement.
SELECT
    fsm.belief_cold_is_stale () AS stale;

