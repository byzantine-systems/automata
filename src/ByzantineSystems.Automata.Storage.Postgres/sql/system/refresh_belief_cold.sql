-- Moves superseded beliefs older than the cutoff into the materialized cold
-- layer, without blocking readers.
--
-- A statement of its own, never part of a batch or a function: REFRESH ...
-- CONCURRENTLY refuses to run inside a transaction block, and a multi-statement
-- command is one. Readers take the boundary from the view's own rows, so a
-- refresh that is late, slow or fails costs speed and never correctness.
REFRESH MATERIALIZED VIEW CONCURRENTLY fsm.belief_cold;

