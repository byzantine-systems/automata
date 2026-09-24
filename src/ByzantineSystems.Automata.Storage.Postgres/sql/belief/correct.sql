-- Supersedes an entity's belief timeline from one instant onward.
--
-- The beliefs arrive as one jsonb array rather than as a statement each, because
-- rewriting a timeline is one change of mind and has to be one transaction. A
-- caller writing them row by row could be interrupted between two of them and
-- leave a timeline with a hole in it that every as-of query would answer from.
SELECT
    fsm.correct_beliefs (@machine_id, @entity_id, @valid_from, @beliefs::jsonb) AS superseded;

