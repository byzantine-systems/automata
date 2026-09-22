-- Pushes out the deadline of a lease this worker still holds.
SELECT
    fsm.extend_lease (@command_id, @lease_token, @lease::interval) AS outcome;

