-- Terminates a leased command and releases its entity.
--
-- @status is cast explicitly to the domain. Npgsql sends a text parameter as
-- text, and leaving the coercion to the server is the kind of thing that works
-- until an overload appears next to it.
SELECT
    fsm.ack_command (@command_id, @lease_token, @status::fsm.command_status) AS outcome;

