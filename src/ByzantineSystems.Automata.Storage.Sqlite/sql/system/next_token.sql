-- A fresh lease token, for one claim of commands or of actions.
--
-- One token per claim rather than per row. The fence compares the row's key
-- and its token together, so what a token must be is new for every claim,
-- which is what a counter advanced inside the claim's own write transaction
-- guarantees. The first token is 1: zero means "not leased".
UPDATE
    fsm_counter
SET
    value = value + 1
WHERE
    name = 'lease_token'
RETURNING
    value;
