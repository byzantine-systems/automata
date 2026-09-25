-- Hands a failed command back for a later attempt. Fenced: zero rows changed
-- means the caller's lease is gone.
--
-- The command keeps its place. It stays open, stays its entity's head, and
-- nothing else for that entity runs before it.
--
-- The delay is capped exponential backoff with full jitter, as PostgreSQL's
-- fsm.retry_backoff computes it: a uniform draw below
-- min(cap, base << attempts), where attempts is the pre-update count, so the
-- delay is for the attempt that has just failed. Jitter is not optional:
-- without it, every command that failed in the same instant retries in the
-- same instant, for as long as the backlog lasts.
--
-- The CASE compares before it shifts, so a large base cannot overflow the
-- shift: once base exceeds cap >> exponent, the product would exceed the cap
-- anyway. Both durations are microseconds and at least 1.
UPDATE
    fsm_command
SET
    status = 'ready',
    lease_token = 0,
    attempts = attempts + 1,
    visible_at = @now + ABS(RANDOM() % (
            CASE WHEN @base_us > (@cap_us >> MIN(attempts, 30)) THEN
                @cap_us
            ELSE
                @base_us << MIN(attempts, 30)
            END))
WHERE
    command_id = @command_id
    AND lease_token = @lease_token
    AND status = 'leased';
