-- ---------------------------------------------------------------------------
-- fsm.command_correction: what a correction command corrects.
--
-- The missed event itself is the command's event; this holds the rest. A row
-- exists exactly for the commands whose kind is 'correction', written in the
-- same transaction as the command by fsm.submit_correction.
-- ---------------------------------------------------------------------------
CREATE DOMAIN fsm.divergence AS text CONSTRAINT divergence_valid CHECK (VALUE IN ('fail', 'truncate'));

CREATE TABLE fsm.command_correction (
    command_id bigint PRIMARY KEY,
    -- When the missed event should have happened: the instant the belief
    -- timeline is rewritten from.
    effective_at timestamptz NOT NULL,
    on_divergence fsm.divergence NOT NULL,
    replay_limit integer NOT NULL,
    CONSTRAINT command_correction_replay_limit_positive CHECK (replay_limit > 0),
    CONSTRAINT command_correction_command_fkey FOREIGN KEY (command_id) REFERENCES fsm.command (command_id)
);

