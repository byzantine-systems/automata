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

-- ---------------------------------------------------------------------------
-- fsm.entity_purge: the furthest back a replay can still see.
--
-- Retention removes an entity's oldest commands together with their transitions,
-- and a transition is the only thing replay reads. The greatest effective instant
-- any purged transition carried is therefore the exact boundary of a complete
-- suffix: a suffix that starts at or before it may have lost events, one that
-- starts strictly after it has not. The value is monotonic (a purge never makes
-- replay see further back) and must outlive the history it describes, so it has
-- no foreign key. The '-infinity' default keeps absence a value rather than a
-- null, but a row only exists once something was actually purged.
-- ---------------------------------------------------------------------------
CREATE TABLE fsm.entity_purge (
    machine_id text NOT NULL,
    entity_id text NOT NULL,
    max_purged_effective_at timestamptz NOT NULL DEFAULT '-infinity',
    PRIMARY KEY (machine_id, entity_id)
);

