-- ---------------------------------------------------------------------------
-- fsm_transition: the append-only record of what each command decided.
--
-- Column for column the PostgreSQL log, so history reads the same from either
-- store. The two text arrays become JSON arrays, since SQLite has no array
-- type.
-- ---------------------------------------------------------------------------
CREATE TABLE fsm_transition (
    machine_id TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    command_id INTEGER NOT NULL,
    chart_version INTEGER NOT NULL,
    event TEXT NOT NULL,
    actions TEXT NOT NULL,
    from_state TEXT NOT NULL,
    to_state TEXT NOT NULL,
    -- Which node handled the event, and the exit and entry paths around the
    -- least common ancestor, so an unexpected transition can be explained.
    handled_by TEXT NOT NULL,
    exited TEXT NOT NULL,
    entered TEXT NOT NULL,
    status TEXT NOT NULL,
    -- Business time, from the caller.
    effective_at INTEGER NOT NULL,
    -- Database time, never caller-supplied: the one clock that decides the
    -- order history is written in.
    committed_at INTEGER NOT NULL,
    -- Gapless per entity. The head invariant already means one finalizer per
    -- entity at a time; this key is the backstop.
    CONSTRAINT transition_pkey PRIMARY KEY (machine_id, entity_id, epoch),
    CONSTRAINT transition_epoch_positive CHECK (epoch > 0),
    CONSTRAINT transition_status_valid CHECK (status IN ('running', 'suspended', 'terminated')),
    CONSTRAINT transition_documents_are_json CHECK (JSON_VALID(event) AND JSON_VALID(from_state) AND JSON_VALID(to_state)),
    CONSTRAINT transition_actions_is_array CHECK (JSON_TYPE(actions) = 'array'),
    CONSTRAINT transition_paths_are_arrays CHECK (JSON_TYPE(exited) = 'array' AND JSON_TYPE(entered) = 'array'),
    -- One command can never produce two transitions, whatever a retry does.
    CONSTRAINT transition_unique_command UNIQUE (command_id),
    CONSTRAINT transition_command_fkey FOREIGN KEY (command_id) REFERENCES fsm_command (command_id)
) STRICT;

-- Append-only, enforced rather than intended. History that could be edited in
-- place is not history. Deleting stays possible, because retention deletes.
CREATE TRIGGER fsm_transition_is_append_only
    BEFORE UPDATE ON fsm_transition
BEGIN
    SELECT
        RAISE(ABORT, 'fsm_transition: the transition log is append-only');
END;
