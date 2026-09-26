-- ---------------------------------------------------------------------------
-- fsm_entity_snapshot: what each entity is now.
--
-- The PostgreSQL store keeps a bitemporal belief timeline, and a snapshot there
-- is the belief whose windows are both open. This store keeps only the current
-- belief, which is everything IStateReader asks for. Reading and changing the
-- past are optional capabilities, and this store does not offer them yet: they
-- need a timeline, and a timeline here needs its own design without tstzrange
-- or WITHOUT OVERLAPS.
--
-- One row per entity that has committed at least once, replaced by every
-- commit in the same transaction that appends its transition.
-- ---------------------------------------------------------------------------
CREATE TABLE fsm_entity_snapshot (
    machine_id TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    state TEXT NOT NULL,
    status TEXT NOT NULL,
    -- The epoch of the transition that produced this state. A commit compares
    -- the caller's expected epoch against it, which is the conflict check.
    epoch INTEGER NOT NULL,
    -- Where it came from, so a snapshot always points at something in the log.
    command_id INTEGER NOT NULL,
    chart_version INTEGER NOT NULL,
    -- Business time of that transition, and database time of the write.
    effective_at INTEGER NOT NULL,
    committed_at INTEGER NOT NULL,
    CONSTRAINT entity_snapshot_pkey PRIMARY KEY (machine_id, entity_id),
    CONSTRAINT entity_snapshot_state_is_json CHECK (JSON_VALID(state)),
    CONSTRAINT entity_snapshot_status_valid CHECK (status IN ('running', 'suspended', 'terminated')),
    CONSTRAINT entity_snapshot_epoch_positive CHECK (epoch > 0),
    CONSTRAINT entity_snapshot_command_fkey FOREIGN KEY (command_id) REFERENCES fsm_command (command_id)
) STRICT;

-- The child side of entity_snapshot_command_fkey. SQLite checks a foreign key
-- on delete by looking up the child table, and with no index here every
-- command retention deletes would scan every entity's snapshot. The other
-- children of fsm_command are keyed by command_id already.
CREATE INDEX fsm_entity_snapshot_command_idx ON fsm_entity_snapshot (command_id);
