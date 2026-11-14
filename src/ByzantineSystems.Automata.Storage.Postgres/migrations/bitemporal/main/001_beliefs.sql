CREATE DOMAIN $temporal_schema$.belief_outcome AS text CONSTRAINT belief_outcome_valid CHECK (VALUE IN ('opened', 'split', 'replaced'));

CREATE TABLE $temporal_schema$.instance_state (machine_id text NOT NULL, entity_id text NOT NULL, state jsonb NOT NULL, status $schema$.instance_status NOT NULL, epoch bigint NOT NULL, command_id bigint NOT NULL, chart_version integer NOT NULL, valid_during tstzrange NOT NULL DEFAULT TSTZRANGE(NOW(), 'infinity', '[)'), system_time tstzrange NOT NULL DEFAULT TSTZRANGE(STATEMENT_TIMESTAMP(), 'infinity', '[)'), PRIMARY KEY (machine_id, entity_id, valid_during WITHOUT OVERLAPS), CONSTRAINT instance_state_valid_half_open CHECK (LOWER_INC(valid_during) AND NOT UPPER_INC(valid_during)), CONSTRAINT instance_state_system_half_open CHECK (LOWER_INC(system_time) AND NOT UPPER_INC(system_time))
);

CREATE UNIQUE INDEX instance_state_live_idx ON $temporal_schema$.instance_state (machine_id, entity_id)
WHERE
    UPPER(valid_during) = 'infinity';

CREATE TABLE $temporal_schema$.instance_state_history (LIKE $temporal_schema$.instance_state INCLUDING DEFAULTS INCLUDING CONSTRAINTS
);

CREATE INDEX instance_state_history_as_of_idx ON $temporal_schema$.instance_state_history USING gist (machine_id, entity_id, system_time);

CREATE INDEX instance_state_history_retention_idx ON $temporal_schema$.instance_state_history (machine_id, UPPER(system_time));

-- Opting in later starts knowledge here. It cannot reconstruct opinions never recorded.
INSERT INTO $temporal_schema$.instance_state (machine_id, entity_id, state, status, epoch, command_id, chart_version, valid_during)
SELECT
    machine_id,
    entity_id,
    state,
    status,
    epoch,
    command_id,
    chart_version,
    TSTZRANGE(effective_at, 'infinity', '[)')
FROM
    $schema$.entity_state;

COMMENT ON TABLE $temporal_schema$.instance_state IS 'Seeded at opt-in from core snapshots; knowledge before opt-in is unknown. Closed valid-time rows are retained indefinitely.';

