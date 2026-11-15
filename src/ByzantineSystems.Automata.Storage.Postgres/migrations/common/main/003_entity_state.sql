CREATE DOMAIN $schema$.instance_status AS text CONSTRAINT instance_status_valid CHECK (VALUE IN ('running', 'suspended', 'terminated'));

-- The single current-state and epoch authority, independent of the optional timeline.
CREATE TABLE $schema$.entity_state (machine_id text NOT NULL, entity_id text NOT NULL, state jsonb NOT NULL, status $schema$.instance_status NOT NULL, epoch bigint NOT NULL CHECK (epoch > 0), command_id bigint NOT NULL REFERENCES $schema$.command (command_id), chart_version integer NOT NULL, effective_at timestamptz NOT NULL, committed_at timestamptz NOT NULL DEFAULT STATEMENT_TIMESTAMP(), PRIMARY KEY (machine_id, entity_id)
);

CREATE INDEX entity_state_command_idx ON $schema$.entity_state (command_id);

CREATE TABLE $schema$.store_profile (singleton boolean PRIMARY KEY DEFAULT TRUE CHECK (singleton), beliefs text NOT NULL CHECK (beliefs IN ('snapshot', 'bitemporal'))
);

INSERT INTO $schema$.store_profile (beliefs)
    VALUES ('snapshot');

