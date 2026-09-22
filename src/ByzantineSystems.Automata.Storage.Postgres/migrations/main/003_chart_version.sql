-- ---------------------------------------------------------------------------
-- fsm.machine_chart_version: what a declared chart version means.
--
-- Every command pins the chart_version it was resolved under, so that replay
-- decides it the same way twice. Until now that was an integer with nothing
-- behind it: no record of which chart the number referred to, and no way to
-- notice that somebody edited a chart's shape and left the number alone. That
-- failure is silent and late. Commands submitted under version 3 replay under a
-- different version 3, and the history records the new answer as fact.
--
-- This table is the record. One row per (machine, version), holding the
-- structural fingerprint of the chart that first claimed it, written once and
-- never overwritten. A process that starts with a different fingerprint for a
-- version already claimed learns so before it writes anything.
--
-- The fingerprint is computed in the application and stored here only to be
-- compared. Producing it means walking closures this database cannot see, and
-- every store implementation has to produce the same value for the same chart,
-- so the algorithm lives in one place: the layer that can see a chart at all.
--
-- The fingerprint is a tripwire and not a proof. It cannot see inside guard
-- predicates, transforms or entry actions, all of which are closures. It
-- catches a chart edited without a version bump; it cannot catch a behaviour
-- change that leaves the structure alone. Replay correctness still rests on the
-- developer bumping the version.
-- ---------------------------------------------------------------------------
-- The outcome of a registration, as a checked text domain for the same reason
-- fsm.command_status is one: adding an outcome later must not be an ALTER TYPE
-- whose new value cannot be used in the transaction that adds it.
CREATE DOMAIN fsm.chart_registration AS text CONSTRAINT chart_registration_valid CHECK (VALUE IN ('registered', 'matched', 'mismatched'));

CREATE TABLE fsm.machine_chart_version (
    machine_id text NOT NULL,
    version integer NOT NULL,
    fingerprint text NOT NULL,
    registered_at timestamptz NOT NULL DEFAULT STATEMENT_TIMESTAMP(),
    CONSTRAINT machine_chart_version_pkey PRIMARY KEY (machine_id, version),
    CONSTRAINT machine_chart_version_positive CHECK (version > 0),
    -- 64 lowercase hex digits, and the check is load-bearing rather than
    -- decorative. A truncated or upper-cased hash would be accepted, then
    -- mismatch against every chart forever, and the operator reading that
    -- mismatch has no way to tell it from a chart somebody really did edit.
    -- Refusing the write is the only report that names the actual fault.
    CONSTRAINT machine_chart_version_fingerprint_shape CHECK (fingerprint ~ '^[0-9a-f]{64}$')
);

COMMENT ON COLUMN fsm.machine_chart_version.fingerprint IS 'SHA-256 of the chart''s structure: root, and per node its id, parent, initial child, terminal flag and the kind of each rule in declaration order. Computed by the application, never here. Nodes are hashed in id order because sibling declaration order decides nothing; rules keep their order because first match wins.';

COMMENT ON COLUMN fsm.machine_chart_version.registered_at IS 'When this version was first claimed. Never updated: a matching re-registration is not a new fact.';

-- ---------------------------------------------------------------------------
-- The reference fsm.command.chart_version was waiting for.
-- ---------------------------------------------------------------------------
-- A command may now only pin a version some chart has declared. Without this,
-- chart_version is an integer nobody validates, and the first time anyone finds
-- out that a command references a version that never existed is during a replay
-- that cannot proceed.
--
-- No ON DELETE clause, so NO ACTION, and that is the useful behaviour: a
-- version that commands still reference cannot be deleted. There is deliberately
-- no routine for forgetting a registration, because forgetting one is how the
-- tripwire gets disarmed by habit. A developer iterating on a chart locally
-- either bumps the version or runs make db-reset.
--
-- Also deliberately no index on the referencing side. PostgreSQL scans the child
-- table when a parent key is deleted or updated, and neither happens here:
-- versions are appended and then kept. An index for it would put a fourth write
-- on every insert into the busiest table in the schema to serve an operation the
-- constraint above forbids.
ALTER TABLE fsm.command
    ADD CONSTRAINT command_chart_version_fkey FOREIGN KEY (machine_id, chart_version) REFERENCES fsm.machine_chart_version (machine_id, version);

