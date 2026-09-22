-- ---------------------------------------------------------------------------
-- fsm.machine_chart_version: what a declared chart version means.
--
-- First of the tables, ahead of the inbox, because fsm.command references it.
-- The order of these scripts is dependency order, and that is the whole reason
-- this schema carries no ALTER TABLE ... ADD CONSTRAINT: a table is created
-- complete or not at all, so reading its definition tells you everything that
-- is true about it.
--
-- Every command pins the chart_version it was resolved under, so that replay
-- decides it the same way twice. Without this table that would be an integer
-- with nothing behind it: no record of which chart the number referred to, and
-- no way to notice that somebody edited a chart's shape and left the number
-- alone. That failure is silent and late. Commands submitted under version 3
-- replay under a different version 3, and the history records the new answer as
-- fact.
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
    -- 64 lowercase hex digits. Without this, a truncated or upper-cased hash is
    -- accepted and then mismatches against every chart forever, and the
    -- operator reading that mismatch cannot tell it from a chart somebody
    -- really did edit. Refusing the write names the actual fault.
    CONSTRAINT machine_chart_version_fingerprint_shape CHECK (fingerprint ~ '^[0-9a-f]{64}$')
);

COMMENT ON COLUMN fsm.machine_chart_version.fingerprint IS 'SHA-256 of the chart''s structure: root, and per node its id, parent, initial child, terminal flag and the kind of each rule in declaration order. Computed by the application, never here. Nodes are hashed in id order because sibling declaration order decides nothing; rules keep their order because first match wins.';

COMMENT ON COLUMN fsm.machine_chart_version.registered_at IS 'When this version was first claimed. Never updated: a matching re-registration is not a new fact.';

