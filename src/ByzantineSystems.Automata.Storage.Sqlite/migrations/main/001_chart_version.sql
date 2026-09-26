-- ---------------------------------------------------------------------------
-- fsm_machine_chart_version: what a declared chart version means.
--
-- The SQLite counterpart of fsm.machine_chart_version, and the same record: one
-- row per (machine, version), holding the structural fingerprint of the chart
-- that first claimed it, written once and never overwritten. The PostgreSQL
-- migration explains why the record exists; this file explains only where
-- SQLite makes it look different.
--
-- Conventions every script here follows:
--
--   * Every table is STRICT. Without it, SQLite's type affinity accepts a
--     string in an INTEGER column, and a constraint written against a number
--     compares against text instead. STRICT is what a PostgreSQL domain gives
--     the other store for free.
--   * Every object is prefixed fsm_. SQLite has no schemas, and a deployment
--     may point this store at a database it also uses for itself.
--   * No column is nullable. A CHECK passes when its expression is NULL, so a
--     nullable column quietly turns every constraint on it into a suggestion.
--     Absence always has a value: 0 for no lease, the empty string for an
--     unsupplied text, and the ends of DateTimeOffset's range for an unset or
--     open instant.
--   * Instants are INTEGER microseconds since the Unix epoch, bound by the
--     application. SQLite's own clock stops at milliseconds, so no column
--     defaults to it: one clock, read once per transaction, stamps everything.
--   * Scripts are in dependency order, and tables are created complete. There
--     is no ALTER TABLE ... ADD CONSTRAINT here, and in SQLite there could not
--     be one.
-- ---------------------------------------------------------------------------
CREATE TABLE fsm_machine_chart_version (
    machine_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    fingerprint TEXT NOT NULL,
    registered_at INTEGER NOT NULL,
    CONSTRAINT machine_chart_version_pkey PRIMARY KEY (machine_id, version),
    CONSTRAINT machine_chart_version_positive CHECK (version > 0),
    -- 64 lowercase hex digits. SQLite has no regular expressions, so the shape
    -- is two tests: the length, and the absence of any character outside the
    -- class. GLOB is case-sensitive, which is what rejects upper case.
    CONSTRAINT machine_chart_version_fingerprint_shape CHECK (LENGTH(fingerprint) = 64 AND fingerprint NOT GLOB '*[^0-9a-f]*')
) STRICT;
