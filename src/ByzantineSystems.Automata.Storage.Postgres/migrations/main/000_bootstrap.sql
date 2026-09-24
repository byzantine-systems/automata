-- ---------------------------------------------------------------------------
-- Database-level objects: the extension and the schema.
--
-- Everything here needs a right on the *database* rather than on a schema, so
-- it is isolated in one script a DBA can review and run separately. Every later
-- script creates objects inside fsm and needs nothing beyond ownership of it.
--
-- btree_gist is trusted (pg_available_extension_versions.trusted), so any role
-- with CREATE on the database can install it; superuser is not required. Of the
-- extensions this design wants, only pg_cron needs superuser, plus a
-- shared_preload_libraries entry, which is why it arrives behind a guard.
--
-- It is needed by the temporal keys: PRIMARY KEY (..., valid_during WITHOUT
-- OVERLAPS) compiles into a GiST index covering the scalar columns as well as
-- the range, and GiST has no operator class for text on its own.
--
-- The command inbox needs no extension: command_id is a bigint identity column
-- and lease_token comes from an ordinary sequence. A deployment that only
-- submits and claims commands runs on a host where nobody may install anything.
--
-- IF NOT EXISTS because a managed host may ship it already, and because
-- make db-reset drops the schema while leaving extensions in place.
--
-- pgmq is the second extension, and unlike btree_gist it is NOT trusted: it
-- needs a superuser to install, which makes this script a genuine DBA step
-- rather than one only in principle. It carries the actions a commit emits,
-- enqueued inside the same transaction as the state change, which is what
-- makes "the transition happened and its effects were queued" a single fact
-- and removes the need for an outbox table of our own.
--
-- pgmq.read is ORDER BY msg_id ... WHERE vt <= clock_timestamp() ... FOR
-- UPDATE SKIP LOCKED against a (vt) index: unordered, at-least-once delivery,
-- which is exactly what an action queue wants. It is deliberately not used for
-- the command inbox, whose per-entity FIFO would cost O(queue depth) per poll
-- through read_grouped_head.
-- ---------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE EXTENSION IF NOT EXISTS pgmq;

-- pg_cron is optional, and arrives only when every condition for it holds.
-- It needs superuser, a shared_preload_libraries entry, and it installs only
-- in the one database its worker runs in (cron.database_name). Anyone else
-- skips it silently: maintenance is not optional, but pg_cron is only one of
-- the two things that can run it, and MaintenanceService is the other.
--
-- The IFs nest because plpgsql does not promise to short-circuit an AND, and
-- the order matters: a role that is not superuser cannot even read
-- cron.database_name ("permission denied to examine"), so the privilege test
-- has to come first.
DO $$
BEGIN
    IF (
        SELECT
            r.rolsuper
        FROM
            pg_roles r
        WHERE
            r.rolname = CURRENT_USER) THEN
        IF CURRENT_DATABASE() = CURRENT_SETTING('cron.database_name', TRUE) THEN
            IF EXISTS (
                SELECT
                    1
                FROM
                    pg_available_extensions a
                WHERE
                    a.name = 'pg_cron') THEN
            CREATE EXTENSION IF NOT EXISTS pg_cron;
        END IF;
    END IF;
END IF;
END
$$;

CREATE SCHEMA fsm;

