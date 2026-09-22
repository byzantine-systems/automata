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
-- ---------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE SCHEMA fsm;

