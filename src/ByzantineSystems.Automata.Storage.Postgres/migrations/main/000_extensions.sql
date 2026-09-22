-- ---------------------------------------------------------------------------
-- Extensions: the only superuser step in the migration set.
--
-- Installing an extension requires superuser, and a library cannot require that
-- of the people who use it. Everything needing that privilege is isolated in
-- this one script so a DBA can review and run it separately; every other script
-- runs as the ordinary schema owner.
--
-- The command inbox deliberately needs no extension at all:
--
--   * command_id is a bigint identity column and lease_token comes from an
--     ordinary sequence, so neither pgcrypto nor a uuid generator is needed.
--   * btree_gist only becomes necessary when the bitemporal belief tables and
--     their WITHOUT OVERLAPS keys land, which is a later step.
--
-- That is a property worth keeping rather than an accident. The inbox is the
-- part of this design that every deployment needs, including managed hosts
-- where nobody can install anything, so it asks for nothing. pgmq (action
-- delivery) and pg_cron (maintenance) arrive later, each behind its own guard,
-- and the development and CI database provides both through Nix.
--
-- Leave the file in place rather than deleting it: the slot, its ordering and
-- the privilege boundary it documents are the point. DbUp requires a statement,
-- so it asserts connectivity and nothing else.
-- ---------------------------------------------------------------------------
SELECT
    1;

