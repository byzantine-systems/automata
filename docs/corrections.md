# Custom corrections over core PostgreSQL

## Bundled temporal transport

The bundled temporal routines take six parallel typed arrays: `jsonb[]` states, `text[]`
statuses, `timestamptz[]` valid starts, `bigint[]` epochs and command IDs, and `integer[]` chart
versions. Build them from one ordered collection of belief rows. Empty arrays erase a suffix
for operator `CorrectAsync`; a replayed correction commit requires at least one belief.
SQL rejects mismatched lengths, SQL-null arrays/elements and invalid ordering atomically.
Application state payloads still use `Codec<'State>` and may encode JSON `null`.

## Correction capabilities

Corrections need three operations, not a particular history representation:

- `ITransitionLog.SuffixAsync` supplies the ordered committed suffix and its purge completeness.
- `IPriorState.BeforeAsync` supplies the snapshot strictly **before** the corrected instant.
- `ICorrectionCommitter.CommitCorrectionAsync` applies the correction atomically under its fence.

`Store.tryCorrections` assembles these into `CorrectionCapability` only when the configured store
implements all three at the machine's exact types. `Machine.correct` and preview refuse before
submission when the capability is absent. No temporal reader is required.

## Minimal transaction

A custom correction writer opens its own `NpgsqlConnection` and `NpgsqlTransaction`, passing
the caller's cancellation token throughout. It uses these common schema-qualified routines:

1. `command_standing(commandId, token)` locks the command and entity, returning `outcome`,
   current `epoch`, stored `status`, `machine_id`, `entity_id`, and `chart_version`.
2. If already finalized or lease-lost, return that verdict without writing. If the live epoch
   differs from the expected epoch, return `FinalizeOutcome.Conflict(expected, actual)`.
3. `append_transition(...)` writes the correction's own transition with **no actions**.
4. `set_entity_state(...)` writes its final replayed state at `expected + 1`, attributed to
   this correction command, regardless of how the custom implementation represents history.
5. Write any application-owned historical facts in the same transaction.
6. `close_command(commandId, 'succeeded')` closes the command and promotes the next entity head.
7. Commit. On an error or cancellation, disposal rolls the transaction back.

These are transaction building blocks, not independent application commands. Always acquire the
standing fence before writes, preserve the ordering, and commit the whole operation together.
Role hardening should grant applications only the entry points their writer needs.

The complete, compiled example is
[`SnapshotCorrectionStore.fs`](../tests/ByzantineSystems.Automata.Storage.Postgres.Tests/SnapshotCorrectionStore.fs).
It derives prior state from the last transition effective strictly before the instant and uses
an actual Npgsql transaction for the shared routines. It has no belief tables. This is a minimal
test implementation, not a general-purpose historical index: applications with repeated historical
revisions must define how their own records reconstruct prior state after earlier corrections.

The same generated correction equations run against that implementation and the bundled
bitemporal store: exact prior state and suffix, stale-token no-op, epoch conflict, idempotent retry,
final snapshot, no historical action re-emission, and continued ordinary processing. A separate
runtime test previews and replays a missed event on a snapshot-only schema.

## Operator belief corrections are a different right

`ICorrectionStore.CorrectAsync` changes beliefs only. It takes an expected **core** epoch and
returns `CorrectionOutcome.Conflict actual` when it moved. It never advances `entity_state`,
even when an empty list removes every current belief. Machine corrections use the atomic
committer instead; mixing those operations would let a timeline rewrite regress processor state.
