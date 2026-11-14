# Storage representation laws

The shared conformance suite checks interface behavior against a small independent model.
Provider tests cover constraints and transactions; the recording interpreter checks programs,
not SQL execution. Schema changes in Phase G edit base migrations and require a fresh database.

## Relational failures

`CommandFailure` is a sum: application error, machine reason, or one of four replay failures.
Only application errors use the application codec. The header and exactly its selected child
are written in the same finalization transaction. Composite foreign keys prevent incompatible
children; readers reject incomplete headers. Global lifecycle completeness belongs to Phase H.

* Encoding followed by decoding preserves every constructor and payload.
* Rejection and dead-lettering preserve the failure through result reads.
* Retrying finalization preserves the original result, even with a different requested ending.
* Stale tokens write nothing. Purging a command removes its complete failure representation.

`FailureTests` exercises all six leaf constructors on every generated case. Shared finalize
properties exercise each case against both providers. Schema tests check invalid child shapes;
the PostgreSQL purge contract walks transitive foreign keys as well as direct command children.

## Supervision audit

Reasons are PostgreSQL text, including empty strings, quotes, newlines and representable Unicode
(U+0000 is not a PostgreSQL text value). Kinds and strategies use the existing `EnumCodec`
spellings and checked domains. `PostgresSupervisionStoreTests` generates reasons across every
constructor pair and checks bounded newest-first reads with insertion order breaking time ties.

## Belief batches

Each encoded belief row is projected into six explicitly typed arrays. Projection preserves
row order and field association. SQL rejects null arrays, SQL-null elements, unequal lengths,
non-ascending times and invalid fields before rewriting. JSON `null` remains a valid payload.
Operator correction accepts an empty replacement (erase the suffix); correction commits require
a non-empty batch. Temporal properties compare replacement against an independent half-open
interval model, and invalid-array tests observe both live beliefs and archived system history.

## Action claims

Queue message identity, delivery count and expiry survive payload decoding failures. Typed
projection never lets malformed envelope numbers abort a batch. Per-row errors are kept inside
the claimed row until `ClaimRecovery` partitions work from poison; the outer query only fails on
infrastructure errors. Healthy work is conserved even when quarantine fails.

Decodable envelopes use business action dead letters. Corrupt envelopes without usable business
identity go to `action_quarantine`, keyed by queue and message ID with raw evidence. Recording and
archiving share the raw fence and transaction. Action-archive retention purges quarantine with
the same bounded batch budget. Provider properties mix poison positions with healthy work and
check durable quarantine, fencing and retention; shared conformance checks ordinary poison work.
