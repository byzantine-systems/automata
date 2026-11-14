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
