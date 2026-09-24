namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Resilience
open Npgsql
open Polly
open Polly.CircuitBreaker
open Polly.Timeout

/// <summary>Shared boundary helpers for the PostgreSQL stores.</summary>
[<RequireQualifiedAccess>]
module internal Db =

    /// Recognises the named PostgreSQL uniqueness constraint used for an idempotency race.
    let (|UniqueViolation|_|) (constraintName: string) (error: exn) =
        match error with
        | :? PostgresException as postgresError when
            postgresError.SqlState = PostgresErrorCodes.UniqueViolation
            && postgresError.ConstraintName = constraintName
            ->
            Some()
        | _ -> None

    /// Recognises the named PostgreSQL foreign-key constraint, for a reference that was never
    /// registered rather than a transient failure.
    let (|ForeignKeyViolation|_|) (constraintName: string) (error: exn) =
        match error with
        | :? PostgresException as postgresError when
            postgresError.SqlState = PostgresErrorCodes.ForeignKeyViolation
            && postgresError.ConstraintName = constraintName
            ->
            Some()
        | _ -> None

    /// <summary>
    /// Which driver failures are worth another attempt in a moment.
    ///
    /// A <see cref="T:Npgsql.PostgresException" /> is the server having answered, and it
    /// answered with a reason: a constraint was violated, a value would not cast, a routine
    /// refused its arguments. Repeating those produces the same reason. The exceptions worth
    /// repeating are the ones where no answer arrived at all, which is every other
    /// <see cref="T:Npgsql.NpgsqlException" />: a dropped socket, a pool timeout, a server
    /// still coming back up.
    ///
    /// The one server answer that is transient is <c>57P01</c>, admin shutdown, which is what
    /// a connection gets when the database is restarting under it.
    /// </summary>
    let isTransient (error: exn) =
        match error with
        | :? PostgresException as postgresError -> postgresError.SqlState = PostgresErrorCodes.AdminShutdown
        | :? NpgsqlException -> true
        | _ -> false

    /// <summary>
    /// The PostgreSQL adapter boundary, and the only place resilience is applied.
    ///
    /// The pipeline runs <em>inside</em> this function rather than around it, because Polly
    /// reads exceptions and everything above this line reads
    /// <see cref="T:Microsoft.FSharp.Core.FSharpResult`2" />. Wrapped the other way round it
    /// would retry nothing: an <c>Error</c> is a successful return to a resilience strategy.
    ///
    /// Afterwards the translation happens once. Caller cancellation stays cancellation, the
    /// driver and strategy failures this layer understands become
    /// <c>StoreError.Unavailable</c>, and anything else is left alone to reach the worker's
    /// supervision boundary with its stack trace intact. Only what is understood is caught.
    /// </summary>
    let private classify
        (work: unit -> Task<Result<'T, StoreError>>)
        (ct: CancellationToken)
        : Task<Result<'T, StoreError>> =
        task {
            try
                return! work ()
            with
            | :? OperationCanceledException when ct.IsCancellationRequested ->
                return! Task.FromCanceled<Result<'T, StoreError>>(ct)
            | :? NpgsqlException as error -> return Error(StoreError.Unavailable error)
            | :? TimeoutRejectedException as error -> return Error(StoreError.Unavailable error)
            | :? BrokenCircuitException as error -> return Error(StoreError.Unavailable error)
        }

    /// <summary>
    /// Runs a statement that is safe to repeat, with the pipeline applied.
    ///
    /// Everything reached this way is idempotent by construction and not by hope. A read
    /// repeats freely. <c>fsm.submit_command</c> deduplicates on its idempotency key.
    /// <c>fsm.finalize_command</c> answers a repeat from the same lease with what that lease
    /// already did, which is the entire reason it was written that way. The fenced lease
    /// updates compare a token that a second attempt still carries.
    /// </summary>
    let protect
        (pipeline: ResiliencePipeline)
        (work: CancellationToken -> Task<Result<'T, StoreError>>)
        (ct: CancellationToken)
        : Task<Result<'T, StoreError>> =
        classify (fun () -> ResiliencePipeline.executeTask pipeline work ct) ct

    /// <summary>
    /// Runs a statement that must not be repeated automatically, translating failures the same
    /// way but without the pipeline.
    ///
    /// <c>fsm.claim_commands</c> is the reason this exists. A claim whose reply was lost has
    /// already leased a batch, and repeating it leases a second one while the first stays
    /// invisible until its lease lapses: no corruption, because the lease expires, but every
    /// entity in that first batch stalls for the lease duration for nothing. A worker that
    /// polls again in a moment recovers faster than a retry does, so the right answer to a
    /// failed claim is to return empty and let the loop come back.
    /// </summary>
    let protectOnce
        (work: CancellationToken -> Task<Result<'T, StoreError>>)
        (ct: CancellationToken)
        : Task<Result<'T, StoreError>> =
        classify (fun () -> work ct) ct

    let timestamp (dto: DateTimeOffset) : DateTime = dto.UtcDateTime

    /// <summary>The instance lifecycle, as the fsm.instance_status domain spells it.</summary>
    let instanceStatusToString (status: InstanceStatus) : string =
        match status with
        | Running -> "running"
        | Suspended -> "suspended"
        | Terminated -> "terminated"

    let epochOf (value: int64) : Epoch = Epoch.ofUInt64 (uint64 value)

    let fromTimestamp (dt: DateTime) : DateTimeOffset = DateTimeOffset(dt, TimeSpan.Zero)

    /// <summary>
    /// Reads one bound of a range as an instant, or <c>None</c> when it is open.
    ///
    /// The schema's rule that absence always has a value, <c>'infinity'</c>, is a storage rule:
    /// it stops a <c>CHECK</c> passing by accident on a null. Above this line an open bound is
    /// better said as <c>None</c>, and Npgsql surfaces <c>'infinity'</c> as
    /// <see cref="F:System.DateTime.MaxValue" />, so the translation happens once, here.
    /// </summary>
    let openEnded (bound: DateTime) : DateTimeOffset option =
        if bound = DateTime.MaxValue then
            None
        else
            Some(fromTimestamp bound)

    let toStoreError (error: CodecError) : StoreError =
        match error with
        | CodecError.EncodeError(typeName, ex)
        | CodecError.DecodeError(typeName, ex) -> StoreError.Serialization(typeName, ex)

    /// <summary>Wraps a decode failure carrying only a message, which is what the entity-id
    /// projection reports, as the serialization error the contract promises.</summary>
    let decodeFailure (typeName: string) (message: string) : StoreError =
        StoreError.Serialization(typeName, FormatException message)

    /// <summary>The instance lifecycle, read back from the fsm.instance_status domain.</summary>
    let instanceStatusFromString (value: string) : Result<InstanceStatus, StoreError> =
        match value with
        | "running" -> Ok Running
        | "suspended" -> Ok Suspended
        | "terminated" -> Ok Terminated
        | other -> Error(decodeFailure (nameof InstanceStatus) $"unknown instance status {other}")

/// <summary>
/// Column access by name.
///
/// Routine results are read by name rather than by ordinal throughout. Composite mapping in F#
/// is fragile, and an ordinal read turns "a column was added in the middle" from a compile error
/// into a wrong value that still type-checks.
/// </summary>
[<RequireQualifiedAccess>]
module internal Row =

    let string (reader: NpgsqlDataReader) (name: string) : string =
        reader.GetString(reader.GetOrdinal name)

    let int32 (reader: NpgsqlDataReader) (name: string) : int = reader.GetInt32(reader.GetOrdinal name)

    let int64 (reader: NpgsqlDataReader) (name: string) : int64 = reader.GetInt64(reader.GetOrdinal name)

    let bool (reader: NpgsqlDataReader) (name: string) : bool =
        reader.GetBoolean(reader.GetOrdinal name)

    let timestamp (reader: NpgsqlDataReader) (name: string) : DateTimeOffset =
        Db.fromTimestamp (reader.GetDateTime(reader.GetOrdinal name))

    let textArray (reader: NpgsqlDataReader) (name: string) : string list =
        reader.GetFieldValue<string array>(reader.GetOrdinal name) |> List.ofArray

    /// <summary>
    /// Reads a text column whose empty value means "not supplied". No column in the schema is
    /// nullable, so absence is the empty string on the way out and <c>None</c> on the way in.
    /// </summary>
    let optionalString (reader: NpgsqlDataReader) (name: string) : string option =
        match reader.GetString(reader.GetOrdinal name) with
        | "" -> None
        | value -> Some value
