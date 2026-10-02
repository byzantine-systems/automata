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
    /// A named constraint the server reported as violated. Named, because a store recognises a
    /// violation by the constraint it wrote, never by the message.
    /// </summary>
    type ConstraintViolation =
        | Unique of constraintName: string
        | ForeignKey of constraintName: string

    /// <summary>What a failed attempt means, decided once from the exception it ended with.</summary>
    type DriverFailure =
        /// <summary>The caller cancelled. Reported as a cancelled task, never as an error.</summary>
        | Cancelled

        /// <summary>No answer arrived: a dropped connection, a pool timeout, a timed-out attempt, an open circuit.</summary>
        | Unavailable of exn

        /// <summary>The server answered that a named constraint refused the write.</summary>
        | Violated of ConstraintViolation * exn

        /// <summary>Anything else, which is a defect rather than an outage.</summary>
        | Unexpected of exn

    /// <summary>
    /// Classifies the exception an attempt ended with. Pure and exhaustive: this is the one place
    /// a driver exception is given a meaning, and it is given one as a value.
    ///
    /// A <see cref="T:Npgsql.PostgresException" /> is the server having answered, so apart from
    /// admin shutdown and a named violation it is a defect. Any other
    /// <see cref="T:Npgsql.NpgsqlException" /> is an answer that never arrived.
    /// </summary>
    let classify (ct: CancellationToken) (error: exn) : DriverFailure =
        match error with
        | :? OperationCanceledException when ct.IsCancellationRequested -> Cancelled
        | :? PostgresException as postgresError when postgresError.SqlState = PostgresErrorCodes.UniqueViolation ->
            Violated(Unique postgresError.ConstraintName, error)
        | :? PostgresException as postgresError when postgresError.SqlState = PostgresErrorCodes.ForeignKeyViolation ->
            Violated(ForeignKey postgresError.ConstraintName, error)
        | :? PostgresException as postgresError when postgresError.SqlState = PostgresErrorCodes.AdminShutdown ->
            Unavailable error
        | :? PostgresException -> Unexpected error
        | :? NpgsqlException
        | :? TimeoutRejectedException
        | :? BrokenCircuitException -> Unavailable error
        | _ -> Unexpected error

    /// <summary>
    /// What one call through the boundary came to: an answer, or the server refusing it under a
    /// named constraint. A refusal is a value the caller can match on, so a store that expects a
    /// particular violation handles it as an outcome rather than catching anything.
    /// </summary>
    type Attempt<'T> =
        | Answered of Result<'T, StoreError>
        | Refused of ConstraintViolation * exn

    /// <summary>Turns an attempt's outcome into an answer or a named refusal.</summary>
    let private settle (ct: CancellationToken) (outcome: Outcome<Result<'T, StoreError>>) : Task<Attempt<'T>> =
        match outcome.Exception with
        | null -> Task.FromResult(Answered outcome.Result)
        | error ->
            match classify ct error with
            | Cancelled -> Task.FromCanceled<Attempt<'T>>(ct)
            | Unavailable cause -> Task.FromResult(Answered(Error(StoreError.Unavailable cause)))
            | Violated(violation, cause) -> Task.FromResult(Refused(violation, cause))
            | Unexpected cause -> Task.FromResult(Answered(Error(StoreError.Unexpected cause)))

    /// <summary>
    /// The PostgreSQL adapter boundary, and the only place resilience is applied.
    ///
    /// The pipeline runs <em>inside</em> this function rather than around it, because Polly
    /// reads exceptions and everything above this line reads
    /// <see cref="T:Microsoft.FSharp.Core.FSharpResult`2" />. Wrapped the other way round it
    /// would retry nothing: an <c>Error</c> is a successful return to a resilience strategy.
    ///
    /// The pipeline hands back the outcome as a value, so nothing here catches anything:
    /// <see cref="M:ByzantineSystems.Automata.Storage.Postgres.Db.classify(System.Threading.CancellationToken,System.Exception)" />
    /// decides what the exception means.
    /// </summary>
    let attempt
        (pipeline: ResiliencePipeline)
        (work: CancellationToken -> Task<Result<'T, StoreError>>)
        (ct: CancellationToken)
        : Task<Attempt<'T>> =
        backgroundTask {
            let! outcome = ResiliencePipeline.executeOutcome pipeline work ct
            return! settle ct outcome
        }

    /// <summary>An attempt whose refusals nobody asked to handle, which makes them defects like any other.</summary>
    let private answered (attempt: Task<Attempt<'T>>) : Task<Result<'T, StoreError>> =
        backgroundTask {
            match! attempt with
            | Answered result -> return result
            | Refused(_, cause) -> return Error(StoreError.Unexpected cause)
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
        answered (attempt pipeline work ct)

    /// <summary>
    /// Runs a statement that must not be repeated automatically, translating failures the same
    /// way but without retries: the empty pipeline makes exactly one attempt.
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
        answered (attempt Polly.ResiliencePipeline.Empty work ct)

    /// <summary>
    /// A column a view reports as nullable, which the schema says never is. PostgreSQL cannot
    /// carry NOT NULL through a view, so every view column arrives as an option; a missing one
    /// means the view and the code disagree, and is reported as such rather than defaulted.
    /// </summary>
    let required (typeName: string) (column: string) (value: 'T option) : Result<'T, StoreError> =
        match value with
        | Some value -> Ok value
        | None -> Error(StoreError.Serialization(typeName, FormatException $"the view returned no {column}"))

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

    /// <summary>Reads every remaining row through <paramref name="read" />, in result order.</summary>
    let all (read: NpgsqlDataReader -> 'T) (reader: NpgsqlDataReader) (ct: CancellationToken) : Task<'T list> =
        let rec next (acc: 'T list) =
            backgroundTask {
                match! reader.ReadAsync ct with
                | true -> return! next (read reader :: acc)
                | false -> return List.rev acc
            }

        next []
