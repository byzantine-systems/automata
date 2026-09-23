namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open Npgsql

/// <summary>Shared boundary helpers for the PostgreSQL stores.</summary>
[<RequireQualifiedAccess>]
module internal Db =

    /// Recognises cancellation owned by the caller rather than by an unrelated operation.
    let (|CanceledBy|_|) (ct: CancellationToken) (error: exn) =
        match error with
        | :? OperationCanceledException when ct.IsCancellationRequested -> Some()
        | _ -> None

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
    /// PostgreSQL adapter boundary. Caller cancellation remains task cancellation, known
    /// driver failures become <c>StoreError.Unavailable</c>, and every unrelated exception
    /// propagates to the runtime safety boundary unchanged.
    /// </summary>
    let protect
        (work: CancellationToken -> Task<Result<'T, StoreError>>)
        (ct: CancellationToken)
        : Task<Result<'T, StoreError>> =
        task {
            try
                return! work ct
            with
            | CanceledBy ct -> return! Task.FromCanceled<Result<'T, StoreError>>(ct)
            | :? NpgsqlException as error -> return Error(StoreError.Unavailable error)
        }

    let timestamp (dto: DateTimeOffset) : DateTime = dto.UtcDateTime

    let fromTimestamp (dt: DateTime) : DateTimeOffset = DateTimeOffset(dt, TimeSpan.Zero)

    let toStoreError (error: CodecError) : StoreError =
        match error with
        | CodecError.EncodeError(typeName, ex)
        | CodecError.DecodeError(typeName, ex) -> StoreError.Serialization(typeName, ex)

    /// <summary>Wraps a decode failure carrying only a message, which is what the entity-id
    /// projection reports, as the serialization error the contract promises.</summary>
    let decodeFailure (typeName: string) (message: string) : StoreError =
        StoreError.Serialization(typeName, FormatException message)

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
