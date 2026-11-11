namespace ByzantineSystems.Automata.Storage

open System

/// <summary>An ASCII pgmq-compatible queue name, at most 47 characters.</summary>
[<Struct>]
type ActionQueueName = private ActionQueueName of string

[<RequireQualifiedAccess>]
module ActionQueueName =

    // pgmq.validate_queue_name reserves 16 identifier characters for its longest prefix.
    // https://github.com/pgmq/pgmq/blob/main/pgmq-extension/sql/pgmq.sql
    let tryCreate (value: string) : Result<ActionQueueName, string> =
        if isNull value || value.Length = 0 then
            Error "An action queue name cannot be empty. Use ActionDelivery.Discard to omit delivery."
        elif value.Length > 47 then
            Error "An action queue name cannot exceed pgmq's 47-character limit."
        elif not (Text.RegularExpressions.Regex.IsMatch(value, "\\A[a-z_][a-z0-9_]*\\z")) then
            Error "An action queue name must match [a-z_][a-z0-9_]*."
        else
            Ok(ActionQueueName value)

    let create value =
        match tryCreate value with
        | Ok name -> name
        | Error message -> invalidArg (nameof value) message

    let value (ActionQueueName value) = value

/// <summary>Deliver a transition's recorded actions, or explicitly discard their delivery.</summary>
[<RequireQualifiedAccess>]
type ActionDelivery =
    | Deliver of ActionQueueName
    | Discard

/// <summary>The maximum number of work items claimed or processed in one operation.</summary>
[<Struct>]
type ClaimBatch = private ClaimBatch of int

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Storage.ClaimBatch" />.</summary>
[<RequireQualifiedAccess>]
module ClaimBatch =

    /// <summary>Constructs a batch size between 1 and 1000, inclusive.</summary>
    let tryCreate (value: int) : Result<ClaimBatch, string> =
        if value < 1 || value > 1000 then
            Error $"A claim batch must be between 1 and 1000, but was {value}."
        else
            Ok(ClaimBatch value)

    /// <summary>Constructs a batch size between 1 and 1000, inclusive.</summary>
    /// <exception cref="T:System.ArgumentException">The value is outside the supported range.</exception>
    let create (value: int) : ClaimBatch =
        match tryCreate value with
        | Ok batch -> batch
        | Error message -> invalidArg (nameof value) message

    /// <summary>Returns the underlying batch size.</summary>
    let value (ClaimBatch value) : int = value

/// <summary>A positive lease duration representable by every bundled provider.</summary>
[<Struct>]
type LeaseDuration = private LeaseDuration of TimeSpan

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Storage.LeaseDuration" />.</summary>
[<RequireQualifiedAccess>]
module LeaseDuration =

    let internal maximum: TimeSpan = TimeSpan.FromMilliseconds(float Int32.MaxValue)

    /// <summary>Constructs a positive duration no greater than <see cref="P:System.Int32.MaxValue" /> milliseconds.</summary>
    let tryCreate (value: TimeSpan) : Result<LeaseDuration, string> =
        if value <= TimeSpan.Zero then
            Error "A lease duration must be positive."
        elif value > maximum then
            Error $"A lease duration cannot exceed {Int32.MaxValue} milliseconds."
        else
            Ok(LeaseDuration value)

    /// <summary>Constructs a positive duration supported by every bundled provider.</summary>
    /// <exception cref="T:System.ArgumentException">The value is outside the supported range.</exception>
    let create (value: TimeSpan) : LeaseDuration =
        match tryCreate value with
        | Ok duration -> duration
        | Error message -> invalidArg (nameof value) message

    /// <summary>Returns the underlying duration.</summary>
    let value (LeaseDuration value) : TimeSpan = value

/// <summary>A non-blank, trimmed explanation for abandoning work.</summary>
type Reason = private Reason of string

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Storage.Reason" />.</summary>
[<RequireQualifiedAccess>]
module Reason =

    /// <summary>Constructs a reason from non-blank text, trimming surrounding whitespace.</summary>
    let tryCreate (value: string) : Result<Reason, string> =
        if String.IsNullOrWhiteSpace value then
            Error "A reason must be a non-empty string."
        else
            Ok(Reason(value.Trim()))

    /// <summary>Constructs a reason from non-blank text, trimming surrounding whitespace.</summary>
    /// <exception cref="T:System.ArgumentException">The input is null, empty, or whitespace.</exception>
    let create (value: string) : Reason =
        match tryCreate value with
        | Ok reason -> reason
        | Error message -> invalidArg (nameof value) message

    /// <summary>Returns the underlying trimmed text.</summary>
    let value (Reason value) : string = value
