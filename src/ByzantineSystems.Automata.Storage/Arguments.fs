namespace ByzantineSystems.Automata.Storage

open System

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
