namespace ByzantineSystems.Automata.Core

open System

/// <summary>
/// Identifies a node in a state chart. Hierarchical ids conventionally use dotted paths
/// (for example <c>active.settled.captured</c>) but any non-empty string is legal.
/// </summary>
[<Struct>]
type StateId = private StateId of string

/// <summary>Identifies a logical machine: one chart plus its configuration.</summary>
[<Struct>]
type MachineId = private MachineId of string

/// <summary>
/// Identifies an entity within a machine. The phantom type parameter ties the id to a domain
/// type so ids that belong to different machines cannot be mixed up.
/// </summary>
[<Struct>]
type EntityId<'entity> = private EntityId of string

/// <summary>
/// A hash of a chart's structure: the node graph, the terminal and initial-child markers, and
/// the kind of every rule, in the order the rules were declared. Rendered as 64 lowercase hex
/// digits.
///
/// This is a tripwire, not a proof of equivalence: guards, transforms, entry and exit actions and
/// the classifier are closures, and nothing can see inside them. It catches a chart edited
/// without a version bump; it cannot catch a behaviour change that leaves the structure alone.
/// </summary>
[<Struct>]
type ChartFingerprint = private ChartFingerprint of string

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Core.StateId" />.</summary>
[<RequireQualifiedAccess>]
module StateId =

    /// <summary>Constructs a state id from a non-empty string, trimming surrounding whitespace.</summary>
    /// <exception cref="T:System.ArgumentException">The input is null, empty, or whitespace.</exception>
    let create (value: string) : StateId =
        if String.IsNullOrWhiteSpace value then
            invalidArg (nameof value) "A state id must be a non-empty string."

        StateId(value.Trim())

    /// <summary>Returns the underlying string.</summary>
    let value (StateId value) : string = value

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Core.MachineId" />.</summary>
[<RequireQualifiedAccess>]
module MachineId =

    /// <summary>Constructs a machine id from a non-empty string, trimming surrounding whitespace.</summary>
    /// <exception cref="T:System.ArgumentException">The input is null, empty, or whitespace.</exception>
    let create (value: string) : MachineId =
        if String.IsNullOrWhiteSpace value then
            invalidArg (nameof value) "A machine id must be a non-empty string."

        MachineId(value.Trim())

    /// <summary>Returns the underlying string.</summary>
    let value (MachineId value) : string = value

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Core.EntityId`1" />.</summary>
[<RequireQualifiedAccess>]
module EntityId =

    /// <summary>Constructs an entity id from a non-empty string, trimming surrounding whitespace.</summary>
    /// <exception cref="T:System.ArgumentException">The input is null, empty, or whitespace.</exception>
    let create (value: string) : EntityId<'entity> =
        if String.IsNullOrWhiteSpace value then
            invalidArg (nameof value) "An entity id must be a non-empty string."

        EntityId(value.Trim())

    /// <summary>Returns the underlying string.</summary>
    let value (EntityId value) : string = value

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Core.ChartFingerprint" />.</summary>
[<RequireQualifiedAccess>]
module ChartFingerprint =

    /// <summary>
    /// Parses a fingerprint read back out of storage. Returns the reason rather than raising, so
    /// a corrupt row becomes a typed serialization failure at the store boundary instead of an
    /// exception escaping it.
    /// </summary>
    let tryCreate (value: string) : Result<ChartFingerprint, string> =
        if value.Length = 64 && value |> Seq.forall Char.IsAsciiHexDigitLower then
            Ok(ChartFingerprint value)
        else
            Error $"A chart fingerprint must be 64 lowercase hex digits, but was '%s{value}'."

    /// <summary>Parses a fingerprint.</summary>
    /// <exception cref="T:System.ArgumentException">The value is not a well-formed fingerprint.</exception>
    let create (value: string) : ChartFingerprint =
        match tryCreate value with
        | Ok fingerprint -> fingerprint
        | Error message -> invalidArg (nameof value) message

    /// <summary>Returns the underlying digest text.</summary>
    let value (ChartFingerprint value) : string = value

/// <summary>Shorthand constructors used by the chart and machine computation expressions.</summary>
[<AutoOpen>]
module TopLevel =

    /// <summary>Shorthand for <see cref="M:ByzantineSystems.Automata.Core.StateId.create" />.</summary>
    let stateId (value: string) : StateId = StateId.create value

    /// <summary>Shorthand for <see cref="M:ByzantineSystems.Automata.Core.MachineId.create" />.</summary>
    let machineId (value: string) : MachineId = MachineId.create value

    /// <summary>Shorthand for <see cref="M:ByzantineSystems.Automata.Core.EntityId.create" />.</summary>
    let entityId (value: string) : EntityId<'entity> = EntityId.create value
