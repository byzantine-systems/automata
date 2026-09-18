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

/// <summary>Shorthand constructors used by the chart and machine computation expressions.</summary>
[<AutoOpen>]
module TopLevel =

    /// <summary>Shorthand for <see cref="M:ByzantineSystems.Automata.Core.StateId.create" />.</summary>
    let stateId (value: string) : StateId = StateId.create value

    /// <summary>Shorthand for <see cref="M:ByzantineSystems.Automata.Core.MachineId.create" />.</summary>
    let machineId (value: string) : MachineId = MachineId.create value

    /// <summary>Shorthand for <see cref="M:ByzantineSystems.Automata.Core.EntityId.create" />.</summary>
    let entityId (value: string) : EntityId<'entity> = EntityId.create value
