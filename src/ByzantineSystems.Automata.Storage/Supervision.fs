namespace ByzantineSystems.Automata.Storage

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core

/// <summary>Identifies a supervisor in the durable audit log.</summary>
[<Struct>]
type SupervisorName = private SupervisorName of string

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Storage.SupervisorName" />.</summary>
[<RequireQualifiedAccess>]
module SupervisorName =

    /// <summary>Constructs a name from a non-empty string, trimming surrounding whitespace.</summary>
    /// <exception cref="T:System.ArgumentException">The input is null, empty, or whitespace.</exception>
    let create (value: string) : SupervisorName =
        if String.IsNullOrWhiteSpace value then
            invalidArg (nameof value) "A supervisor name must be a non-empty string."

        SupervisorName(value.Trim())

    /// <summary>Returns the underlying string.</summary>
    let value (SupervisorName value) : string = value

/// <summary>Identifies one child within a supervisor.</summary>
[<Struct>]
type SupervisedChildId = private SupervisedChildId of string

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Storage.SupervisedChildId" />.</summary>
[<RequireQualifiedAccess>]
module SupervisedChildId =

    /// <summary>Constructs an id from a non-empty string, trimming surrounding whitespace.</summary>
    /// <exception cref="T:System.ArgumentException">The input is null, empty, or whitespace.</exception>
    let create (value: string) : SupervisedChildId =
        if String.IsNullOrWhiteSpace value then
            invalidArg (nameof value) "A supervised child id must be a non-empty string."

        SupervisedChildId(value.Trim())

    /// <summary>Returns the underlying string.</summary>
    let value (SupervisedChildId value) : string = value

/// <summary>What happened to a supervised child.</summary>
type SupervisionAuditKind =
    | Started
    | Restarted
    | Escalated
    | Stopped

/// <summary>Which children are affected when a supervised child is restarted.</summary>
type SupervisionAuditStrategy =
    | OneForOne
    | OneForAll
    | RestForOne

/// <summary>One append-only supervision audit fact.</summary>
type SupervisionRecord =
    { Supervisor: SupervisorName
      ChildId: SupervisedChildId
      Kind: SupervisionAuditKind
      Strategy: SupervisionAuditStrategy
      Reason: string
      At: DateTimeOffset }

/// <summary>Narrow append-only surface for recording supervision facts.</summary>
type ISupervisionEventStore =

    /// <summary>Records one supervision fact.</summary>
    abstract Record: record: SupervisionRecord * ct: CancellationToken -> Task<Result<unit, StoreError>>
