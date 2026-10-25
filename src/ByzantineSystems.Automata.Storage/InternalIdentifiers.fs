namespace ByzantineSystems.Automata.Storage.Internal

/// <summary>
/// An entity id after the application's encoder has projected it to the database key. Internal
/// code carries the wrapper so an encoded key cannot be confused with arbitrary text.
/// </summary>
[<Struct>]
type internal EntityKey = private EntityKey of string

/// <summary>Operations on an encoded entity key.</summary>
[<RequireQualifiedAccess>]
module internal EntityKey =

    /// <summary>Marks application-encoded text as the key stored by a provider.</summary>
    let ofString (value: string) : EntityKey = EntityKey value

    /// <summary>Returns the encoded text at the database parameter boundary.</summary>
    let value (EntityKey value) : string = value
